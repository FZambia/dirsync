package dirsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/FZambia/dirsync/internal/fsutil"
	"github.com/FZambia/dirsync/internal/hashutil"
	"github.com/FZambia/dirsync/internal/service"

	"github.com/dc0d/dirwatch"
	"google.golang.org/grpc/metadata"
)

// Client synchronizes local directory to server.
type Client struct {
	client           service.DirSyncClient
	absDir           string
	blockSize        uint32
	syncedAt         time.Time
	previousElements map[string]*service.Element
}

// NewClient creates new Client.
func NewClient(client service.DirSyncClient, dir string, blockSize uint32) (*Client, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(absDir); os.IsNotExist(err) || !info.IsDir() {
		return nil, errors.New("directory to sync from does not exist")
	}
	return &Client{
		client:           client,
		absDir:           absDir,
		blockSize:        blockSize,
		previousElements: make(map[string]*service.Element),
	}, nil
}

// Sync starts synchronization process until context cancellation.
func (c *Client) Sync(ctx context.Context) error {
	triggerCh := make(chan struct{}, 1)

	notify := func(ev dirwatch.Event) {
		log.Println("sth changed", ev.Name, ev.Op.String())
		select {
		case triggerCh <- struct{}{}:
		default:
			// Do nothing, the signal to sync directory already
			// sent and waiting to be processed.
		}
	}

	watcher := dirwatch.New(dirwatch.Notify(notify))
	defer watcher.Stop()
	watcher.Add(c.absDir, true)

	for {
		c.syncOnce()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-triggerCh:
			continue
		}
	}
}

func (c *Client) syncOnce() error {
	started := time.Now()
	elements, lastModTime, err := c.getStructureElements()
	if err != nil {
		return err
	}
	log.Printf("got directory tree structure, elapsed: %s", time.Since(started))

	if lastModTime.Sub(c.syncedAt) < 0 {
		log.Println("no changes detected, nothing to sync")
		return nil
	}

	pathToElement := map[string]*service.Element{}
	for _, e := range elements {
		pathToElement[e.GetPath()] = e
	}

	started = time.Now()
	if c.syncedAt.IsZero() {
		// Initial tree structure sync.
		log.Println("start syncing full directory tree")
		_, err = c.client.SyncStructure(context.Background(), &service.SyncRequest{
			Sep:      string(os.PathSeparator),
			Elements: elements,
		})
		if err != nil {
			return err
		}
		log.Printf("finished syncing full directory tree, elapsed: %s", time.Since(started))
	} else {
		log.Println("start syncing directory tree diff")
		diff := getStructureDiff(c.previousElements, pathToElement)
		if len(diff.created) > 0 || len(diff.deleted) > 0 {
			log.Printf("send structure difference, created: %d, deleted: %d", len(diff.created), len(diff.deleted))
			_, err = c.client.DiffStructure(context.Background(), &service.DiffRequest{
				Sep:     string(os.PathSeparator),
				Created: diff.created,
				Deleted: diff.deleted,
			})
			if err != nil {
				return err
			}
		}
		log.Printf("finished syncing directory tree diff, elapsed: %s", time.Since(started))
	}
	log.Println("start syncing files")
	started = time.Now()
	err = c.syncFiles(c.syncedAt, elements)
	if err != nil {
		return err
	}
	log.Printf("finished syncing files, elapsed: %s", time.Since(started))
	c.syncedAt = lastModTime
	c.previousElements = pathToElement
	return nil
}

type diff struct {
	created []*service.Element
	deleted []*service.Element
}

func getStructureDiff(previousElements map[string]*service.Element, pathToElement map[string]*service.Element) diff {
	created := []*service.Element{}
	deleted := []*service.Element{}

	for _, e := range pathToElement {
		if _, ok := previousElements[e.GetPath()]; ok {
			continue
		} else {
			if e.GetIsDir() {
				created = append(created, e)
			}
		}
	}
	for _, e := range previousElements {
		if _, ok := pathToElement[e.GetPath()]; ok {
			continue
		} else {
			deleted = append(deleted, e)
		}
	}
	return diff{
		created: created,
		deleted: deleted,
	}
}

// Convert to map for faster checksum check.
func getChecksumMap(checksums []*service.Checksum) map[uint32]map[string]int64 {
	m := make(map[uint32]map[string]int64)
	for i, cs := range checksums {
		if _, ok := m[cs.Weak]; !ok {
			m[cs.Weak] = map[string]int64{}
		}
		m[cs.Weak][cs.Strong] = int64(i)
	}
	return m
}

func (c *Client) syncFile(path string) error {
	absPath := filepath.Join(c.absDir, path)

	fileChecksum, err := fsutil.SHA256Checksum(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("%s does not exist, nothing to sync", path)
			return nil
		}
		return err
	}

	checksums, err := c.client.GetChecksum(context.Background(), &service.ChecksumRequest{Path: path, Checksum: fileChecksum, BlockSize: c.blockSize})
	if err != nil {
		return fmt.Errorf("error GetChecksum: %w", err)
	}
	log.Printf("checksum response for %s, num blocks: %d", checksums.Path, len(checksums.Checksums))

	if fileChecksum == checksums.GetChecksum() {
		log.Printf("%s checksum matched, already synced", path)
		return nil
	}

	// Use metadata to pass file name to sync over client-side stream.
	header := metadata.New(map[string]string{
		"path":       checksums.GetPath(),
		"block_size": strconv.FormatUint(uint64(c.blockSize), 10),
	})
	ctx := metadata.NewOutgoingContext(context.Background(), header)

	stream, err := c.client.UploadBlocks(ctx)
	if err != nil {
		return fmt.Errorf("error creating upload stream: %w", err)
	}

	numReferences := 0
	numBlockChanges := 0

	sendBlock := func(block *service.Block) error {
		if block.GetReference() {
			numReferences++
		} else {
			numBlockChanges++
		}
		if err := stream.Send(block); err != nil {
			return fmt.Errorf("%v.Send(%v) = %v", stream, block, err)
		}
		return nil
	}

	var buf bytes.Buffer

	flushBuf := func() error {
		block := &service.Block{Payload: buf.Bytes()}
		if err := sendBlock(block); err != nil {
			return err
		}
		buf.Reset()
		return nil
	}

	iterator, err := fsutil.NewFileIterator(filepath.Join(c.absDir, path), int64(c.blockSize))
	if err != nil {
		return fmt.Errorf("error creating file iterator: %w", err)
	}
	defer iterator.Close()

	checksumMap := getChecksumMap(checksums.GetChecksums())

	for {
		chunk, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("error reading chunk: %w", err)
		}
		if len(chunk) == 0 {
			break
		}

		var isReference bool
		var referenceNumber uint32
		if strongMap, ok := checksumMap[hashutil.WeakChecksum(chunk)]; ok {
			if num, ok := strongMap[hashutil.StrongChecksum(chunk)]; ok {
				isReference = true
				referenceNumber = uint32(num)
			}
		}

		if !isReference {
			if len(checksums.GetChecksums()) > 0 {
				// Looking for rolling checksums makes sense.
				iterator.IncOffset(1)
				buf.Write([]byte{chunk[0]})
			} else {
				// Looking for rolling checksums does not make sense.
				iterator.IncOffset(int64(c.blockSize))
				buf.Write(chunk)
			}
			if int64(buf.Len()) >= int64(c.blockSize) {
				if err := flushBuf(); err != nil {
					return err
				}
			}
		} else {
			iterator.IncOffset(int64(c.blockSize))
			if int64(buf.Len()) > 0 {
				if err := flushBuf(); err != nil {
					return err
				}
			}
			block := &service.Block{Reference: true, Number: referenceNumber}
			if err := sendBlock(block); err != nil {
				return err
			}
		}
	}

	if int64(buf.Len()) > 0 {
		if err := flushBuf(); err != nil {
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("error uploading: %w", err)
	}

	log.Printf("uploaded %s: %d references, %d changes", path, numReferences, numBlockChanges)
	return nil
}

func (c *Client) getStructureElements() ([]*service.Element, time.Time, error) {
	var lastModTime time.Time
	var elements []*service.Element

	err := filepath.Walk(c.absDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("getStructureElements error while walk: %w", err)
		}
		if info.ModTime().Sub(lastModTime) > 0 {
			lastModTime = info.ModTime()
		}
		trimmedPath := strings.TrimPrefix(strings.TrimPrefix(path, c.absDir), string(os.PathSeparator))
		if trimmedPath == "" {
			return nil
		}
		elements = append(elements, &service.Element{
			Path:    trimmedPath,
			IsDir:   info.IsDir(),
			ModTime: info.ModTime().UnixNano(),
		})
		return nil
	})
	return elements, lastModTime, err
}

func (c *Client) syncFiles(lastSynced time.Time, elements []*service.Element) error {
	for _, e := range elements {
		if e.GetIsDir() {
			continue
		}
		if e.ModTime-lastSynced.UnixNano() <= 0 {
			continue
		}
		log.Printf("syncing file: %s", e.GetPath())
		err := c.syncFile(e.GetPath())
		if err != nil {
			return err
		}
	}
	return nil
}
