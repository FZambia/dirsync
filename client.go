package dirsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/FZambia/dirsync/internal/fsutil"
	"github.com/FZambia/dirsync/internal/hashutil"
	"github.com/FZambia/dirsync/internal/service"

	"github.com/dc0d/dirwatch"
	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc/metadata"
)

// Client synchronizes local directory to server.
type Client struct {
	client    service.DirSyncClient
	absDir    string
	blockSize int64
}

// NewClient creates new Client.
func NewClient(client service.DirSyncClient, dir string, blockSize int64) (*Client, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(absDir); os.IsNotExist(err) || !info.IsDir() {
		return nil, errors.New("directory to sync from does not exist")
	}
	return &Client{client, absDir, blockSize}, nil
}

// Sync starts synchronization process until context cancellation.
func (s *Client) Sync(ctx context.Context) error {
	triggerCh := make(chan struct{}, 1)

	notify := func(ev dirwatch.Event) {
		if ev.Op == fsnotify.Chmod {
			// Not interesting as we don't bother about permissions.
			return
		}
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
	watcher.Add(s.absDir, true)

	var syncedAt time.Time
	for {
		elements, lastModTime, err := s.getStructureElements()
		if err != nil {
			return err
		}
		if lastModTime.Sub(syncedAt) > 0 {
			log.Println("start syncing directory tree")
			started := time.Now()
			_, err = s.client.SyncStructure(context.Background(), &service.StructureRequest{
				Sep:      string(os.PathSeparator),
				Elements: elements,
			})
			log.Printf("finished syncing directory tree, elapsed: %s", time.Since(started))
			log.Println("start syncing files")
			started = time.Now()
			err = s.syncFiles()
			if err != nil {
				return err
			}
			log.Printf("finished syncing files, elapsed: %s", time.Since(started))
			syncedAt = lastModTime
		} else {
			log.Println("no changes detected, nothing to sync")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-triggerCh:
			continue
		}
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

func (s *Client) syncFile(path string) error {

	checksums, err := s.client.GetChecksum(context.Background(), &service.ChecksumRequest{Path: path})
	if err != nil {
		return fmt.Errorf("error GetChecksum: %w", err)
	}
	log.Printf("checksum response for %s, num blocks: %d", checksums.Path, len(checksums.Checksums))

	fileChecksum, err := fsutil.SHA256Checksum(filepath.Join(s.absDir, path))
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("%s does not exist, nothing to sync", path)
			return nil
		}
		return err
	}

	if fileChecksum == checksums.GetChecksum() {
		log.Printf("%s checksum matched, nothing to sync", path)
		return nil
	}

	// Use metadata to pass file name to sync over client-side stream.
	header := metadata.New(map[string]string{"path": checksums.GetPath()})
	ctx := metadata.NewOutgoingContext(context.Background(), header)

	stream, err := s.client.UploadBlocks(ctx)
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

	iterator, err := fsutil.NewFileIterator(filepath.Join(s.absDir, path), s.blockSize)
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
				iterator.IncOffset(s.blockSize)
				buf.Write(chunk)
			}
			if int64(buf.Len()) >= s.blockSize {
				if err := flushBuf(); err != nil {
					return err
				}
			}
		} else {
			iterator.IncOffset(s.blockSize)
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

func (s *Client) getStructureElements() ([]*service.Element, time.Time, error) {
	var lastModTime time.Time
	var elements []*service.Element

	err := filepath.Walk(s.absDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("getStructureElements error while walk: %w", err)
		}
		if info.ModTime().Sub(lastModTime) > 0 {
			lastModTime = info.ModTime()
		}
		trimmedPath := strings.TrimPrefix(strings.TrimPrefix(path, s.absDir), string(os.PathSeparator))
		if trimmedPath == "" {
			return nil
		}
		elements = append(elements, &service.Element{
			Path:  trimmedPath,
			IsDir: info.IsDir(),
		})
		return nil
	})
	return elements, lastModTime, err
}

func (s *Client) syncFiles() error {
	err := filepath.Walk(s.absDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("syncFiles error while walk: %w", err)
		}
		if info.IsDir() {
			return nil
		}
		trimmedPath := strings.TrimPrefix(strings.TrimPrefix(path, s.absDir), string(os.PathSeparator))
		if trimmedPath == "" {
			return nil
		}
		log.Printf("syncing file: %s", trimmedPath)
		return s.syncFile(trimmedPath)
	})
	return err
}
