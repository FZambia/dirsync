package dirsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/FZambia/dirsync/internal/fsutil"
	"github.com/FZambia/dirsync/internal/hashutil"
	"github.com/FZambia/dirsync/internal/service"

	"google.golang.org/grpc/metadata"
)

// Server keeps directory synchronized with client.
type Server struct {
	mu             sync.RWMutex
	absDir         string
	blockSize      int64
	pathToChecksum map[string]string
}

// NewServer creates new Server.
func NewServer(dir string, blockSize int64) (*Server, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(absDir); os.IsNotExist(err) || !info.IsDir() {
		return nil, errors.New("directory to sync to does not exist")
	}

	return &Server{
		absDir:         absDir,
		blockSize:      blockSize,
		pathToChecksum: make(map[string]string),
	}, nil
}

func (s *Server) updateChecksumMapping(absPath, checksum string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pathToChecksum[absPath] = checksum
}

func (s *Server) deleteChecksumMapping(absPath string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pathToChecksum, absPath)
}

func (s *Server) copyExisting(absPath, incomingChecksum string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// TODO: use a better data structure to speed up same file lookup.
	for sameFilePath, cs := range s.pathToChecksum {
		if cs != incomingChecksum {
			continue
		}
		if err := fsutil.ForceCopy(sameFilePath, absPath); err == nil {
			return true
		}
	}
	return false
}

// SyncStructure synchronizes directories and removes non-actual directories and files.
func (s *Server) SyncStructure(ctx context.Context, req *service.StructureRequest) (*service.StructureResponse, error) {
	pathMap := make(map[string]struct{}, len(req.GetElements()))

	for _, el := range req.GetElements() {
		path := filepath.Join(s.absDir, el.GetPath())
		pathMap[path] = struct{}{}
		if el.GetIsDir() {
			log.Println("creating dir", path)
			err := os.MkdirAll(path, 0755)
			if err != nil {
				return nil, err
			}
		}
	}

	err := filepath.Walk(s.absDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if s.absDir == path {
			return nil
		}
		if _, ok := pathMap[path]; !ok {
			log.Println("removing path", path)
			err := os.RemoveAll(path)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			s.deleteChecksumMapping(path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &service.StructureResponse{}, nil
}

// GetChecksum returns checksum file info.
func (s *Server) GetChecksum(ctx context.Context, req *service.ChecksumRequest) (*service.ChecksumResponse, error) {
	checksums := make([]*service.Checksum, 0)

	absPath := filepath.Join(s.absDir, fsutil.CleanPath(req.Path))

	fileChecksum, err := fsutil.SHA256Checksum(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			if s.copyExisting(absPath, req.GetChecksum()) {
				log.Println(req.Path, "copied from same file")
				s.updateChecksumMapping(absPath, req.GetChecksum())
				return &service.ChecksumResponse{
					Checksum: req.GetChecksum(),
					Path:     req.Path,
				}, nil
			}
			return &service.ChecksumResponse{
				Path: req.Path,
			}, nil
		}
		return nil, err
	}

	if fileChecksum == req.GetChecksum() {
		log.Println("file checksum match", req.Path)
		s.updateChecksumMapping(absPath, req.GetChecksum())
		return &service.ChecksumResponse{
			Path:     req.Path,
			Checksum: req.GetChecksum(),
		}, nil
	}

	if s.copyExisting(absPath, req.GetChecksum()) {
		log.Println(req.Path, "copied from same file")
		s.updateChecksumMapping(absPath, req.GetChecksum())
		return &service.ChecksumResponse{
			Checksum: req.GetChecksum(),
			Path:     req.Path,
		}, nil
	}

	chunker, err := fsutil.NewFileChunker(absPath, s.blockSize)
	if err != nil {
		if os.IsNotExist(err) {
			return &service.ChecksumResponse{
				Path: req.Path,
			}, nil
		}
		return nil, err
	}
	defer chunker.Close()

	fileHash := sha256.New()

	for {
		chunk, hasMore, err := chunker.Next()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}

		fileHash.Write(chunk)
		cs := &service.Checksum{
			Weak:   hashutil.WeakChecksum(chunk),
			Strong: hashutil.StrongChecksum(chunk),
		}
		checksums = append(checksums, cs)
		if !hasMore {
			break
		}
	}

	return &service.ChecksumResponse{
		Path:      req.Path,
		Checksum:  fileChecksum,
		Checksums: checksums,
	}, nil
}

func extractPath(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("metadata required")
	}
	paths := md.Get("path")
	if len(paths) != 1 {
		return "", errors.New("malformed path")
	}
	path := paths[0]
	if path == "" {
		return "", errors.New("empty path")
	}
	return path, nil
}

// UploadBlocks accepts file block stream from client to create modified
// file using changes and references to original blocks.
func (s *Server) UploadBlocks(stream service.DirSync_UploadBlocksServer) error {
	path, err := extractPath(stream.Context())
	if err != nil {
		return err
	}

	tmpfile, err := ioutil.TempFile("", "netsync")
	if err != nil {
		log.Fatal(err)
	}
	defer tmpfile.Close()

	absPath := filepath.Join(s.absDir, fsutil.CleanPath(path))

	err = os.MkdirAll(filepath.Dir(absPath), 0755)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(absPath, os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	fileHash := sha256.New()
	startTime := time.Now()
	for {
		block, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				// Atomically move tmp file to directory.
				os.Rename(tmpfile.Name(), absPath)
				fileChecksum := hex.EncodeToString(fileHash.Sum(nil))
				s.updateChecksumMapping(absPath, fileChecksum)
				fmt.Printf("uploading %s, elapsed: %s\n", path, time.Since(startTime))
				return stream.SendAndClose(&service.UploadResponse{})
			}
			return err
		}
		if block.GetReference() {
			file.Seek(int64(block.GetNumber())*s.blockSize, 0)
			buf := make([]byte, s.blockSize)
			n, _ := io.ReadFull(file, buf)
			_, err := tmpfile.Write(buf[:n])
			if err != nil {
				return err
			}
			fileHash.Write(buf[:n])
		} else {
			_, err := tmpfile.Write(block.GetPayload())
			if err != nil {
				return err
			}
			fileHash.Write(block.GetPayload())
		}
	}
}
