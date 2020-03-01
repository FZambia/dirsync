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
	"time"

	"github.com/FZambia/dirsync/internal/fsutil"
	"github.com/FZambia/dirsync/internal/hashutil"
	"github.com/FZambia/dirsync/internal/service"
	"google.golang.org/grpc/metadata"
)

// Server keeps directory synchronized with client.
type Server struct {
	absDir    string
	blockSize int64
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
		absDir:    absDir,
		blockSize: blockSize,
	}, nil
}

// SyncStructure ...
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
		} else {
			log.Println("creating file", path)
			f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0755)
			if err != nil {
				return nil, err
			}
			f.Close()
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
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &service.StructureResponse{}, nil
}

// GetChecksum ...
func (s *Server) GetChecksum(ctx context.Context, req *service.ChecksumRequest) (*service.ChecksumResponse, error) {
	checksums := make([]*service.Checksum, 0)

	chunker, err := fsutil.NewFileChunker(filepath.Join(s.absDir, req.Path), s.blockSize)
	if err != nil {
		if os.IsNotExist(err) {
			return &service.ChecksumResponse{
				Path:      req.Path,
				Checksums: checksums,
			}, nil
		}
		return nil, err
	}
	defer chunker.Close()

	fileChecksum := sha256.New()

	for {
		chunk, hasMore, err := chunker.Next()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			break
		}

		fileChecksum.Write(chunk)
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
		Checksum:  hex.EncodeToString(fileChecksum.Sum(nil)),
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

// UploadBlocks ...
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

	fpath := filepath.Join(s.absDir, path)

	file, err := os.Open(fpath)
	if err != nil {
		return err
	}
	defer file.Close()

	startTime := time.Now()
	for {
		block, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				// Atomically move tmp file to directory.
				os.Rename(tmpfile.Name(), fpath)
				fmt.Printf("elapsed for %s: %s\n", path, time.Since(startTime))
				return stream.SendAndClose(&service.UploadResponse{})
			}
			return err
		}
		if block.GetReference() {
			file.Seek(int64(block.GetNumber())*s.blockSize, 0)
			buf := make([]byte, s.blockSize)
			n, _ := io.ReadFull(file, buf)
			tmpfile.Write(buf[:n])
		} else {
			tmpfile.Write(block.GetPayload())
		}
	}
}
