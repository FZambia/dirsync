package fsutil

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
)

// SHA256Checksum returns SHA-256 checksum of file in path.
func SHA256Checksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// FileChunker ...
type FileChunker struct {
	path string
	size int64
	file *os.File
}

// NewFileChunker ...
func NewFileChunker(path string, size int64) (*FileChunker, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &FileChunker{
		path: path,
		size: size,
		file: file,
	}, nil
}

// Close ...
func (c *FileChunker) Close() error {
	return c.file.Close()
}

// Next ...
func (c *FileChunker) Next() ([]byte, bool, error) {
	buf := make([]byte, c.size)
	n, err := io.ReadFull(c.file, buf)
	if err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return buf[:n], false, nil
		}
		return nil, false, err
	}
	return buf[:], true, nil
}

// FileIterator ...
type FileIterator struct {
	path   string
	size   int64
	file   *os.File
	offset int64
}

// NewFileIterator ...
func NewFileIterator(path string, size int64) (*FileIterator, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &FileIterator{
		path: path,
		size: size,
		file: file,
	}, nil
}

// Close ...
func (c *FileIterator) Close() error {
	return c.file.Close()
}

// Next ...
func (c *FileIterator) Next() ([]byte, error) {
	c.file.Seek(c.offset, 0)
	buf := make([]byte, c.size)
	n, err := io.ReadFull(c.file, buf)
	if err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return buf[:n], nil
		}
		return nil, err
	}
	return buf[:], nil
}

// IncOffset ...
func (c *FileIterator) IncOffset(inc int64) {
	c.offset += inc
}
