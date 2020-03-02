package fsutil

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
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

// CleanPath makes a path safe for use with filepath.Join. This is done by not
// only cleaning the path, but also (if the path is relative) adding a leading
// '/' and cleaning it (then removing the leading '/'). This ensures that a
// path resulting from prepending another path will always resolve to lexically
// be a subdirectory of the prefixed path. This is all done lexically, so paths
// that include symlinks won't be safe as a result of using CleanPath.
// Credits: https://github.com/opencontainers/runc/blob/master/libcontainer/utils/utils.go#L67
// See related discussion here: https://github.com/golang/go/issues/20126
func CleanPath(path string) string {
	// Deal with empty strings nicely.
	if path == "" {
		return ""
	}

	// Ensure that all paths are cleaned (especially problematic ones like
	// "/../../../../../" which can cause lots of issues).
	path = filepath.Clean(path)

	// If the path isn't absolute, we need to do more processing to fix paths
	// such as "../../../../<etc>/some/path". We also shouldn't convert absolute
	// paths to relative ones.
	if !filepath.IsAbs(path) {
		path = filepath.Clean(string(os.PathSeparator) + path)
		// This can't fail, as (by definition) all paths are relative to root.
		path, _ = filepath.Rel(string(os.PathSeparator), path)
	}

	// Clean the path again for good measure.
	return filepath.Clean(path)
}
