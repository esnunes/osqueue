// Package local provides a filesystem-based Storage implementation using
// atomic file writes and content hashing for ETags.
package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"

	"github.com/esnunes/osqueue/storage"
)

// Storage implements the storage.Storage interface using the local filesystem.
// Each key maps to a file under the configured root directory.
// ETags are computed as SHA-256 hashes of the file content.
type Storage struct {
	root string
	mu   sync.Mutex
}

// New creates a new local filesystem storage rooted at dir.
// The directory is created if it does not exist.
func New(dir string) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Storage{root: dir}, nil
}

func (s *Storage) path(key string) string {
	return filepath.Join(s.root, key)
}

func etag(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// Read returns the object stored at key.
// Returns storage.ErrNotFound if the file does not exist.
func (s *Storage) Read(_ context.Context, key string) (storage.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path(key))
	if err != nil {
		if os.IsNotExist(err) {
			return storage.Object{}, storage.ErrNotFound
		}
		return storage.Object{}, err
	}
	return storage.Object{Data: data, ETag: etag(data)}, nil
}

// WriteIfMatch atomically writes data to key only if the current content's
// ETag matches the provided value. An empty etag means "create only if the
// file does not exist."
func (s *Storage) WriteIfMatch(_ context.Context, key string, data []byte, expectedETag string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p := s.path(key)

	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}

	existing, err := os.ReadFile(p)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}

	if os.IsNotExist(err) {
		// File does not exist.
		if expectedETag != "" {
			return "", storage.ErrConflict
		}
	} else {
		// File exists.
		if expectedETag == "" {
			return "", storage.ErrConflict
		}
		if etag(existing) != expectedETag {
			return "", storage.ErrConflict
		}
	}

	// Write atomically via temp file + rename.
	tmp, err := os.CreateTemp(filepath.Dir(p), ".osqueue-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return "", err
	}
	if err := os.Rename(tmpName, p); err != nil {
		os.Remove(tmpName)
		return "", err
	}

	return etag(data), nil
}
