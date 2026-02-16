// Package storage defines the interface for compare-and-set (CAS) storage
// backends used by the object storage queue.
package storage

import (
	"context"
	"errors"
)

var (
	// ErrNotFound is returned when the requested object does not exist.
	ErrNotFound = errors.New("osqueue: object not found")

	// ErrConflict is returned when a WriteIfMatch fails due to an ETag mismatch,
	// indicating another writer modified the object since it was last read.
	ErrConflict = errors.New("osqueue: etag mismatch")
)

// Object represents a stored object along with its version identifier.
type Object struct {
	Data []byte
	ETag string
}

// Storage provides compare-and-set semantics over a key-value object store.
type Storage interface {
	// Read returns the object at key.
	// Returns ErrNotFound if the object does not exist.
	Read(ctx context.Context, key string) (Object, error)

	// WriteIfMatch atomically writes data to key only if the current ETag
	// matches the provided value. An empty etag means "create only if the
	// object does not exist."
	// Returns the new ETag on success, or ErrConflict on mismatch.
	WriteIfMatch(ctx context.Context, key string, data []byte, etag string) (newETag string, err error)
}
