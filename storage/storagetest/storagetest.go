// Package storagetest provides a reusable test suite that validates any
// storage.Storage implementation against the interface contract.
package storagetest

import (
	"context"
	"errors"
	"testing"

	"github.com/esnunes/osqueue/storage"
)

// Factory creates a fresh Storage instance for each test.
type Factory func(t *testing.T) storage.Storage

// Run executes the full contract test suite against the given Storage factory.
func Run(t *testing.T, factory Factory) {
	t.Run("ReadNotFound", func(t *testing.T) {
		s := factory(t)
		_, err := s.Read(context.Background(), "nonexistent")
		if !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("Read nonexistent: got err = %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateIfNotExists", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		data := []byte("hello")

		etag, err := s.WriteIfMatch(ctx, "key1", data, "")
		if err != nil {
			t.Fatalf("WriteIfMatch create: %v", err)
		}
		if etag == "" {
			t.Fatal("expected non-empty etag after create")
		}

		obj, err := s.Read(ctx, "key1")
		if err != nil {
			t.Fatalf("Read after create: %v", err)
		}
		if string(obj.Data) != "hello" {
			t.Errorf("data = %q, want %q", obj.Data, "hello")
		}
		if obj.ETag != etag {
			t.Errorf("etag = %q, want %q", obj.ETag, etag)
		}
	})

	t.Run("CreateConflictWhenExists", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		_, err := s.WriteIfMatch(ctx, "key1", []byte("first"), "")
		if err != nil {
			t.Fatalf("first create: %v", err)
		}

		// Trying to create again with empty etag should conflict.
		_, err = s.WriteIfMatch(ctx, "key1", []byte("second"), "")
		if !errors.Is(err, storage.ErrConflict) {
			t.Errorf("duplicate create: got err = %v, want ErrConflict", err)
		}
	})

	t.Run("UpdateWithMatchingETag", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		etag1, err := s.WriteIfMatch(ctx, "key1", []byte("v1"), "")
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		etag2, err := s.WriteIfMatch(ctx, "key1", []byte("v2"), etag1)
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		if etag2 == etag1 {
			t.Error("expected different etag after update")
		}

		obj, err := s.Read(ctx, "key1")
		if err != nil {
			t.Fatalf("read after update: %v", err)
		}
		if string(obj.Data) != "v2" {
			t.Errorf("data = %q, want %q", obj.Data, "v2")
		}
	})

	t.Run("UpdateConflictWrongETag", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		_, err := s.WriteIfMatch(ctx, "key1", []byte("v1"), "")
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		_, err = s.WriteIfMatch(ctx, "key1", []byte("v2"), "wrong-etag")
		if !errors.Is(err, storage.ErrConflict) {
			t.Errorf("update with wrong etag: got err = %v, want ErrConflict", err)
		}

		// Original data should be unchanged.
		obj, err := s.Read(ctx, "key1")
		if err != nil {
			t.Fatalf("read after failed update: %v", err)
		}
		if string(obj.Data) != "v1" {
			t.Errorf("data = %q, want %q (should be unchanged)", obj.Data, "v1")
		}
	})

	t.Run("UpdateConflictStaleETag", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		etag1, err := s.WriteIfMatch(ctx, "key1", []byte("v1"), "")
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		_, err = s.WriteIfMatch(ctx, "key1", []byte("v2"), etag1)
		if err != nil {
			t.Fatalf("update to v2: %v", err)
		}

		// Using the old etag should conflict.
		_, err = s.WriteIfMatch(ctx, "key1", []byte("v3"), etag1)
		if !errors.Is(err, storage.ErrConflict) {
			t.Errorf("update with stale etag: got err = %v, want ErrConflict", err)
		}
	})

	t.Run("DifferentKeys", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		_, err := s.WriteIfMatch(ctx, "a", []byte("data-a"), "")
		if err != nil {
			t.Fatalf("create a: %v", err)
		}
		_, err = s.WriteIfMatch(ctx, "b", []byte("data-b"), "")
		if err != nil {
			t.Fatalf("create b: %v", err)
		}

		objA, err := s.Read(ctx, "a")
		if err != nil {
			t.Fatalf("read a: %v", err)
		}
		objB, err := s.Read(ctx, "b")
		if err != nil {
			t.Fatalf("read b: %v", err)
		}

		if string(objA.Data) != "data-a" {
			t.Errorf("a data = %q, want %q", objA.Data, "data-a")
		}
		if string(objB.Data) != "data-b" {
			t.Errorf("b data = %q, want %q", objB.Data, "data-b")
		}
	})

	t.Run("NestedKeys", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		_, err := s.WriteIfMatch(ctx, "dir/subdir/file", []byte("nested"), "")
		if err != nil {
			t.Fatalf("create nested key: %v", err)
		}

		obj, err := s.Read(ctx, "dir/subdir/file")
		if err != nil {
			t.Fatalf("read nested key: %v", err)
		}
		if string(obj.Data) != "nested" {
			t.Errorf("data = %q, want %q", obj.Data, "nested")
		}
	})

	t.Run("SameContentDifferentETag", func(t *testing.T) {
		// Writing the same content should produce the same etag (content-addressed).
		s := factory(t)
		ctx := context.Background()

		etag1, err := s.WriteIfMatch(ctx, "key1", []byte("same"), "")
		if err != nil {
			t.Fatalf("create: %v", err)
		}

		// Write different content, then write back the same.
		etag2, err := s.WriteIfMatch(ctx, "key1", []byte("different"), etag1)
		if err != nil {
			t.Fatalf("update to different: %v", err)
		}

		etag3, err := s.WriteIfMatch(ctx, "key1", []byte("same"), etag2)
		if err != nil {
			t.Fatalf("update back to same: %v", err)
		}
		if etag3 != etag1 {
			t.Errorf("same content produced different etags: %q vs %q", etag3, etag1)
		}
	})

	t.Run("UpdateNonexistentWithETag", func(t *testing.T) {
		s := factory(t)
		_, err := s.WriteIfMatch(context.Background(), "missing", []byte("data"), "some-etag")
		if !errors.Is(err, storage.ErrConflict) {
			t.Errorf("update nonexistent with etag: got err = %v, want ErrConflict", err)
		}
	})
}
