package local

import (
	"testing"

	"github.com/esnunes/osqueue/storage"
	"github.com/esnunes/osqueue/storage/storagetest"
)

func TestLocalStorage(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Storage {
		t.Helper()
		s, err := New(t.TempDir())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return s
	})
}
