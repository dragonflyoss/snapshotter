package metadata

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBbolt_NewAndClose(t *testing.T) {
	tempDir := t.TempDir()
	rootDir := filepath.Join(tempDir, "root")

	store, err := newBbolt(rootDir)
	require.NoError(t, err)
	require.NotNil(t, store)

	err = store.Close()
	require.NoError(t, err)
}

func TestBbolt_PutAndGetMetadata(t *testing.T) {
	tempDir := t.TempDir()
	store, err := newBbolt(tempDir)
	require.NoError(t, err)
	defer store.Close()

	key := "test-meta-key"
	entry := MetadataEntry{
		Config: Config{
			Files: []File{{Digest: "abc123"}},
		},
	}

	err = store.PutMetadata(context.Background(), key, entry)
	require.NoError(t, err)

	got, err := store.GetMetadata(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, entry.Config.Files, got.Config.Files)
}

func TestBbolt_GetMetadataNotFound(t *testing.T) {
	tempDir := t.TempDir()
	store, err := newBbolt(tempDir)
	require.NoError(t, err)
	defer store.Close()

	_, err = store.GetMetadata(context.Background(), "nonexistent")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestBbolt_PutAndGetContent(t *testing.T) {
	tempDir := t.TempDir()
	store, err := newBbolt(tempDir)
	require.NoError(t, err)
	defer store.Close()

	key := "content-key"
	file := File{Digest: "sha256:abc123"}

	err = store.PutContent(context.Background(), key, file)
	require.NoError(t, err)

	got, err := store.GetContent(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, file, *got)
}

func TestBbolt_PutAndGetContentMetadata(t *testing.T) {
	tempDir := t.TempDir()
	store, err := newBbolt(tempDir)
	require.NoError(t, err)
	defer store.Close()

	metaKey := "task-1"
	entry := MetadataEntry{
		Config: Config{
			Files: []File{
				{Digest: "digest1"},
				{Digest: "digest2"},
			},
		},
	}

	err = store.PutContentMetadata(context.Background(), metaKey, entry)
	require.NoError(t, err)

	for _, digest := range []string{"digest1", "digest2"} {
		cm, err := store.GetContentMetadata(context.Background(), digest)
		require.NoError(t, err)
		require.Contains(t, cm.References, metaKey)
	}
}

func TestBbolt_TouchMetadata(t *testing.T) {
	tempDir := t.TempDir()
	store, err := newBbolt(tempDir)
	require.NoError(t, err)
	defer store.Close()

	key := "touch-me"
	entry := MetadataEntry{LastAccessedAt: time.Unix(1000, 0)}
	err = store.PutMetadata(context.Background(), key, entry)
	require.NoError(t, err)

	before := time.Now()
	time.Sleep(10 * time.Millisecond)

	err = store.TouchMetadata(context.Background(), key)
	require.NoError(t, err)

	updated, err := store.GetMetadata(context.Background(), key)
	require.NoError(t, err)
	require.True(t, updated.LastAccessedAt.After(before), "LastAccessedAt should be updated")
}

func TestBbolt_IterateMetadata(t *testing.T) {
	tempDir := t.TempDir()
	store, err := newBbolt(tempDir)
	require.NoError(t, err)
	defer store.Close()

	entries := map[string]MetadataEntry{
		"key1": {Config: Config{Files: []File{{Digest: "digest1"}}}},
		"key2": {Config: Config{Files: []File{{Digest: "digest2"}}}},
	}

	for k, v := range entries {
		err := store.PutMetadata(context.Background(), k, v)
		require.NoError(t, err)
	}

	count := 0
	err = store.IterateMetadata(context.Background(), func(key string, entry MetadataEntry) error {
		count++
		expected, ok := entries[key]
		require.True(t, ok)
		require.Equal(t, expected, entry)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestBbolt_Prune(t *testing.T) {
	tempDir := t.TempDir()
	store, err := newBbolt(tempDir)
	require.NoError(t, err)
	defer store.Close()

	metaKey1 := "task-1"
	metaKey2 := "task-2"
	entry1 := MetadataEntry{
		Config: Config{
			Files: []File{
				{Digest: "shared-digest"},
				{Digest: "unique1"},
			},
		},
	}
	entry2 := MetadataEntry{
		Config: Config{
			Files: []File{
				{Digest: "shared-digest"}, // shared
				{Digest: "unique2"},
			},
		},
	}

	err = store.PutMetadata(context.Background(), metaKey1, entry1)
	require.NoError(t, err)
	err = store.PutMetadata(context.Background(), metaKey2, entry2)
	require.NoError(t, err)

	err = store.PutContentMetadata(context.Background(), metaKey1, entry1)
	require.NoError(t, err)
	err = store.PutContentMetadata(context.Background(), metaKey2, entry2)
	require.NoError(t, err)

	// Prune metaKey1.
	pruned, err := store.Prune(context.Background(), metaKey1)
	require.NoError(t, err)
	require.Len(t, pruned, 1) // only "unique1" should be pruned
	require.Equal(t, "unique1", pruned[0].Digest)

	// "shared-digest" should still exist.
	cm, err := store.GetContentMetadata(context.Background(), "shared-digest")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{metaKey2}, cm.References)

	// "unique1" should be gone.
	_, err = store.GetContent(context.Background(), "unique1")
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = store.GetContentMetadata(context.Background(), "unique1")
	require.ErrorIs(t, err, ErrKeyNotFound)

	// Now prune metaKey2 → shared and unique2 should be pruned.
	pruned2, err := store.Prune(context.Background(), metaKey2)
	require.NoError(t, err)
	require.Len(t, pruned2, 2) // both shared and unique2
	digests := []string{pruned2[0].Digest, pruned2[1].Digest}
	require.Contains(t, digests, "shared-digest")
	require.Contains(t, digests, "unique2")
}
