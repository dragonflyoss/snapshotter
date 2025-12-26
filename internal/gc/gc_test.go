/*
 *     Copyright 2025 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	internalmetadata "d7y.io/snapshotter/internal/metadata"
	"d7y.io/snapshotter/mocks/metadata"
	"d7y.io/snapshotter/mocks/storage"
)

// TestNewGC tests that New returns a non-nil GC instance with correct defaults.
func TestNewGC(t *testing.T) {
	meta := &metadata.Metadata{}
	store := &storage.Storage{}
	gc := New("/tmp", meta, store)
	require.NotNil(t, gc)
	assert.Equal(t, defaultGCInterval, gc.interval)
	assert.Equal(t, defaultDiskHighThresholdPercent, gc.diskHighThresholdPercent)
	assert.Equal(t, defaultDiskLowThresholdPercent, gc.diskLowThresholdPercent)
	assert.Equal(t, defaultGCBatchSize, gc.batchSize)
	assert.Equal(t, defaultMinRetentionPeriod, gc.minRetentionPeriod)
}

// TestWithOptions tests that GC options correctly override defaults.
func TestWithOptions(t *testing.T) {
	meta := &metadata.Metadata{}
	store := &storage.Storage{}
	gc := New("/tmp", meta, store,
		WithInterval(10*time.Second),
		WithDiskHighThresholdPercent(80.0),
		WithDiskLowThresholdPercent(70.0),
		WithBatchSize(5),
		WithMinRetentionPeriod(10*time.Minute),
	)
	require.NotNil(t, gc)
	assert.Equal(t, 10*time.Second, gc.interval)
	assert.Equal(t, 80.0, gc.diskHighThresholdPercent)
	assert.Equal(t, 70.0, gc.diskLowThresholdPercent)
	assert.Equal(t, 5, gc.batchSize)
	assert.Equal(t, 10*time.Minute, gc.minRetentionPeriod)
}

// TestWithOptions_InvalidValues tests that invalid option values are ignored.
func TestWithOptions_InvalidValues(t *testing.T) {
	meta := &metadata.Metadata{}
	store := &storage.Storage{}
	gc := New("/tmp", meta, store,
		WithDiskHighThresholdPercent(-10.0),    // Invalid: negative
		WithDiskLowThresholdPercent(110.0),     // Invalid: > 100
		WithBatchSize(-5),                      // Invalid: negative
		WithMinRetentionPeriod(-1*time.Second), // Invalid: negative
	)
	require.NotNil(t, gc)
	// Should keep defaults.
	assert.Equal(t, defaultDiskHighThresholdPercent, gc.diskHighThresholdPercent)
	assert.Equal(t, defaultDiskLowThresholdPercent, gc.diskLowThresholdPercent)
	assert.Equal(t, defaultGCBatchSize, gc.batchSize)
	assert.Equal(t, defaultMinRetentionPeriod, gc.minRetentionPeriod)
}

// TestCollectBatch_NoCandidates tests collectBatch when no metadata entries exist.
func TestCollectBatch_NoCandidates(t *testing.T) {
	mockMeta := metadata.NewMetadata(t)
	mockMeta.EXPECT().IterateMetadata(mock.Anything, mock.Anything).Return(nil).Run(func(ctx context.Context, fn func(string, internalmetadata.MetadataEntry) error) {
		// Empty metadata - don't call fn
	})

	gc := &gc{
		metadata:           mockMeta,
		batchSize:          defaultGCBatchSize,
		minRetentionPeriod: defaultMinRetentionPeriod,
	}

	ctx := context.Background()
	_, err := gc.collectBatch(ctx)
	assert.ErrorIs(t, err, ErrNoCandidate)
}

// TestCollectBatch_SortsByLastAccessed tests that collectBatch returns LRU entries.
func TestCollectBatch_SortsByLastAccessed(t *testing.T) {
	now := time.Now()
	entries := []struct {
		key string
		ts  time.Time
	}{
		{"key1", now.Add(-10 * time.Minute)},
		{"key2", now.Add(-20 * time.Minute)}, // Older
		{"key3", now.Add(-15 * time.Minute)},
	}

	mockMeta := metadata.NewMetadata(t)
	mockMeta.EXPECT().IterateMetadata(mock.Anything, mock.Anything).Return(nil).Run(func(ctx context.Context, fn func(string, internalmetadata.MetadataEntry) error) {
		for _, e := range entries {
			_ = fn(e.key, internalmetadata.MetadataEntry{LastAccessedAt: e.ts})
		}
	})

	gc := &gc{
		metadata:           mockMeta,
		batchSize:          10,
		minRetentionPeriod: 5 * time.Minute, // All entries are older than this
	}

	ctx := context.Background()
	keys, err := gc.collectBatch(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 3)
	// Expected order: key2 (oldest), key3, key1 (newest).
	assert.Equal(t, []string{"key2", "key3", "key1"}, keys)
}

// TestCollectBatch_RespectsMinRetentionPeriod tests that recently accessed snapshots are excluded.
func TestCollectBatch_RespectsMinRetentionPeriod(t *testing.T) {
	now := time.Now()
	entries := []struct {
		key string
		ts  time.Time
	}{
		{"old1", now.Add(-10 * time.Minute)},   // Old enough
		{"recent1", now.Add(-2 * time.Minute)}, // Too recent
		{"old2", now.Add(-15 * time.Minute)},   // Old enough
		{"recent2", now.Add(-1 * time.Minute)}, // Too recent
	}

	mockMeta := metadata.NewMetadata(t)
	mockMeta.EXPECT().IterateMetadata(mock.Anything, mock.Anything).Return(nil).Run(func(ctx context.Context, fn func(string, internalmetadata.MetadataEntry) error) {
		for _, e := range entries {
			_ = fn(e.key, internalmetadata.MetadataEntry{LastAccessedAt: e.ts})
		}
	})

	gc := &gc{
		metadata:           mockMeta,
		batchSize:          10,
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()
	keys, err := gc.collectBatch(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 2)
	// Should only include old1 and old2.
	assert.Contains(t, keys, "old1")
	assert.Contains(t, keys, "old2")
	assert.NotContains(t, keys, "recent1")
	assert.NotContains(t, keys, "recent2")
}

// TestCollectBatch_RespectsBatchSize tests that collectBatch respects the batch size limit.
func TestCollectBatch_RespectsBatchSize(t *testing.T) {
	now := time.Now()
	mockMeta := metadata.NewMetadata(t)
	mockMeta.EXPECT().IterateMetadata(mock.Anything, mock.Anything).Return(nil).Run(func(ctx context.Context, fn func(string, internalmetadata.MetadataEntry) error) {
		for i := range 20 {
			_ = fn("key"+string(rune(i)), internalmetadata.MetadataEntry{
				LastAccessedAt: now.Add(-10 * time.Minute),
			})
		}
	})

	gc := &gc{
		metadata:           mockMeta,
		batchSize:          5,
		minRetentionPeriod: 1 * time.Minute,
	}

	ctx := context.Background()
	keys, err := gc.collectBatch(ctx)
	require.NoError(t, err)
	assert.Len(t, keys, 5) // Should respect batch size
}

// TestCollectBatch_IterateError tests error handling when iteration fails.
func TestCollectBatch_IterateError(t *testing.T) {
	expectedErr := errors.New("iterate error")
	mockMeta := metadata.NewMetadata(t)
	mockMeta.EXPECT().IterateMetadata(mock.Anything, mock.Anything).Return(expectedErr)

	gc := &gc{
		metadata:           mockMeta,
		batchSize:          defaultGCBatchSize,
		minRetentionPeriod: defaultMinRetentionPeriod,
	}

	ctx := context.Background()
	_, err := gc.collectBatch(ctx)
	assert.ErrorIs(t, err, expectedErr)
}

// TestPruneBatch_Success tests successful pruning of a batch.
func TestPruneBatch_Success(t *testing.T) {
	now := time.Now()
	mockMeta := metadata.NewMetadata(t)
	mockStore := storage.NewStorage(t)

	content1 := internalmetadata.File{Digest: "xxh3:abc"}
	content2 := internalmetadata.File{Digest: "xxh3:def"}

	// Re-validation should succeed.
	mockMeta.EXPECT().GetMetadata(mock.Anything, "snap1").Return(&internalmetadata.MetadataEntry{
		LastAccessedAt: now.Add(-10 * time.Minute),
	}, nil)

	mockMeta.EXPECT().Prune(mock.Anything, "snap1").Return([]internalmetadata.File{content1, content2}, nil)
	mockStore.EXPECT().Prune(mock.Anything, "abc").Return(nil)
	mockStore.EXPECT().Prune(mock.Anything, "def").Return(nil)

	gc := &gc{
		metadata:           mockMeta,
		storage:            mockStore,
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()
	err := gc.pruneBatch(ctx, []string{"snap1"})
	require.NoError(t, err)
}

// TestPruneBatch_SkipsRecentlyAccessed tests TOCTOU protection for recently accessed snapshots.
func TestPruneBatch_SkipsRecentlyAccessed(t *testing.T) {
	now := time.Now()
	mockMeta := metadata.NewMetadata(t)

	// Re-validation shows the snapshot was recently accessed.
	mockMeta.EXPECT().GetMetadata(mock.Anything, "snap1").Return(&internalmetadata.MetadataEntry{
		LastAccessedAt: now.Add(-1 * time.Minute), // Too recent!
	}, nil)

	// Prune should NOT be called.
	mockMeta.AssertNotCalled(t, "Prune", mock.Anything, "snap1")

	gc := &gc{
		metadata:           mockMeta,
		storage:            &storage.Storage{},
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()
	err := gc.pruneBatch(ctx, []string{"snap1"})
	require.NoError(t, err)
}

// TestPruneBatch_SkipsAlreadyDeleted tests handling of already deleted snapshots.
func TestPruneBatch_SkipsAlreadyDeleted(t *testing.T) {
	mockMeta := metadata.NewMetadata(t)

	// Re-validation shows the snapshot is already deleted.
	mockMeta.EXPECT().GetMetadata(mock.Anything, "snap1").Return(nil, internalmetadata.ErrKeyNotFound)

	// Prune should NOT be called.
	mockMeta.AssertNotCalled(t, "Prune", mock.Anything, "snap1")

	gc := &gc{
		metadata:           mockMeta,
		storage:            &storage.Storage{},
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()
	err := gc.pruneBatch(ctx, []string{"snap1"})
	require.NoError(t, err)
}

// TestPruneBatch_ContinuesOnError tests that pruning continues when one snapshot fails.
func TestPruneBatch_ContinuesOnError(t *testing.T) {
	now := time.Now()
	mockMeta := metadata.NewMetadata(t)
	mockStore := storage.NewStorage(t)

	// snap1: validation fails.
	mockMeta.EXPECT().GetMetadata(mock.Anything, "snap1").Return(nil, errors.New("validation error"))

	// snap2: succeeds.
	mockMeta.EXPECT().GetMetadata(mock.Anything, "snap2").Return(&internalmetadata.MetadataEntry{
		LastAccessedAt: now.Add(-10 * time.Minute),
	}, nil)
	content := internalmetadata.File{Digest: "xxh3:xyz"}
	mockMeta.EXPECT().Prune(mock.Anything, "snap2").Return([]internalmetadata.File{content}, nil)
	mockStore.EXPECT().Prune(mock.Anything, "xyz").Return(nil)

	gc := &gc{
		metadata:           mockMeta,
		storage:            mockStore,
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()
	err := gc.pruneBatch(ctx, []string{"snap1", "snap2"})
	require.NoError(t, err)
}

// TestPruneBatch_ContextCancellation tests that pruning respects context cancellation.
func TestPruneBatch_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	gc := &gc{
		metadata:           &metadata.Metadata{},
		storage:            &storage.Storage{},
		minRetentionPeriod: 5 * time.Minute,
	}

	err := gc.pruneBatch(ctx, []string{"snap1"})
	assert.ErrorIs(t, err, context.Canceled)
}

// TestPruneBatch_StoragePruneError tests that storage prune errors are logged but don't fail the batch.
func TestPruneBatch_StoragePruneError(t *testing.T) {
	now := time.Now()
	mockMeta := metadata.NewMetadata(t)
	mockStore := storage.NewStorage(t)

	content := internalmetadata.File{Digest: "xxh3:abc"}

	mockMeta.EXPECT().GetMetadata(mock.Anything, "snap1").Return(&internalmetadata.MetadataEntry{
		LastAccessedAt: now.Add(-10 * time.Minute),
	}, nil)
	mockMeta.EXPECT().Prune(mock.Anything, "snap1").Return([]internalmetadata.File{content}, nil)
	mockStore.EXPECT().Prune(mock.Anything, "abc").Return(errors.New("storage error"))

	gc := &gc{
		metadata:           mockMeta,
		storage:            mockStore,
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()
	err := gc.pruneBatch(ctx, []string{"snap1"})
	require.NoError(t, err) // Should not fail even if storage prune fails
}

// TestPruneBatch_Concurrent tests concurrent pruning is safe.
func TestPruneBatch_Concurrent(t *testing.T) {
	now := time.Now()
	mockMeta := metadata.NewMetadata(t)
	mockStore := storage.NewStorage(t)

	// Setup expectations for multiple snapshots.
	for i := 1; i <= 10; i++ {
		key := "snap" + string(rune(i))
		content := internalmetadata.File{Digest: "xxh3:abc" + string(rune(i))}

		mockMeta.EXPECT().GetMetadata(mock.Anything, key).Return(&internalmetadata.MetadataEntry{
			LastAccessedAt: now.Add(-10 * time.Minute),
		}, nil).Maybe()
		mockMeta.EXPECT().Prune(mock.Anything, key).Return([]internalmetadata.File{content}, nil).Maybe()
		mockStore.EXPECT().Prune(mock.Anything, mock.Anything).Return(nil).Maybe()
	}

	gc := &gc{
		metadata:           mockMeta,
		storage:            mockStore,
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()

	// Run multiple pruning operations concurrently.
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			targets := []string{"snap" + string(rune(id*2-1)), "snap" + string(rune(id*2))}
			_ = gc.pruneBatch(ctx, targets)
		}(i)
	}

	wg.Wait()
}

// TestRun_IntegrationWithTempDir tests the full GC run with a real temporary directory.
func TestRun_IntegrationWithTempDir(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a file to ensure directory exists and has some content.
	testFile := filepath.Join(tempDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test data"), 0644)
	require.NoError(t, err)

	now := time.Now()
	mockMeta := metadata.NewMetadata(t)
	mockStore := storage.NewStorage(t)

	// Setup expectations.
	entries := []struct {
		key string
		ts  time.Time
	}{
		{"snap1", now.Add(-10 * time.Minute)},
		{"snap2", now.Add(-15 * time.Minute)},
	}

	mockMeta.EXPECT().IterateMetadata(mock.Anything, mock.Anything).Return(nil).Run(func(ctx context.Context, fn func(string, internalmetadata.MetadataEntry) error) {
		for _, e := range entries {
			_ = fn(e.key, internalmetadata.MetadataEntry{LastAccessedAt: e.ts})
		}
	})

	for _, e := range entries {
		mockMeta.EXPECT().GetMetadata(mock.Anything, e.key).Return(&internalmetadata.MetadataEntry{
			LastAccessedAt: e.ts,
		}, nil)
		mockMeta.EXPECT().Prune(mock.Anything, e.key).Return([]internalmetadata.File{}, nil)
	}

	gc := New(tempDir, mockMeta, mockStore,
		WithDiskHighThresholdPercent(0.0), // Set very low to trigger GC
		WithDiskLowThresholdPercent(0.0),
		WithMinRetentionPeriod(1*time.Minute),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Run GC once.
	gc.run(ctx)
}

// TestRun_ContextCancellation tests that Run respects context cancellation.
func TestRun_ContextCancellation(t *testing.T) {
	mockMeta := metadata.NewMetadata(t)
	mockStore := storage.NewStorage(t)

	gc := New("/tmp", mockMeta, mockStore)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Run should return quickly.
	done := make(chan struct{})
	go func() {
		gc.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Run did not respect context cancellation")
	}
}

// TestRun_StopsWhenBelowThreshold tests that run stops when disk usage is below threshold.
func TestRun_StopsWhenBelowThreshold(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mockMeta := metadata.NewMetadata(t)
	mockStore := storage.NewStorage(t)

	// IterateMetadata should not be called if disk usage is below threshold.
	mockMeta.AssertNotCalled(t, "IterateMetadata", mock.Anything, mock.Anything)

	gc := New(tempDir, mockMeta, mockStore,
		WithDiskHighThresholdPercent(99.0), // Very high threshold
	)

	ctx := context.Background()
	gc.run(ctx)
}

// BenchmarkCollectBatch benchmarks the collectBatch operation.
func BenchmarkCollectBatch(b *testing.B) {
	now := time.Now()
	mockMeta := metadata.NewMetadata(b)

	// Setup 1000 entries.
	mockMeta.EXPECT().IterateMetadata(mock.Anything, mock.Anything).Return(nil).Run(func(ctx context.Context, fn func(string, internalmetadata.MetadataEntry) error) {
		for i := range 1000 {
			_ = fn("key"+string(rune(i)), internalmetadata.MetadataEntry{
				LastAccessedAt: now.Add(-time.Duration(i) * time.Minute),
			})
		}
	}).Maybe()

	gc := &gc{
		metadata:           mockMeta,
		batchSize:          100,
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gc.collectBatch(ctx)
	}
}

// BenchmarkPruneBatch benchmarks the pruneBatch operation.
func BenchmarkPruneBatch(b *testing.B) {
	now := time.Now()
	mockMeta := metadata.NewMetadata(b)
	mockStore := storage.NewStorage(b)

	// Setup expectations.
	mockMeta.EXPECT().GetMetadata(mock.Anything, mock.Anything).Return(&internalmetadata.MetadataEntry{
		LastAccessedAt: now.Add(-10 * time.Minute),
	}, nil).Maybe()
	mockMeta.EXPECT().Prune(mock.Anything, mock.Anything).Return([]internalmetadata.File{}, nil).Maybe()

	gc := &gc{
		metadata:           mockMeta,
		storage:            mockStore,
		minRetentionPeriod: 5 * time.Minute,
	}

	ctx := context.Background()
	targets := []string{"snap1", "snap2", "snap3", "snap4", "snap5"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = gc.pruneBatch(ctx, targets)
	}
}
