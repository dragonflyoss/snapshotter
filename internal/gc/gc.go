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
	"fmt"
	"log/slog"
	"sort"
	"time"

	"d7y.io/snapshotter/internal/metadata"
	"d7y.io/snapshotter/internal/storage"
	"d7y.io/snapshotter/pkg/fs"
)

// Option is a function that configures a GC instance.
type Option func(*gc)

// WithInterval sets the interval for garbage collection.
func WithInterval(interval time.Duration) Option {
	return func(g *gc) {
		g.interval = interval
	}
}

// WithDiskHighThresholdPercent sets the high threshold percentage for disk usage.
func WithDiskHighThresholdPercent(percent float64) Option {
	return func(g *gc) {
		if percent >= 0 && percent <= 100 {
			g.diskHighThresholdPercent = percent
		}
	}
}

// WithDiskLowThresholdPercent sets the low threshold percentage for disk usage.
func WithDiskLowThresholdPercent(percent float64) Option {
	return func(g *gc) {
		if percent >= 0 && percent <= 100 && percent <= g.diskHighThresholdPercent {
			g.diskLowThresholdPercent = percent
		}
	}
}

// WithBatchSize sets the number of snapshots to collect in each GC round.
func WithBatchSize(size int) Option {
	return func(g *gc) {
		if size > 0 {
			g.batchSize = size
		}
	}
}

// WithMinRetentionPeriod sets the minimum retention period for snapshots.
// Snapshots accessed within this period will not be eligible for GC.
func WithMinRetentionPeriod(period time.Duration) Option {
	return func(g *gc) {
		if period > 0 {
			g.minRetentionPeriod = period
		}
	}
}

const (
	// defaultGCInterval is the default interval for garbage collection.
	defaultGCInterval = 5 * time.Minute

	// defaultDiskHighThresholdPercent is the default percentage of disk usage that triggers garbage collection.
	defaultDiskHighThresholdPercent = 90.0

	// defaultDiskLowThresholdPercent is the default percentage of disk usage that triggers garbage collection.
	defaultDiskLowThresholdPercent = 70.0

	// defaultGCBatchSize is the default number of snapshots to collect in each GC round.
	defaultGCBatchSize = 10

	// defaultMinRetentionPeriod is the default minimum retention period for snapshots.
	// Snapshots accessed within this period will not be eligible for GC.
	defaultMinRetentionPeriod = 5 * time.Minute

	// maxGCAttempts is the maximum number of garbage collection attempts.
	maxGCAttempts = 100
)

// ErrNoCandidate indicates no GC candidate found.
var ErrNoCandidate = errors.New("no GC candidate found")

// New creates a new GC instance.
func New(rootDir string, metadata metadata.Metadata, storage storage.Storage, opts ...Option) *gc {
	gc := &gc{
		rootDir:                  rootDir,
		interval:                 defaultGCInterval,
		diskHighThresholdPercent: defaultDiskHighThresholdPercent,
		diskLowThresholdPercent:  defaultDiskLowThresholdPercent,
		batchSize:                defaultGCBatchSize,
		minRetentionPeriod:       defaultMinRetentionPeriod,
		metadata:                 metadata,
		storage:                  storage,
	}

	for _, opt := range opts {
		opt(gc)
	}

	return gc
}

// gc is the GC instance.
type gc struct {
	// rootDir is the root directory for snapshots.
	rootDir string

	// interval is the duration between garbage collection runs.
	interval time.Duration

	// diskHighThresholdPercent is the percentage of disk usage that triggers garbage collection.
	diskHighThresholdPercent float64

	// diskLowThresholdPercent is the percentage of disk usage that triggers garbage collection.
	diskLowThresholdPercent float64

	// batchSize is the number of snapshots to collect in each GC round.
	batchSize int

	// minRetentionPeriod is the minimum retention period for snapshots.
	// Snapshots accessed within this period will not be eligible for GC.
	minRetentionPeriod time.Duration

	// storage is the storage instance.
	storage storage.Storage

	// metadata is the metadata manager.
	metadata metadata.Metadata
}

// Run starts the garbage collection process.
func (g *gc) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("GC loop stopped due to context cancellation")
			return
		case <-time.After(g.interval):
			g.run(ctx)
		}
	}
}

func (g *gc) run(ctx context.Context) {
	diskUsage, err := fs.GetDiskUsage(g.rootDir)
	if err != nil {
		slog.Error("failed to get disk usage", "err", err)
		return
	}

	usedPercent := diskUsage.UsedPercent
	slog.Info("current disk usage",
		"used_percent", fmt.Sprintf("%.2f%%", usedPercent),
		"high_threshold", fmt.Sprintf("%.2f%%", g.diskHighThresholdPercent),
		"low_threshold", fmt.Sprintf("%.2f%%", g.diskLowThresholdPercent))

	if usedPercent < g.diskHighThresholdPercent {
		slog.Info("disk usage below high threshold, skipping GC")
		return
	}

	slog.Info("disk usage above high threshold, starting GC",
		"used_percent", fmt.Sprintf("%.2f%%", usedPercent))

	attempt := 0
	for usedPercent >= g.diskLowThresholdPercent {
		if err := ctx.Err(); err != nil {
			slog.Info("GC interrupted by context", "err", err)
			break
		}

		if attempt >= maxGCAttempts {
			slog.Warn("max GC attempts reached, stopping GC loop",
				"attempts", attempt,
				"used_percent", fmt.Sprintf("%.2f%%", usedPercent))
			break
		}

		candidates, err := g.collectBatch(ctx)
		if err != nil {
			if errors.Is(err, ErrNoCandidate) {
				slog.Info("no more GC candidates")
			} else {
				slog.Error("failed to collect GC candidates", "err", err)
			}
			break
		}

		if len(candidates) == 0 {
			slog.Info("no candidates returned, stopping GC")
			break
		}

		if err := g.pruneBatch(ctx, candidates); err != nil {
			slog.Error("batch prune failed", "err", err)
			break
		}

		attempt++

		// Refresh disk usage.
		diskUsage, err = fs.GetDiskUsage(g.rootDir)
		if err != nil {
			slog.Error("failed to refresh disk usage after prune", "err", err)
			break
		}
		usedPercent = diskUsage.UsedPercent
	}

	slog.Info("GC completed",
		"final_used_percent", fmt.Sprintf("%.2f%%", usedPercent),
		"attempts", attempt)
}

// collectBatch returns up to g.batchSize least recently used snapshot keys.
// Returns empty slice and ErrNoCandidate if no candidates exist.
// Only snapshots that are older than minRetentionPeriod are considered.
func (g *gc) collectBatch(ctx context.Context) ([]string, error) {
	type metadataEntryWithKey struct {
		key   string
		entry metadata.MetadataEntry
	}

	now := time.Now()
	minAccessTime := now.Add(-g.minRetentionPeriod)

	var entries []metadataEntryWithKey
	if err := g.metadata.IterateMetadata(ctx, func(key string, entry metadata.MetadataEntry) error {
		// Skip snapshots that were accessed recently (within retention period).
		if entry.LastAccessedAt.After(minAccessTime) {
			return nil
		}

		entries = append(entries, metadataEntryWithKey{
			key:   key,
			entry: entry,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, ErrNoCandidate
	}

	// Sort by LastAccessedAt (oldest first).
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entry.LastAccessedAt.Before(entries[j].entry.LastAccessedAt)
	})

	// Take at most batchSize.
	n := min(g.batchSize, len(entries))
	keys := make([]string, n)
	for i := range n {
		keys[i] = entries[i].key
	}

	slog.Info("collected GC candidates",
		"count", len(keys),
		"oldest_access", entries[0].entry.LastAccessedAt,
		"retention_cutoff", minAccessTime)

	return keys, nil
}

// pruneBatch removes multiple metadata entries and their content.
// Before pruning each target, it re-validates that the snapshot still exists
// and hasn't been recently accessed (to avoid TOCTOU issues).
func (g *gc) pruneBatch(ctx context.Context, targets []string) error {
	totalPruned := 0
	skipped := 0
	now := time.Now()
	minAccessTime := now.Add(-g.minRetentionPeriod)

	for _, target := range targets {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Re-validate: check if the snapshot still exists and is still eligible for GC.
		// This helps prevent TOCTOU race conditions where a snapshot might have been
		// accessed or deleted between collection and pruning.
		entry, err := g.metadata.GetMetadata(ctx, target)
		if err != nil {
			if errors.Is(err, metadata.ErrKeyNotFound) {
				slog.Debug("snapshot already deleted, skipping", "target", target)
				skipped++
				continue
			}
			slog.Warn("failed to re-validate snapshot, skipping",
				"target", target, "err", err)
			skipped++
			continue
		}

		// Double-check: ensure it's still outside the retention period.
		if entry.LastAccessedAt.After(minAccessTime) {
			slog.Info("snapshot was recently accessed, skipping GC",
				"target", target,
				"last_accessed", entry.LastAccessedAt,
				"retention_cutoff", minAccessTime)
			skipped++
			continue
		}

		// Prune the metadata and get list of content that no longer has references.
		prunedContent, err := g.metadata.Prune(ctx, target)
		if err != nil {
			slog.Warn("failed to prune metadata, skipping",
				"target", target, "err", err)
			skipped++
			continue
		}

		slog.Info("pruned metadata",
			"target", target,
			"content_count", len(prunedContent),
			"last_accessed", entry.LastAccessedAt)
		totalPruned += len(prunedContent)

		// Delete content files that are no longer referenced by any snapshot.
		for _, content := range prunedContent {
			filename := storage.ParseFilenameFromDigest(content.Digest)
			if err := g.storage.Prune(ctx, filename); err != nil {
				slog.Error("failed to prune content file",
					"digest", content.Digest, "err", err)
			} else {
				slog.Info("pruned content file", "digest", content.Digest)
			}
		}
	}

	slog.Info("batch GC completed",
		"targets_attempted", len(targets),
		"targets_pruned", len(targets)-skipped,
		"targets_skipped", skipped,
		"content_files_pruned", totalPruned)
	return nil
}
