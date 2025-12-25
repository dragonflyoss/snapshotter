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

package snapshotter

import (
	"context"
	"sync"
	"time"

	"d7y.io/snapshotter/pkg/dragonfly"
	"d7y.io/snapshotter/pkg/oci"
)

var (
	// instance is the singleton instance of the snapshotter.
	instance Snapshotter

	// mu protects instance initialization.
	mu sync.Mutex

	// initialized indicates whether the instance has been successfully initialized.
	initialized bool
)

// Snapshotter is an interface that defines the methods for snapshotting and restoring files.
type Snapshotter interface {
	// Snapshot creates a snapshot of the specified files.
	Snapshot(ctx context.Context, req *SnapshotRequest) error

	// Restore restores the specified files from the snapshot.
	Restore(ctx context.Context, req *RestoreRequest) error
}

// SnapshotRequest defines the request parameters for creating a snapshot.
type SnapshotRequest struct {
	// Name is the name of the snapshot.
	Name string

	// Version is the version of the snapshot.
	Version string

	// BaseDir is the base directory of the snapshot.
	BaseDir string

	// Files is the list of files to be included in the snapshot.
	Files []File
}

// RestoreRequest defines the request parameters for restoring files from a snapshot.
type RestoreRequest struct {
	// Name is the name of the snapshot.
	Name string

	// Version is the version of the snapshot.
	Version string

	// OutputDir is the directory where the files will be restored.
	OutputDir string
}

// Config defines the configuration for the snapshotter.
type Config struct {
	// GC is the garbage collection configuration for the snapshotter.
	GC GC

	// RootDir is the root directory for the snapshotter.
	RootDir string

	// Metadata is the metadata configuration for the snapshotter.
	Metadata Metadata

	// Content is the content configuration for the snapshotter.
	Content dragonfly.ContentProvider

	// Dragonfly is the Dragonfly configuration for the snapshotter.
	Dragonfly dragonfly.Dragonfly

	// SnapshotConcurrency is the maximum number of concurrent snapshots.
	SnapshotConcurrency *int

	// RestoreConcurrency is the maximum number of concurrent restores.
	RestoreConcurrency *int
}

// Metadata defines the metadata configuration for the snapshotter.
type Metadata struct {
	// Registry is the OCI registry configuration for the snapshotter.
	Registry oci.Registry
}

// File defines the file information for snapshotting and restoring.
type File struct {
	// RelativePath is the relative path of the file.
	RelativePath string

	// ReadOnly indicates whether the file is read-only.
	ReadOnly bool
}

// GC defines the garbage collection configuration for the snapshotter.
type GC struct {
	// Interval is the interval at which garbage collection is performed.
	Interval *time.Duration

	// DiskHighThresholdPercent is the percentage of disk space that triggers garbage collection.
	DiskHighThresholdPercent *float64

	// DiskLowThresholdPercent is the percentage of disk space that triggers garbage collection.
	DiskLowThresholdPercent *float64

	// BatchSize is the number of snapshots to process in each batch.
	BatchSize *int

	// MinRetentionPeriod is the minimum retention period for snapshots.
	MinRetentionPeriod *time.Duration
}

// New creates a new snapshotter instance using the singleton pattern.
// If initialization fails, subsequent calls can retry with a new config.
// Once successfully initialized, all subsequent calls return the same instance.
func New(config Config) (Snapshotter, error) {
	mu.Lock()
	defer mu.Unlock()

	// If already successfully initialized, return the existing instance.
	if initialized {
		return instance, nil
	}

	// Try to initialize.
	var err error
	instance, err = NewSnapshot(config)
	if err != nil {
		// Keep initialized as false, allowing retry on next call.
		return nil, err
	}

	// Mark as successfully initialized.
	initialized = true
	return instance, nil
}
