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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"golang.org/x/sync/errgroup"

	"d7y.io/snapshotter/internal/gc"
	"d7y.io/snapshotter/internal/metadata"
	"d7y.io/snapshotter/internal/storage"
	"d7y.io/snapshotter/pkg/dragonfly"
	"d7y.io/snapshotter/pkg/oci"
)

var (
	// ErrSnapshotAlreadyExists indicates that the snapshot already exists.
	ErrSnapshotAlreadyExists = errors.New("snapshot already exists")
)

const (
	// defaultSnapshotConcurrency is the default concurrency limit for snapshot operations.
	defaultSnapshotConcurrency = 5

	// defaultRestoreConcurrency is the default concurrency limit for restore operations.
	defaultRestoreConcurrency = 5
)

// Option is a function that configures the snapshotter.
type Option func(*Config)

// Snapshotter is an interface that defines the methods for snapshotting and restoring files.
type Snapshotter interface {
	// Snapshot creates a snapshot of the specified files.
	Snapshot(ctx context.Context, req *SnapshotRequest, opts ...Option) error

	// Restore restores the specified files from the snapshot.
	Restore(ctx context.Context, req *RestoreRequest, opts ...Option) error
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

	// OutputDir is the directory where the files will be restored. If OutputDir is nil,
	// only the download is performed without exporting files.
	OutputDir *string
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

// WithMetadata configures the snapshotter with metadata.
func WithMetadata(metadata Metadata) Option {
	return func(c *Config) {
		c.Metadata = metadata
	}
}

// WithContent configures the snapshotter with content.
func WithContent(content dragonfly.ContentProvider) Option {
	return func(c *Config) {
		c.Content = content
	}
}

// snapshotter is the implementation of snapshotter.Snapshotter.
type snapshotter struct {
	// mu is the mutex for concurrent access.
	mu sync.Mutex

	// config is the configuration for the snapshotter.
	config *Config

	// snapshotConcurrency is the concurrency limit for snapshot operations.
	snapshotConcurrency int

	// restoreConcurrency is the concurrency limit for restore operations.
	restoreConcurrency int

	// metadata is the metadata instance for metadata operations.
	metadata metadata.Metadata

	// storage is the storage instance for storage operations.
	storage storage.Storage
}

// New creates a new snapshotter instance.
func New(config Config, opts ...Option) (Snapshotter, error) {
	for _, opt := range opts {
		opt(&config)
	}

	if err := validate(config); err != nil {
		return nil, err
	}

	snapshotConcurrency := defaultSnapshotConcurrency
	restoreConcurrency := defaultRestoreConcurrency

	if config.SnapshotConcurrency != nil {
		snapshotConcurrency = *config.SnapshotConcurrency
	}

	if config.RestoreConcurrency != nil {
		restoreConcurrency = *config.RestoreConcurrency
	}

	metadata, err := metadata.New(config.RootDir)
	if err != nil {
		return nil, err
	}

	storage, err := storage.New(config.RootDir)
	if err != nil {
		return nil, err
	}

	gcOpts := []gc.Option{}
	if interval := config.GC.Interval; interval != nil {
		gcOpts = append(gcOpts, gc.WithInterval(*interval))
	}

	if diskHighThresholdPercent := config.GC.DiskHighThresholdPercent; diskHighThresholdPercent != nil {
		gcOpts = append(gcOpts, gc.WithDiskHighThresholdPercent(*diskHighThresholdPercent))
	}

	if diskLowThresholdPercent := config.GC.DiskLowThresholdPercent; diskLowThresholdPercent != nil {
		gcOpts = append(gcOpts, gc.WithDiskLowThresholdPercent(*diskLowThresholdPercent))
	}

	if batchSize := config.GC.BatchSize; batchSize != nil {
		gcOpts = append(gcOpts, gc.WithBatchSize(*batchSize))
	}

	if minRetentionPeriod := config.GC.MinRetentionPeriod; minRetentionPeriod != nil {
		gcOpts = append(gcOpts, gc.WithMinRetentionPeriod(*minRetentionPeriod))
	}

	// Run the gc service in background.
	gc := gc.New(config.RootDir, metadata, storage, gcOpts...)
	go gc.Run(context.Background())

	return &snapshotter{
		config:              &config,
		snapshotConcurrency: snapshotConcurrency,
		restoreConcurrency:  restoreConcurrency,
		metadata:            metadata,
		storage:             storage,
	}, nil
}

// validate validates the configuration of the snapshotter.
func validate(config Config) error {
	if config.RootDir == "" {
		return errors.New("root directory is required")
	}

	return nil
}

// initialize initializes the snapshotter.
func (s *snapshotter) initialize(opts ...Option) (oci.Client, dragonfly.Client, error) {
	config := &Config{}
	if err := copier.Copy(&config, &s.config); err != nil {
		return nil, nil, fmt.Errorf("failed to copy config: %w", err)
	}

	for _, opt := range opts {
		opt(config)
	}

	registryClient, err := oci.New(config.RootDir, config.Metadata.Registry)
	if err != nil {
		return nil, nil, err
	}

	dragonflyClient, err := dragonfly.New(config.Dragonfly, config.Content, config.RootDir)
	if err != nil {
		return nil, nil, err
	}

	return registryClient, dragonflyClient, nil
}

// Snapshot creates a snapshot of the specified files.
func (s *snapshotter) Snapshot(ctx context.Context, req *SnapshotRequest, opts ...Option) error {
	registryClient, dragonflyClient, err := s.initialize(opts...)
	if err != nil {
		return fmt.Errorf("failed to initialize: %w", err)
	}

	// Forbid creating a snapshot if it already exists.
	if _, err := s.metadata.GetMetadata(ctx, metadata.Key(req.Name, req.Version)); err == nil {
		return ErrSnapshotAlreadyExists
	}

	config := metadata.Config{
		Files: make([]metadata.File, 0, len(req.Files)),
	}

	var (
		mu sync.Mutex
		eg errgroup.Group
	)
	eg.SetLimit(s.snapshotConcurrency)
	for _, file := range req.Files {
		eg.Go(func() error {
			info, err := os.Stat(filepath.Join(req.BaseDir, file.RelativePath))
			if err != nil {
				return fmt.Errorf("failed to stat file: %w", err)
			}

			// Retrieves the file metadata.
			fileMetadata, err := metadata.GetFileMetadata(info)
			if err != nil {
				return fmt.Errorf("failed to get file metadata: %w", err)
			}

			storageFile := storage.File{
				RelativePath: file.RelativePath,
				ReadOnly:     file.ReadOnly,
			}
			// Ensure the file exists in the storage.
			content, err := s.storage.StatContent(ctx, req.BaseDir, storageFile)
			if err != nil {
				// The content is not exist, then try to write it to the storage.
				content, err = s.storage.WriteContent(ctx, req.BaseDir, storageFile)
				if err != nil {
					return fmt.Errorf("failed to write content: %w", err)
				}

				// Hardlink the file to the snapshot for read only file.
				if file.ReadOnly {
					if err := s.storage.HardlinkSnapshot(ctx, req.BaseDir, storageFile, content.Name()); err != nil {
						return fmt.Errorf("failed to hardlink snapshot: %w", err)
					}
				}
			} else {
				// The content already exists, then check whether the snapshot exists for read only file.
				if file.ReadOnly {
					if _, err := s.storage.StatSnapshot(ctx, content.Name()); err != nil {
						// The snapshot not exists, then try to hardlink the file to the snapshot.
						if err := s.storage.HardlinkSnapshot(ctx, req.BaseDir, storageFile, content.Name()); err != nil {
							return fmt.Errorf("failed to hardlink snapshot: %w", err)
						}
					}
				}
			}

			// Upload the file to the dragonfly.
			digest := storage.ContentDigest(content.Name())
			if err := dragonflyClient.Upload(ctx, &dragonfly.UploadRequest{
				Digest:  digest,
				SrcPath: s.storage.GetContentPath(ctx, storage.ParseFilenameFromDigest(digest))}); err != nil {
				return fmt.Errorf("failed to upload file: %w", err)
			}

			metadataFile := metadata.File{
				ReadOnly:     file.ReadOnly,
				RelativePath: file.RelativePath,
				Metadata:     fileMetadata,
				Digest:       storage.ContentDigest(content.Name()),
			}

			// Store the content metadata.
			if err := s.metadata.PutContent(ctx, metadataFile.Digest, metadataFile); err != nil {
				return fmt.Errorf("failed to store content metadata: %w", err)
			}

			mu.Lock()
			config.Files = append(config.Files, metadataFile)
			mu.Unlock()

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	// Construct OCI artifact to store the metadata and push it to the registry.
	manifest, err := oci.BuildManifest(config)
	if err != nil {
		return fmt.Errorf("failed to build manifest: %w", err)
	}

	configBytes, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Push the config blob.
	if _, err = registryClient.PushBlob(ctx, req.Name, io.NopCloser(bytes.NewReader(configBytes))); err != nil {
		return fmt.Errorf("failed to push config blob: %w", err)
	}

	// Push the manifest blob.
	if _, err = registryClient.PushManifest(ctx, req.Name, req.Version, manifest); err != nil {
		return fmt.Errorf("failed to push manifest blob: %w", err)
	}

	metadataEntry := metadata.MetadataEntry{
		CreatedAt:      time.Now(),
		LastAccessedAt: time.Now(),
		Config:         config,
	}

	// Store the metadata in final.
	if err := s.metadata.PutMetadata(ctx, metadata.Key(req.Name, req.Version), metadataEntry); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	// Store the content metadata.
	if err := s.metadata.PutContentMetadata(ctx, metadata.Key(req.Name, req.Version), metadataEntry); err != nil {
		return fmt.Errorf("failed to store content metadata: %w", err)
	}

	return nil
}

// Restore restores the specified files from the snapshot.
func (s *snapshotter) Restore(ctx context.Context, req *RestoreRequest, opts ...Option) error {
	registryClient, dragonflyClient, err := s.initialize(opts...)
	if err != nil {
		return fmt.Errorf("failed to initialize: %w", err)
	}

	metadataEntry, err := s.metadata.GetMetadata(ctx, metadata.Key(req.Name, req.Version))
	if err != nil && !errors.Is(err, metadata.ErrKeyNotFound) {
		return err
	}

	// Retrieves from the remote if not found in local metadata.
	if metadataEntry == nil {
		metadataEntry, err = s.syncFromRemote(ctx, req.Name, req.Version, registryClient, dragonflyClient)
		if err != nil {
			return fmt.Errorf("failed to sync from remote: %w", err)
		}
	} else {
		// Touch the metadata for refreshing the last accessed time.
		if err := s.metadata.TouchMetadata(ctx, metadata.Key(req.Name, req.Version)); err != nil {
			slog.Warn("failed to touch metadata", "err", err)
		}
	}

	// Export the files to the output directory if specified.
	if req.OutputDir != nil {
		return s.export(ctx, *req.OutputDir, metadataEntry.Config.Files)
	}

	return nil
}

// export exports the files to the output directory.
func (s *snapshotter) export(ctx context.Context, outputDir string, files []metadata.File) error {
	var eg errgroup.Group
	eg.SetLimit(s.restoreConcurrency)
	for _, file := range files {
		eg.Go(func() error {
			if _, err := s.metadata.GetContent(ctx, file.Digest); err != nil {
				return fmt.Errorf("failed to get content: %w", err)
			}

			if err := s.storage.Export(ctx,
				outputDir,
				file,
				storage.ParseFilenameFromDigest(file.Digest)); err != nil {
				return fmt.Errorf("failed to export file: %w", err)
			}

			return nil
		})
	}

	return eg.Wait()
}

// sync restores the data from remote to local storage.
func (s *snapshotter) syncFromRemote(ctx context.Context, name, version string, registryClient oci.Client, dragonflyClient dragonfly.Client) (*metadata.MetadataEntry, error) {
	// Pull the manifest from registry to get the metadata.
	manifest, err := registryClient.PullManifest(ctx, name, version)
	if err != nil {
		return nil, fmt.Errorf("failed to pull manifest: %w", err)
	}

	// Pull the config blob from registry.
	configBlob, err := registryClient.PullBlob(ctx, name, manifest.Config.Digest.String())
	if err != nil {
		return nil, fmt.Errorf("failed to pull config blob: %w", err)
	}
	defer configBlob.Close()

	// Parse the config to retrieves the metadata.
	var config metadata.Config
	if err := json.NewDecoder(configBlob).Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	var eg errgroup.Group
	eg.SetLimit(s.restoreConcurrency)
	for _, file := range config.Files {
		eg.Go(func() error {
			outputPath := s.storage.GetContentPath(ctx, storage.ParseFilenameFromDigest(file.Digest))
			if _, err := os.Stat(outputPath); err == nil {
				slog.Debug("file already exists, skip download", "path", outputPath)
			} else {
				if err := dragonflyClient.Download(ctx, &dragonfly.DownloadRequest{
					Digest:     file.Digest,
					OutputPath: outputPath,
				}); err != nil {
					return fmt.Errorf("failed to download file: %w", err)
				}
			}

			// Restore the snapshot from content for read only file.
			if file.ReadOnly {
				if err := s.storage.RestoreSnapshotFromContent(ctx, storage.ParseFilenameFromDigest(file.Digest)); err != nil {
					return fmt.Errorf("failed to restore snapshot from content: %w", err)
				}
			}

			// Store the content metadata.
			if err := s.metadata.PutContent(ctx, file.Digest, file); err != nil {
				return fmt.Errorf("failed to store content metadata: %w", err)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	metadataEntry := metadata.MetadataEntry{
		CreatedAt:      time.Now(),
		LastAccessedAt: time.Now(),
		Config:         config,
	}
	// Store the metadata in final.
	if err := s.metadata.PutMetadata(ctx, metadata.Key(name, version), metadataEntry); err != nil {
		return nil, fmt.Errorf("failed to store metadata: %w", err)
	}

	return &metadataEntry, nil
}
