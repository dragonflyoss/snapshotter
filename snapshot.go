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

	"d7y.io/snapshotter/internal/gc"
	"d7y.io/snapshotter/internal/metadata"
	"d7y.io/snapshotter/internal/storage"
	"d7y.io/snapshotter/pkg/dragonfly"
	"d7y.io/snapshotter/pkg/oci"
	"golang.org/x/sync/errgroup"
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

// snapshot is the implementation of snapshotter.Snapshotter.
type snapshot struct {
	// snapshotConcurrency is the concurrency limit for snapshot operations.
	snapshotConcurrency int

	// restoreConcurrency is the concurrency limit for restore operations.
	restoreConcurrency int

	// metadata is the metadata instance for metadata operations.
	metadata metadata.Metadata

	// storage is the storage instance for storage operations.
	storage storage.Storage

	// registry is the registry instance for registry operations.
	registry oci.Client

	// dragonfly is the dragonfly instance for dragonfly operations.
	dragonfly dragonfly.Client
}

// validate validates the configuration of the snapshotter.
func validate(config Config) error {
	if config.RootDir == "" {
		return errors.New("root directory is required")
	}

	if config.Metadata.Registry.Endpoint == "" {
		return errors.New("registry endpoint is required")
	}

	if config.Dragonfly.Endpoint == "" {
		return errors.New("dragonfly endpoint is required")
	}

	return nil
}

// NewSnapshot creates a new snapshotter instance.
func NewSnapshot(config Config) (*snapshot, error) {
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

	registry, err := oci.New(config.RootDir, config.Metadata.Registry)
	if err != nil {
		return nil, err
	}

	dragonfly, err := dragonfly.New(config.Dragonfly, config.Content)
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

	return &snapshot{
		snapshotConcurrency: snapshotConcurrency,
		restoreConcurrency:  restoreConcurrency,
		metadata:            metadata,
		storage:             storage,
		registry:            registry,
		dragonfly:           dragonfly,
	}, nil
}

// Snapshot creates a snapshot of the specified files.
func (s *snapshot) Snapshot(ctx context.Context, req *SnapshotRequest) error {
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
			if err := s.dragonfly.Upload(ctx, &dragonfly.UploadRequest{
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
	_, err = s.registry.PushBlob(ctx, req.Name, io.NopCloser(bytes.NewReader(configBytes)))
	if err != nil {
		return fmt.Errorf("failed to push config blob: %w", err)
	}

	// Push the manifest blob.
	_, err = s.registry.PushManifest(ctx, req.Name, req.Version, manifest)
	if err != nil {
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
func (s *snapshot) Restore(ctx context.Context, req *RestoreRequest) error {
	metadataEntry, err := s.metadata.GetMetadata(ctx, metadata.Key(req.Name, req.Version))
	if err != nil && !errors.Is(err, metadata.ErrKeyNotFound) {
		return err
	}

	// Retrieves from the remote if not found in local metadata.
	if metadataEntry == nil {
		metadataEntry, err = s.syncFromRemote(ctx, req.Name, req.Version)
		if err != nil {
			return fmt.Errorf("failed to sync from remote: %w", err)
		}
	} else {
		// Touch the metadata for refreshing the last accessed time.
		if err := s.metadata.TouchMetadata(ctx, metadata.Key(req.Name, req.Version)); err != nil {
			slog.Warn("failed to touch metadata", "err", err)
		}
	}

	var eg errgroup.Group
	eg.SetLimit(s.restoreConcurrency)

	for _, file := range metadataEntry.Config.Files {
		eg.Go(func() error {
			_, err := s.metadata.GetContent(ctx, file.Digest)
			if err != nil {
				return fmt.Errorf("failed to get content: %w", err)
			}

			if err := s.storage.Export(ctx,
				req.OutputDir,
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
func (s *snapshot) syncFromRemote(ctx context.Context, name, version string) (*metadata.MetadataEntry, error) {
	// Pull the manifest from registry to get the metadata.
	manifest, err := s.registry.PullManifest(ctx, name, version)
	if err != nil {
		return nil, fmt.Errorf("failed to pull manifest: %w", err)
	}

	// Pull the config blob from registry.
	configBlob, err := s.registry.PullBlob(ctx, name, manifest.Config.Digest.String())
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
			if err := s.dragonfly.Download(ctx, &dragonfly.DownloadRequest{
				Digest:     file.Digest,
				OutputPath: s.storage.GetContentPath(ctx, storage.ParseFilenameFromDigest(file.Digest)),
			}); err != nil {
				return fmt.Errorf("failed to download file: %w", err)
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
