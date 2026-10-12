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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"d7y.io/snapshotter/internal/gc"
	"d7y.io/snapshotter/internal/metadata"
	"d7y.io/snapshotter/internal/storage"
	"d7y.io/snapshotter/pkg/dragonfly"
	"d7y.io/snapshotter/pkg/oci"
	"d7y.io/snapshotter/pkg/sparsefile"
)

var (
	// ErrSnapshotAlreadyExists indicates that the snapshot already exists.
	ErrSnapshotAlreadyExists = errors.New("snapshot already exists")

	// ErrContentAlreadyExists indicates that the content already exists.
	ErrContentAlreadyExists = errors.New("content already exists")
)

const (
	// ContentEncodingSparseV1 identifies the sparse content encoding used by the snapshotter.
	ContentEncodingSparseV1 = "sparse-v1"

	// defaultSnapshotConcurrency is the default concurrency limit for snapshot operations.
	defaultSnapshotConcurrency = 5

	// defaultRestoreConcurrency is the default concurrency limit for restore operations.
	defaultRestoreConcurrency = 5

	// defaultDownloadTimeout bounds a shared content download so a stuck
	// transfer cannot hold its singleflight key forever.
	defaultDownloadTimeout = 30 * time.Minute

	standaloneContentMetadataPrefix = "__content__:"
)

// Option is a function that configures the snapshotter.
type Option func(*Config)

// Snapshotter is an interface that defines the methods for snapshotting and restoring files.
type Snapshotter interface {
	// Close releases resources owned by the snapshotter.
	Close() error

	// PutContent stores and uploads a file without publishing snapshot metadata to a registry.
	PutContent(ctx context.Context, baseDir string, file File, opts ...Option) (*ContentDescriptor, error)

	// GetContent downloads and restores content to outputPath without reading snapshot metadata from a registry.
	GetContent(ctx context.Context, descriptor ContentDescriptor, outputPath string, opts ...Option) error

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

// ContentDescriptor identifies content stored through Dragonfly.
type ContentDescriptor struct {
	// Digest is the content digest of the sparse-encoded file.
	Digest string `json:"digest"`

	// Size is the logical size of the original file in bytes.
	Size int64 `json:"size"`

	// Encoding identifies how the object is encoded in remote storage.
	Encoding string `json:"encoding"`

	// ReadOnly enables the decoded snapshot cache and hardlink-based export.
	ReadOnly bool `json:"readOnly"`

	// Mode is the source file's permission bits.
	Mode uint32 `json:"mode"`

	// ModTime is the source file's last modification time.
	ModTime time.Time `json:"mtime"`
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

	// dragonflyClient is shared by all operations so its gRPC connection is reused.
	dragonflyClient dragonfly.ProviderClient

	gcCancel  context.CancelFunc
	gcDone    chan struct{}
	closeOnce sync.Once
	closeErr  error

	// downloadTimeout bounds each shared content download flight.
	downloadTimeout time.Duration

	contentDownloads singleflight.Group
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
	closeMetadata := func(cause error) error {
		if closeErr := metadata.Close(); closeErr != nil {
			return errors.Join(cause, fmt.Errorf("failed to close metadata: %w", closeErr))
		}
		return cause
	}

	storage, err := storage.New(config.RootDir)
	if err != nil {
		return nil, closeMetadata(err)
	}

	dragonflyClient, err := dragonfly.New(config.Dragonfly, config.Content, config.RootDir)
	if err != nil {
		return nil, closeMetadata(err)
	}

	// Sweep staging directories orphaned by a previous crash.
	if err := cleanupOrphanStagingDirs(storage.ContentDir(), defaultDownloadTimeout); err != nil {
		slog.Warn("failed to clean up orphan staging directories", "error", err)
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

	// Run the gc service in background and retain its lifecycle controls.
	gc := gc.New(config.RootDir, metadata, storage, gcOpts...)
	gcCtx, gcCancel := context.WithCancel(context.Background())
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		gc.Run(gcCtx)
	}()

	return &snapshotter{
		config:              &config,
		snapshotConcurrency: snapshotConcurrency,
		restoreConcurrency:  restoreConcurrency,
		metadata:            metadata,
		storage:             storage,
		dragonflyClient:     dragonflyClient,
		gcCancel:            gcCancel,
		gcDone:              gcDone,
		downloadTimeout:     defaultDownloadTimeout,
	}, nil
}

// Close releases resources owned by the snapshotter.
func (s *snapshotter) Close() error {
	s.closeOnce.Do(func() {
		if s.gcCancel != nil {
			s.gcCancel()
		}
		if s.gcDone != nil {
			<-s.gcDone
		}

		var closeErrors []error
		if s.dragonflyClient != nil {
			if err := s.dragonflyClient.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close Dragonfly client: %w", err))
			}
		}
		if s.metadata != nil {
			if err := s.metadata.Close(); err != nil {
				closeErrors = append(closeErrors, fmt.Errorf("failed to close metadata: %w", err))
			}
		}
		s.closeErr = errors.Join(closeErrors...)
	})
	return s.closeErr
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
	config, err := s.configWithOptions(opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to copy config: %w", err)
	}

	registryClient, err := oci.New(config.RootDir, config.Metadata.Registry)
	if err != nil {
		return nil, nil, err
	}

	return registryClient, s.dragonflyClient.WithContent(config.Content), nil
}

func (s *snapshotter) initializeDragonfly(opts ...Option) (dragonfly.Client, error) {
	config, err := s.configWithOptions(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to copy config: %w", err)
	}

	return s.dragonflyClient.WithContent(config.Content), nil
}

func (s *snapshotter) configWithOptions(opts ...Option) (*Config, error) {
	config := &Config{}
	if err := copier.Copy(config, s.config); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(config)
	}
	return config, nil
}

// PutContent stores and uploads a file without publishing an OCI manifest.
func (s *snapshotter) PutContent(ctx context.Context, baseDir string, file File, opts ...Option) (*ContentDescriptor, error) {
	dragonflyClient, err := s.initializeDragonfly(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize: %w", err)
	}

	metadataFile, err := s.uploadContent(ctx, baseDir, file, dragonflyClient)
	if err != nil {
		return nil, err
	}
	if err := s.trackStandaloneContent(ctx, metadataFile); err != nil {
		return nil, err
	}

	return &ContentDescriptor{
		Digest:   metadataFile.Digest,
		Size:     metadataFile.Metadata.Size,
		Encoding: ContentEncodingSparseV1,
		ReadOnly: metadataFile.ReadOnly,
		Mode:     metadataFile.Metadata.Mode,
		ModTime:  metadataFile.Metadata.ModTime,
	}, nil
}

// GetContent downloads and restores content without reading an OCI manifest.
func (s *snapshotter) GetContent(ctx context.Context, descriptor ContentDescriptor, outputPath string, opts ...Option) error {
	dragonflyClient, err := s.initializeDragonfly(opts...)
	if err != nil {
		return fmt.Errorf("failed to initialize: %w", err)
	}

	return s.restoreContent(ctx, descriptor, outputPath, dragonflyClient)
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
			metadataFile, err := s.uploadContent(ctx, req.BaseDir, file, dragonflyClient)
			if err != nil {
				return err
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

func (s *snapshotter) uploadContent(ctx context.Context, baseDir string, file File, client dragonfly.Client) (metadata.File, error) {
	info, err := os.Stat(filepath.Join(baseDir, file.RelativePath))
	if err != nil {
		return metadata.File{}, fmt.Errorf("failed to stat file: %w", err)
	}
	fileMetadata, err := metadata.GetFileMetadata(info)
	if err != nil {
		return metadata.File{}, fmt.Errorf("failed to get file metadata: %w", err)
	}

	storageFile := storage.File{RelativePath: file.RelativePath, ReadOnly: file.ReadOnly}
	content, err := s.storage.StatContent(ctx, baseDir, storageFile)
	if err != nil {
		content, err = s.storage.WriteContent(ctx, baseDir, storageFile)
		if err != nil {
			return metadata.File{}, fmt.Errorf("failed to write content: %w", err)
		}
	}

	if file.ReadOnly {
		if _, err := s.storage.StatSnapshot(ctx, content.Name()); err != nil {
			if err := s.storage.HardlinkSnapshot(ctx, baseDir, storageFile, content.Name()); err != nil {
				return metadata.File{}, fmt.Errorf("failed to hardlink snapshot: %w", err)
			}
		}
	}

	digest := storage.ContentDigest(content.Name())
	if err := s.touchStandaloneContent(ctx, digest); err != nil {
		return metadata.File{}, err
	}
	if err := client.Upload(ctx, &dragonfly.UploadRequest{
		Digest:  digest,
		SrcPath: s.storage.GetContentPath(ctx, content.Name()),
	}); err != nil {
		return metadata.File{}, fmt.Errorf("failed to upload file: %w", err)
	}

	metadataFile := metadata.File{
		Digest:       digest,
		ReadOnly:     file.ReadOnly,
		RelativePath: file.RelativePath,
		Metadata:     fileMetadata,
	}
	if err := s.metadata.PutContent(ctx, metadataFile.Digest, metadataFile); err != nil {
		return metadata.File{}, fmt.Errorf("failed to store content metadata: %w", err)
	}
	return metadataFile, nil
}

func (s *snapshotter) restoreContent(ctx context.Context, descriptor ContentDescriptor, outputPath string, client dragonfly.Client) error {
	if err := validateContentDescriptor(descriptor); err != nil {
		return err
	}
	relativePath := filepath.Base(outputPath)
	if outputPath == "" {
		relativePath = storage.ParseFilenameFromDigest(descriptor.Digest)
	}

	file := metadata.File{
		Digest:       descriptor.Digest,
		ReadOnly:     descriptor.ReadOnly,
		RelativePath: relativePath,
		Metadata: metadata.FileMetadata{
			Size:    descriptor.Size,
			Mode:    descriptor.Mode,
			ModTime: descriptor.ModTime,
		},
	}
	if err := s.prepareContent(ctx, file, client); err != nil {
		return err
	}
	if err := s.trackStandaloneContent(ctx, file); err != nil {
		return err
	}
	if outputPath == "" {
		return nil
	}
	if err := s.exportContent(ctx, filepath.Dir(outputPath), file); err != nil {
		return err
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		return fmt.Errorf("failed to stat restored content: %w", err)
	}
	if info.Size() != descriptor.Size {
		if err := os.Remove(outputPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("restored content size %d does not match descriptor size %d; failed to remove invalid output %q: %w", info.Size(), descriptor.Size, outputPath, err)
		}
		return fmt.Errorf("restored content size %d does not match descriptor size %d", info.Size(), descriptor.Size)
	}
	return nil
}

func (s *snapshotter) touchStandaloneContent(ctx context.Context, digest string) error {
	key := standaloneContentMetadataPrefix + digest
	if _, err := s.metadata.GetMetadata(ctx, key); err == nil {
		if err := s.metadata.TouchMetadata(ctx, key); err != nil {
			return fmt.Errorf("failed to touch standalone content metadata: %w", err)
		}
		return nil
	} else if !errors.Is(err, metadata.ErrKeyNotFound) {
		return fmt.Errorf("failed to get standalone content metadata: %w", err)
	}
	return nil
}

func (s *snapshotter) trackStandaloneContent(ctx context.Context, file metadata.File) error {
	key := standaloneContentMetadataPrefix + file.Digest
	entry := metadata.MetadataEntry{
		CreatedAt:      time.Now(),
		LastAccessedAt: time.Now(),
		Config:         metadata.Config{Files: []metadata.File{file}},
	}
	if _, err := s.metadata.GetMetadata(ctx, key); err == nil {
		if err := s.metadata.TouchMetadata(ctx, key); err != nil {
			return fmt.Errorf("failed to touch standalone content metadata: %w", err)
		}
	} else if errors.Is(err, metadata.ErrKeyNotFound) {
		if err := s.metadata.PutMetadata(ctx, key, entry); err != nil {
			return fmt.Errorf("failed to store standalone content metadata: %w", err)
		}
	} else {
		return fmt.Errorf("failed to get standalone content metadata: %w", err)
	}
	if err := s.metadata.PutContentMetadata(ctx, key, entry); err != nil {
		return fmt.Errorf("failed to reference standalone content: %w", err)
	}
	return nil
}

func validateContentDescriptor(descriptor ContentDescriptor) error {
	algorithm, encoded, ok := strings.Cut(descriptor.Digest, ":")
	if !ok || algorithm != storage.ContentHashAlgorithm || encoded == "" || len(encoded) > 16 {
		return fmt.Errorf("invalid content digest %q", descriptor.Digest)
	}
	if _, err := strconv.ParseUint(encoded, 16, 64); err != nil {
		return fmt.Errorf("invalid content digest %q: %w", descriptor.Digest, err)
	}
	if descriptor.Size < 0 {
		return fmt.Errorf("invalid content size %d", descriptor.Size)
	}
	if descriptor.Encoding != ContentEncodingSparseV1 {
		return fmt.Errorf("unsupported content encoding %q", descriptor.Encoding)
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
			return s.exportContent(ctx, outputDir, file)
		})
	}

	return eg.Wait()
}

func (s *snapshotter) prepareContent(ctx context.Context, file metadata.File, client dragonfly.Client) error {
	if err := s.touchStandaloneContent(ctx, file.Digest); err != nil {
		return err
	}
	if err := s.ensureContent(ctx, file, client); err != nil {
		return err
	}

	if file.ReadOnly {
		filename := storage.ParseFilenameFromDigest(file.Digest)
		if _, err := s.storage.StatSnapshot(ctx, filename); err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to stat snapshot content: %w", err)
			}
			if err := s.storage.RestoreSnapshotFromContent(ctx, filename); err != nil {
				return fmt.Errorf("failed to restore snapshot from content: %w", err)
			}
		}
	}

	if err := s.metadata.PutContent(ctx, file.Digest, file); err != nil {
		return fmt.Errorf("failed to store content metadata: %w", err)
	}
	return nil
}

func (s *snapshotter) exportContent(ctx context.Context, outputDir string, file metadata.File) error {
	if _, err := s.metadata.GetContent(ctx, file.Digest); err != nil {
		return fmt.Errorf("failed to get content: %w", err)
	}
	if err := s.storage.Export(ctx, outputDir, file, storage.ParseFilenameFromDigest(file.Digest)); err != nil {
		return fmt.Errorf("failed to export file: %w", err)
	}
	return nil
}

func (s *snapshotter) ensureContent(ctx context.Context, file metadata.File, client dragonfly.Client) error {
	filename := storage.ParseFilenameFromDigest(file.Digest)
	if err := ctx.Err(); err != nil {
		return err
	}

	timeout := s.downloadTimeout
	if timeout <= 0 {
		timeout = defaultDownloadTimeout
	}
	result := s.contentDownloads.DoChan(filename, func() (any, error) {
		downloadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		return nil, s.downloadContent(downloadCtx, filename, file.Digest, client)
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-result:
		return result.Err
	}
}

func cleanupOrphanStagingDirs(contentDir string, maxAge time.Duration) error {
	// Staging directories left behind by a crashed process are not tracked
	// by the metadata store, so sweep them here based on their age.
	stagingDirs, err := filepath.Glob(filepath.Join(contentDir, storage.StagingDirPattern))
	if err != nil {
		return fmt.Errorf("failed to list staging directories: %w", err)
	}

	for _, stagingDir := range stagingDirs {
		info, err := os.Stat(stagingDir)
		if err != nil {
			if !os.IsNotExist(err) {
				slog.Warn("failed to stat orphan staging directory", "path", stagingDir, "error", err)
			}
			continue
		}

		if !info.IsDir() || time.Since(info.ModTime()) < maxAge {
			continue
		}

		if err := os.RemoveAll(stagingDir); err != nil {
			slog.Warn("failed to remove orphan staging directory", "path", stagingDir, "error", err)
		}
	}
	return nil
}

func prepareContentDownload(finalPath string) (string, func(), error) {
	stagingDir, err := os.MkdirTemp(filepath.Dir(finalPath), storage.StagingDirPattern)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create content download staging directory: %w", err)
	}
	if err := os.Chmod(stagingDir, storage.DirPerm); err != nil {
		_ = os.RemoveAll(stagingDir)
		return "", nil, fmt.Errorf("failed to set content download staging directory permissions: %w", err)
	}

	return filepath.Join(stagingDir, "content"), func() { _ = os.RemoveAll(stagingDir) }, nil
}

func publishContent(stagingPath, finalPath string) error {
	if err := os.Link(stagingPath, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("%w: %q", ErrContentAlreadyExists, finalPath)
		}
		return fmt.Errorf("failed to publish staged content as %q: %w", finalPath, err)
	}

	return nil
}

func (s *snapshotter) downloadContent(ctx context.Context, filename, digest string, client dragonfly.Client) error {
	finalPath := s.storage.GetContentPath(ctx, filename)
	if _, err := os.Stat(finalPath); err == nil {
		if err := validateContent(finalPath); err == nil {
			slog.Debug("file already exists, skip download", "path", finalPath)
			return nil
		}
		slog.Warn("cached content is invalid, removing it before re-downloading", "path", finalPath)
		if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove invalid cached content %q: %w", finalPath, err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat content %q: %w", finalPath, err)
	}

	stagingPath, cleanup, err := prepareContentDownload(finalPath)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := client.Download(ctx, &dragonfly.DownloadRequest{
		Digest:     digest,
		OutputPath: stagingPath,
	}); err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	if err := validateContent(stagingPath); err != nil {
		return fmt.Errorf("downloaded content %q is invalid: %w", stagingPath, err)
	}

	if err := publishContent(stagingPath, finalPath); err != nil {
		if !errors.Is(err, ErrContentAlreadyExists) {
			return fmt.Errorf("failed to publish downloaded content: %w", err)
		}
		if err := validateContent(finalPath); err == nil {
			return nil
		}
		slog.Warn("published content is invalid, replacing it with the validated download", "path", finalPath)
		if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove invalid published content %q: %w", finalPath, err)
		}
		if err := publishContent(stagingPath, finalPath); err != nil && !errors.Is(err, ErrContentAlreadyExists) {
			return fmt.Errorf("failed to publish downloaded content: %w", err)
		}
		if err := validateContent(finalPath); err != nil {
			return fmt.Errorf("published content %q is invalid: %w", finalPath, err)
		}
	}

	return nil
}

func validateContent(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return sparsefile.ValidateHeader(file)
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
			return s.prepareContent(ctx, file, dragonflyClient)
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
