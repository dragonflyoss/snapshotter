package snapshotter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	godigest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/mock"

	"d7y.io/snapshotter/internal/metadata"
	"d7y.io/snapshotter/internal/storage"
	mocksdragonfly "d7y.io/snapshotter/mocks/dragonfly"
	"d7y.io/snapshotter/pkg/dragonfly"
	"d7y.io/snapshotter/pkg/sparsefile"
)

var errUnexpectedTestCall = errors.New("unexpected test call")

var errDownloadFailed = errors.New("download failed")

type restoreOCIClient struct {
	config string
}

func (c *restoreOCIClient) PullBlob(context.Context, string, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(c.config)), nil
}

func (c *restoreOCIClient) PullManifest(context.Context, string, string) (ocispec.Manifest, error) {
	return ocispec.Manifest{
		Config: ocispec.Descriptor{Digest: godigest.FromString(c.config)},
	}, nil
}

func (c *restoreOCIClient) PushBlob(context.Context, string, io.ReadCloser) (godigest.Digest, error) {
	return "", errUnexpectedTestCall
}

func (c *restoreOCIClient) PushManifest(context.Context, string, string, ocispec.Manifest) (godigest.Digest, error) {
	return "", errUnexpectedTestCall
}

// blockingDownload returns a Download implementation that writes incomplete
// placeholder bytes, signals started once, waits for release, and then encodes
// the source file into the output path.
func blockingDownload(sourcePath string, started chan<- struct{}, release <-chan struct{}) func(context.Context, *dragonfly.DownloadRequest) error {
	var once sync.Once
	return func(ctx context.Context, req *dragonfly.DownloadRequest) error {
		if err := os.WriteFile(req.OutputPath, make([]byte, 4), 0o644); err != nil {
			return err
		}
		once.Do(func() { close(started) })

		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}

		output, err := os.Create(req.OutputPath)
		if err != nil {
			return err
		}
		if err := sparsefile.Encode(sourcePath, output); err != nil {
			_ = output.Close()
			return err
		}
		return output.Close()
	}
}

// failingDownload writes incomplete placeholder bytes and then fails.
func failingDownload(_ context.Context, req *dragonfly.DownloadRequest) error {
	if err := os.WriteFile(req.OutputPath, make([]byte, 4), 0o644); err != nil {
		return err
	}
	return errDownloadFailed
}

func TestNewResourceLifecycle(t *testing.T) {
	t.Run("closes metadata when client creation fails", func(t *testing.T) {
		rootDir := t.TempDir()
		_, err := New(Config{
			RootDir: rootDir,
			Dragonfly: dragonfly.Dragonfly{
				Endpoint: "\x00",
			},
		})
		if err == nil {
			t.Fatal("New() error = nil, want invalid Dragonfly endpoint error")
		}

		metadataStore, err := metadata.New(rootDir)
		if err != nil {
			t.Fatalf("metadata.New() after failed New() error = %v", err)
		}
		if err := metadataStore.Close(); err != nil {
			t.Fatalf("metadata.Close() error = %v", err)
		}
	})

	t.Run("close is idempotent and releases metadata", func(t *testing.T) {
		rootDir := t.TempDir()
		s, err := New(Config{RootDir: rootDir})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("second Close() error = %v", err)
		}

		metadataStore, err := metadata.New(rootDir)
		if err != nil {
			t.Fatalf("metadata.New() after Close() error = %v", err)
		}
		if err := metadataStore.Close(); err != nil {
			t.Fatalf("metadata.Close() error = %v", err)
		}
	})
}

func TestSyncFromRemote(t *testing.T) {
	t.Run("keeps incomplete download private", func(t *testing.T) {
		// Given: one restore is downloading content while another requests the same digest.
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		metadataStore, err := metadata.New(rootDir)
		if err != nil {
			t.Fatalf("metadata.New() error = %v", err)
		}

		sourcePath := filepath.Join(rootDir, "source")
		if err := os.WriteFile(sourcePath, []byte("restored snapshot content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		file := metadata.File{Digest: "xxh3:0123456789abcdef", ReadOnly: true}
		config, err := json.Marshal(metadata.Config{Files: []metadata.File{file}})
		if err != nil {
			t.Fatalf("failed to marshal config: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		release := make(chan struct{})
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(blockingDownload(sourcePath, started, release))
		s := &snapshotter{
			metadata:           metadataStore,
			storage:            store,
			restoreConcurrency: 1,
		}

		results := make(chan error, 2)
		go func() {
			_, err := s.syncFromRemote(ctx, "snapshot", "version", &restoreOCIClient{config: string(config)}, client)
			results <- err
		}()
		<-started
		go func() {
			_, err := s.syncFromRemote(ctx, "snapshot", "version", &restoreOCIClient{config: string(config)}, client)
			results <- err
		}()

		// When: the active download has written incomplete data but has not returned EOF.
		finalPath := store.GetContentPath(ctx, storage.ParseFilenameFromDigest(file.Digest))
		_, statErr := os.Stat(finalPath)

		// Then: incomplete content stays private and both restores complete from published content.
		if !os.IsNotExist(statErr) {
			t.Fatalf("incomplete content is visible at final path: %v", statErr)
		}
		close(release)
		if err := <-results; err != nil {
			t.Fatalf("first syncFromRemote() error = %v", err)
		}
		if err := <-results; err != nil {
			t.Fatalf("syncFromRemote() error = %v", err)
		}
		if _, err := store.StatSnapshot(ctx, storage.ParseFilenameFromDigest(file.Digest)); err != nil {
			t.Fatalf("restored snapshot is not available: %v", err)
		}
	})

	t.Run("keeps failed download private", func(t *testing.T) {
		// Given: Dragonfly writes incomplete content and then fails the download.
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		metadataStore, err := metadata.New(rootDir)
		if err != nil {
			t.Fatalf("metadata.New() error = %v", err)
		}

		file := metadata.File{Digest: "xxh3:fedcba9876543210", ReadOnly: true}
		config, err := json.Marshal(metadata.Config{Files: []metadata.File{file}})
		if err != nil {
			t.Fatalf("failed to marshal config: %v", err)
		}
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(failingDownload)
		s := &snapshotter{
			metadata:           metadataStore,
			storage:            store,
			restoreConcurrency: 1,
		}

		// When: syncing the content fails after the staging path is written.
		_, syncErr := s.syncFromRemote(context.Background(), "snapshot", "version", &restoreOCIClient{config: string(config)}, client)

		// Then: the error is returned without publishing incomplete content.
		if !errors.Is(syncErr, errDownloadFailed) {
			t.Fatalf("syncFromRemote() error = %v, want %v", syncErr, errDownloadFailed)
		}
		finalPath := store.GetContentPath(context.Background(), storage.ParseFilenameFromDigest(file.Digest))
		if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
			t.Fatalf("failed content is visible at final path: %v", err)
		}
	})
}

func TestEnsureContent(t *testing.T) {
	t.Run("preserves existing digest filename parsing", func(t *testing.T) {
		// Given: a digest using the existing algorithm:filename parsing contract.
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		sourcePath := filepath.Join(rootDir, "source")
		if err := os.WriteFile(sourcePath, []byte("compatible digest content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		release := make(chan struct{})
		close(release)
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(blockingDownload(sourcePath, make(chan struct{}), release))
		s := &snapshotter{storage: store}

		// When: content is downloaded using that digest.
		err = s.ensureContent(context.Background(), metadata.File{Digest: "legacy:0123456789abcdef"}, client)

		// Then: the suffix remains the content filename, matching ParseFilenameFromDigest.
		if err != nil {
			t.Fatalf("ensureContent() error = %v", err)
		}
		filename := storage.ParseFilenameFromDigest("legacy:0123456789abcdef")
		if err := validateContent(store.GetContentPath(context.Background(), filename)); err != nil {
			t.Fatalf("published content is invalid: %v", err)
		}
	})

	t.Run("repairs invalid cached content", func(t *testing.T) {
		// Given a cached content file with an invalid header and a client that
		// can serve a valid download.
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		file := metadata.File{Digest: "xxh3:0011223344556677"}
		filename := storage.ParseFilenameFromDigest(file.Digest)
		finalPath := store.GetContentPath(context.Background(), filename)
		if err := os.WriteFile(finalPath, make([]byte, 4), 0o644); err != nil {
			t.Fatalf("failed to create incomplete cached content: %v", err)
		}
		sourcePath := filepath.Join(rootDir, "source")
		if err := os.WriteFile(sourcePath, []byte("repaired content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		release := make(chan struct{})
		close(release)
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(blockingDownload(sourcePath, make(chan struct{}, 1), release))
		s := &snapshotter{storage: store}

		// When ensuring the content.
		if err := s.ensureContent(context.Background(), file, client); err != nil {
			t.Fatalf("ensureContent() error = %v", err)
		}

		// Then the invalid cached file is replaced by a valid download.
		if err := validateContent(finalPath); err != nil {
			t.Fatalf("validateContent() error = %v", err)
		}
	})

	t.Run("repairs invalid published content when losing publish race", func(t *testing.T) {
		// Given a download in flight and an invalid file appearing at the
		// final path before the download publishes.
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		file := metadata.File{Digest: "xxh3:99aabbccddeeff00"}
		filename := storage.ParseFilenameFromDigest(file.Digest)
		finalPath := store.GetContentPath(context.Background(), filename)
		sourcePath := filepath.Join(rootDir, "source")
		if err := os.WriteFile(sourcePath, []byte("race repaired content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		started := make(chan struct{})
		release := make(chan struct{})
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(blockingDownload(sourcePath, started, release))
		s := &snapshotter{storage: store}
		result := make(chan error, 1)
		go func() {
			result <- s.ensureContent(context.Background(), file, client)
		}()
		<-started
		if err := os.WriteFile(finalPath, make([]byte, 4), 0o644); err != nil {
			t.Fatalf("failed to create invalid published content: %v", err)
		}
		close(release)

		// When the download finishes and publishing hits the existing file.
		if err := <-result; err != nil {
			t.Fatalf("ensureContent() error = %v", err)
		}

		// Then the invalid winner is replaced by the validated download.
		if err := validateContent(finalPath); err != nil {
			t.Fatalf("validateContent() error = %v", err)
		}
	})

	t.Run("singleflights concurrent download", func(t *testing.T) {
		// Given: one download is in progress when a second caller requests the same digest.
		rootDir := t.TempDir()
		baseStore, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		sourcePath := filepath.Join(rootDir, "source")
		if err := os.WriteFile(sourcePath, []byte("singleflight content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		started := make(chan struct{})
		release := make(chan struct{})
		var downloads atomic.Int32
		download := blockingDownload(sourcePath, started, release)
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *dragonfly.DownloadRequest) error {
			if downloads.Add(1) > 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return download(ctx, req)
		})
		s := &snapshotter{storage: baseStore}
		file := metadata.File{Digest: "xxh3:1122334455667788"}
		leaderResult := make(chan error, 1)
		go func() {
			leaderResult <- s.ensureContent(context.Background(), file, client)
		}()
		<-started

		followerCtx, cancelFollower := context.WithCancel(context.Background())
		followerResult := make(chan error, 1)
		go func() {
			followerResult <- s.ensureContent(followerCtx, file, client)
		}()

		// When: the follower is canceled while the leader still owns the download.
		cancelFollower()
		followerErr := <-followerResult

		// Then: the follower never starts another download and the leader still publishes the content.
		if !errors.Is(followerErr, context.Canceled) {
			t.Fatalf("follower ensureContent() error = %v, want %v", followerErr, context.Canceled)
		}
		if got := downloads.Load(); got != 1 {
			t.Fatalf("Download() calls = %d, want 1", got)
		}
		close(release)
		if err := <-leaderResult; err != nil {
			t.Fatalf("leader ensureContent() error = %v", err)
		}
	})

	t.Run("keeps flight alive when leader is canceled", func(t *testing.T) {
		// Given: a follower with a valid context joins a download started by another caller.
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		sourcePath := filepath.Join(rootDir, "source")
		if err := os.WriteFile(sourcePath, []byte("surviving follower content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		started := make(chan struct{})
		release := make(chan struct{})
		var downloads atomic.Int32
		download := blockingDownload(sourcePath, started, release)
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *dragonfly.DownloadRequest) error {
			if downloads.Add(1) > 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return download(ctx, req)
		})
		s := &snapshotter{storage: store}
		file := metadata.File{Digest: "xxh3:1234567890abcdef"}
		leaderCtx, cancelLeader := context.WithCancel(context.Background())
		leaderResult := make(chan error, 1)
		go func() {
			leaderResult <- s.ensureContent(leaderCtx, file, client)
		}()
		<-started
		followerResult := make(chan error, 1)
		go func() {
			followerResult <- s.ensureContent(context.Background(), file, client)
		}()

		// When: the caller that started the flight is canceled before the download completes.
		cancelLeader()
		if err := <-leaderResult; !errors.Is(err, context.Canceled) {
			t.Fatalf("leader ensureContent() error = %v, want %v", err, context.Canceled)
		}
		close(release)

		// Then: the valid follower still receives the completed shared download.
		if err := <-followerResult; err != nil {
			t.Fatalf("follower ensureContent() error = %v", err)
		}
		if got := downloads.Load(); got != 1 {
			t.Fatalf("Download() calls = %d, want 1", got)
		}
	})

	t.Run("retries after singleflight failure", func(t *testing.T) {
		// Given: the first download for a digest fails after writing incomplete staging content.
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		s := &snapshotter{storage: store}
		file := metadata.File{Digest: "xxh3:8877665544332211"}
		failing := mocksdragonfly.NewClient(t)
		failing.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(failingDownload)
		if err := s.ensureContent(context.Background(), file, failing); !errors.Is(err, errDownloadFailed) {
			t.Fatalf("first ensureContent() error = %v, want %v", err, errDownloadFailed)
		}

		sourcePath := filepath.Join(rootDir, "source")
		if err := os.WriteFile(sourcePath, []byte("retry content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		release := make(chan struct{})
		close(release)
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(blockingDownload(sourcePath, make(chan struct{}), release))

		// When: a later request retries the same digest.
		err = s.ensureContent(context.Background(), file, client)

		// Then: the failed flight was removed and the retry publishes valid content.
		if err != nil {
			t.Fatalf("retry ensureContent() error = %v", err)
		}
		filename := storage.ParseFilenameFromDigest(file.Digest)
		if err := validateContent(store.GetContentPath(context.Background(), filename)); err != nil {
			t.Fatalf("published content is invalid: %v", err)
		}
	})

	t.Run("times out stuck download", func(t *testing.T) {
		// Given: a snapshotter with a short download timeout and a client that never finishes.
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		sourcePath := filepath.Join(rootDir, "source")
		if err := os.WriteFile(sourcePath, []byte("timed out content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		stuck := mocksdragonfly.NewClient(t)
		stuck.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(blockingDownload(sourcePath, make(chan struct{}, 1), make(chan struct{})))
		s := &snapshotter{storage: store, downloadTimeout: 50 * time.Millisecond}
		file := metadata.File{Digest: "xxh3:aabbccdd00112233"}

		// When: the download never completes within the timeout.
		err = s.ensureContent(context.Background(), file, stuck)

		// Then: the flight fails with a deadline error and the key can be retried.
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ensureContent() error = %v, want %v", err, context.DeadlineExceeded)
		}
		release := make(chan struct{})
		close(release)
		healthy := mocksdragonfly.NewClient(t)
		healthy.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(blockingDownload(sourcePath, make(chan struct{}, 1), release))
		if err := s.ensureContent(context.Background(), file, healthy); err != nil {
			t.Fatalf("retry ensureContent() error = %v", err)
		}
	})
}

func TestPublishContent(t *testing.T) {
	t.Run("publishes completed download without copying", func(t *testing.T) {
		// Given: a completed download in staging storage.
		store, err := storage.New(t.TempDir())
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		finalPath := store.GetContentPath(context.Background(), "content-digest")
		stagingPath, cleanup, err := prepareContentDownload(finalPath)
		if err != nil {
			t.Fatalf("prepareContentDownload() error = %v", err)
		}
		defer cleanup()

		if err := os.WriteFile(stagingPath, []byte("SPF1-complete-content"), 0o644); err != nil {
			t.Fatalf("failed to write staged content: %v", err)
		}
		stagingInfo, err := os.Stat(stagingPath)
		if err != nil {
			t.Fatalf("failed to stat staged content: %v", err)
		}
		if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
			t.Fatalf("final content exists before publish: %v", err)
		}

		// When: the completed download is published.
		if err := publishContent(stagingPath, finalPath); err != nil {
			t.Fatalf("publishContent() error = %v", err)
		}

		// Then: the final path contains the same inode without copying data.
		finalInfo, err := os.Stat(finalPath)
		if err != nil {
			t.Fatalf("failed to stat final content: %v", err)
		}
		if !os.SameFile(stagingInfo, finalInfo) {
			t.Fatal("published content does not reference the staged inode")
		}
	})

	t.Run("returns error when final content already exists", func(t *testing.T) {
		dir := t.TempDir()
		finalPath := filepath.Join(dir, "content")
		if err := os.WriteFile(finalPath, []byte("winner"), 0o644); err != nil {
			t.Fatalf("failed to write winning content: %v", err)
		}
		stagingPath := filepath.Join(dir, "staging")
		if err := os.WriteFile(stagingPath, []byte("late publisher"), 0o644); err != nil {
			t.Fatalf("failed to write staged content: %v", err)
		}

		if err := publishContent(stagingPath, finalPath); !errors.Is(err, ErrContentAlreadyExists) {
			t.Fatalf("publishContent() error = %v, want %v", err, ErrContentAlreadyExists)
		}
		content, err := os.ReadFile(finalPath)
		if err != nil {
			t.Fatalf("failed to read final content: %v", err)
		}
		if string(content) != "winner" {
			t.Fatalf("final content = %q, want winner", content)
		}
	})
}

func TestPrepareContentDownload(t *testing.T) {
	t.Run("creates staging directory traversable by daemon", func(t *testing.T) {
		finalPath := filepath.Join(t.TempDir(), "content")
		stagingPath, cleanup, err := prepareContentDownload(finalPath)
		if err != nil {
			t.Fatalf("prepareContentDownload() error = %v", err)
		}
		defer cleanup()

		info, err := os.Stat(filepath.Dir(stagingPath))
		if err != nil {
			t.Fatalf("failed to stat staging directory: %v", err)
		}
		if info.Mode().Perm() != storage.DirPerm {
			t.Fatalf("staging directory mode = %o, want %o", info.Mode().Perm(), storage.DirPerm)
		}
	})
}

func TestUploadContent(t *testing.T) {
	rootDir := t.TempDir()
	store, err := storage.New(rootDir)
	if err != nil {
		t.Fatalf("storage.New() error = %v", err)
	}
	metadataStore, err := metadata.New(rootDir)
	if err != nil {
		t.Fatalf("metadata.New() error = %v", err)
	}

	baseDir := t.TempDir()
	const relativePath = "rootfs.img"
	sourcePath := filepath.Join(baseDir, relativePath)
	source := []byte("standalone rootfs content")
	if err := os.WriteFile(sourcePath, source, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	client := mocksdragonfly.NewClient(t)
	client.EXPECT().Upload(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *dragonfly.UploadRequest) error {
		if !strings.HasPrefix(req.Digest, "xxh3:") {
			t.Fatalf("Upload() digest = %q, want xxh3 digest", req.Digest)
		}
		if err := validateContent(req.SrcPath); err != nil {
			t.Fatalf("uploaded content is not sparse encoded: %v", err)
		}
		return nil
	})

	s := &snapshotter{metadata: metadataStore, storage: store}
	file, err := s.uploadContent(context.Background(), baseDir, File{
		RelativePath: relativePath,
		ReadOnly:     true,
	}, client)
	if err != nil {
		t.Fatalf("uploadContent() error = %v", err)
	}
	if file.Metadata.Size != int64(len(source)) {
		t.Fatalf("content size = %d, want %d", file.Metadata.Size, len(source))
	}
	if !file.ReadOnly {
		t.Fatal("content is not marked read-only")
	}

	filename := storage.ParseFilenameFromDigest(file.Digest)
	snapshotInfo, err := store.StatSnapshot(context.Background(), filename)
	if err != nil {
		t.Fatalf("StatSnapshot() error = %v", err)
	}
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		t.Fatalf("failed to stat source: %v", err)
	}
	if !os.SameFile(sourceInfo, snapshotInfo) {
		t.Fatal("read-only snapshot does not hardlink the source")
	}
	if err := s.trackStandaloneContent(context.Background(), file); err != nil {
		t.Fatalf("trackStandaloneContent() error = %v", err)
	}
	contentMetadata, err := metadataStore.GetContentMetadata(context.Background(), file.Digest)
	if err != nil {
		t.Fatalf("GetContentMetadata() error = %v", err)
	}
	if len(contentMetadata.References) != 1 || contentMetadata.References[0] != standaloneContentMetadataPrefix+file.Digest {
		t.Fatalf("content references = %v, want standalone reference", contentMetadata.References)
	}
}

func TestPutContentPreservesMetadata(t *testing.T) {
	rootDir := t.TempDir()
	store, err := storage.New(rootDir)
	if err != nil {
		t.Fatalf("storage.New() error = %v", err)
	}
	metadataStore, err := metadata.New(rootDir)
	if err != nil {
		t.Fatalf("metadata.New() error = %v", err)
	}

	baseDir := t.TempDir()
	const relativePath = "rootfs.img"
	sourcePath := filepath.Join(baseDir, relativePath)
	if err := os.WriteFile(sourcePath, []byte("standalone rootfs content"), 0o640); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}
	wantModTime := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(sourcePath, wantModTime, wantModTime); err != nil {
		t.Fatalf("failed to set source mtime: %v", err)
	}

	provider := dragonfly.ContentProvider{Provider: "oss", Bucket: "bucket"}
	client := mocksdragonfly.NewProviderClient(t)
	client.EXPECT().WithContent(provider).Return(client)
	client.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	s := &snapshotter{
		config:          &Config{Content: provider},
		metadata:        metadataStore,
		storage:         store,
		dragonflyClient: client,
	}

	descriptor, err := s.PutContent(context.Background(), baseDir, File{
		RelativePath: relativePath,
		ReadOnly:     true,
	})
	if err != nil {
		t.Fatalf("PutContent() error = %v", err)
	}
	if descriptor.Mode != 0o640 {
		t.Fatalf("descriptor mode = %o, want 640", descriptor.Mode)
	}
	if !descriptor.ModTime.Equal(wantModTime) {
		t.Fatalf("descriptor mtime = %v, want %v", descriptor.ModTime, wantModTime)
	}
}

func TestRestoreContentWithoutOutputPreheatsCache(t *testing.T) {
	rootDir := t.TempDir()
	store, err := storage.New(rootDir)
	if err != nil {
		t.Fatalf("storage.New() error = %v", err)
	}
	metadataStore, err := metadata.New(rootDir)
	if err != nil {
		t.Fatalf("metadata.New() error = %v", err)
	}

	sourcePath := filepath.Join(t.TempDir(), "rootfs.img")
	source := []byte("preheated standalone rootfs")
	if err := os.WriteFile(sourcePath, source, 0o644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}
	release := make(chan struct{})
	close(release)
	client := mocksdragonfly.NewClient(t)
	client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
		blockingDownload(sourcePath, make(chan struct{}, 1), release),
	)

	s := &snapshotter{metadata: metadataStore, storage: store}
	descriptor := ContentDescriptor{
		Digest:   "xxh3:0123456789abcdef",
		Size:     int64(len(source)),
		Encoding: ContentEncodingSparseV1,
		ReadOnly: true,
	}
	if err := s.restoreContent(context.Background(), descriptor, "", client); err != nil {
		t.Fatalf("restoreContent() error = %v", err)
	}

	filename := storage.ParseFilenameFromDigest(descriptor.Digest)
	if _, err := store.StatSnapshot(context.Background(), filename); err != nil {
		t.Fatalf("preheated snapshot cache missing: %v", err)
	}
	contentMetadata, err := metadataStore.GetContentMetadata(context.Background(), descriptor.Digest)
	if err != nil {
		t.Fatalf("GetContentMetadata() error = %v", err)
	}
	if len(contentMetadata.References) != 1 || contentMetadata.References[0] != standaloneContentMetadataPrefix+descriptor.Digest {
		t.Fatalf("content references = %v, want standalone reference", contentMetadata.References)
	}
}

func TestStandaloneContentReferenceLifecycle(t *testing.T) {
	t.Run("touches existing reference before download completes", func(t *testing.T) {
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		metadataStore, err := metadata.New(rootDir)
		if err != nil {
			t.Fatalf("metadata.New() error = %v", err)
		}

		file := metadata.File{
			Digest:   "xxh3:0123456789abcdef",
			ReadOnly: false,
		}
		key := standaloneContentMetadataPrefix + file.Digest
		oldAccessTime := time.Unix(1, 0)
		entry := metadata.MetadataEntry{
			CreatedAt:      oldAccessTime,
			LastAccessedAt: oldAccessTime,
			Config:         metadata.Config{Files: []metadata.File{file}},
		}
		if err := metadataStore.PutMetadata(context.Background(), key, entry); err != nil {
			t.Fatalf("PutMetadata() error = %v", err)
		}
		if err := metadataStore.PutContentMetadata(context.Background(), key, entry); err != nil {
			t.Fatalf("PutContentMetadata() error = %v", err)
		}

		sourcePath := filepath.Join(t.TempDir(), "rootfs.img")
		if err := os.WriteFile(sourcePath, []byte("standalone rootfs content"), 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		started := make(chan struct{})
		release := make(chan struct{})
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
			blockingDownload(sourcePath, started, release),
		)
		s := &snapshotter{metadata: metadataStore, storage: store}
		result := make(chan error, 1)
		go func() {
			result <- s.prepareContent(context.Background(), file, client)
		}()

		<-started
		updated, err := metadataStore.GetMetadata(context.Background(), key)
		if err != nil {
			close(release)
			t.Fatalf("GetMetadata() error = %v", err)
		}
		if !updated.LastAccessedAt.After(oldAccessTime) {
			close(release)
			t.Fatalf("LastAccessedAt = %v, want after %v", updated.LastAccessedAt, oldAccessTime)
		}

		close(release)
		if err := <-result; err != nil {
			t.Fatalf("prepareContent() error = %v", err)
		}
	})

	t.Run("does not create reference when preparation fails", func(t *testing.T) {
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		metadataStore, err := metadata.New(rootDir)
		if err != nil {
			t.Fatalf("metadata.New() error = %v", err)
		}

		file := metadata.File{Digest: "xxh3:fedcba9876543210"}
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(failingDownload)
		s := &snapshotter{metadata: metadataStore, storage: store}

		err = s.restoreContent(context.Background(), ContentDescriptor{
			Digest:   file.Digest,
			Encoding: ContentEncodingSparseV1,
		}, "", client)
		if !errors.Is(err, errDownloadFailed) {
			t.Fatalf("restoreContent() error = %v, want %v", err, errDownloadFailed)
		}
		if _, err := metadataStore.GetMetadata(context.Background(), standaloneContentMetadataPrefix+file.Digest); !errors.Is(err, metadata.ErrKeyNotFound) {
			t.Fatalf("standalone reference exists after failed preparation: %v", err)
		}
	})
}

func TestRestoreContent(t *testing.T) {
	t.Run("downloads and exports read-only content", func(t *testing.T) {
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		metadataStore, err := metadata.New(rootDir)
		if err != nil {
			t.Fatalf("metadata.New() error = %v", err)
		}

		sourcePath := filepath.Join(t.TempDir(), "rootfs.img")
		source := []byte("restored standalone rootfs")
		if err := os.WriteFile(sourcePath, source, 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		release := make(chan struct{})
		close(release)
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
			blockingDownload(sourcePath, make(chan struct{}, 1), release),
		)

		s := &snapshotter{metadata: metadataStore, storage: store}
		descriptor := ContentDescriptor{
			Digest:   "xxh3:0123456789abcdef",
			Size:     int64(len(source)),
			Encoding: ContentEncodingSparseV1,
			ReadOnly: true,
			Mode:     0o640,
			ModTime:  time.Unix(1_700_000_000, 0),
		}
		outputPath := filepath.Join(t.TempDir(), "checkpoint", "rootfs.img")
		if err := s.restoreContent(context.Background(), descriptor, outputPath, client); err != nil {
			t.Fatalf("restoreContent() error = %v", err)
		}

		got, err := os.ReadFile(outputPath)
		if err != nil {
			t.Fatalf("failed to read restored content: %v", err)
		}
		if string(got) != string(source) {
			t.Fatalf("restored content = %q, want %q", got, source)
		}
		filename := storage.ParseFilenameFromDigest(descriptor.Digest)
		snapshotInfo, err := store.StatSnapshot(context.Background(), filename)
		if err != nil {
			t.Fatalf("StatSnapshot() error = %v", err)
		}
		outputInfo, err := os.Stat(outputPath)
		if err != nil {
			t.Fatalf("failed to stat restored content: %v", err)
		}
		if !os.SameFile(snapshotInfo, outputInfo) {
			t.Fatal("restored read-only content does not use the snapshot hardlink")
		}
		if outputInfo.Mode().Perm() != os.FileMode(descriptor.Mode) {
			t.Fatalf("restored mode = %o, want %o", outputInfo.Mode().Perm(), descriptor.Mode)
		}
		if !outputInfo.ModTime().Equal(descriptor.ModTime) {
			t.Fatalf("restored mtime = %v, want %v", outputInfo.ModTime(), descriptor.ModTime)
		}
	})

	t.Run("removes output on size mismatch", func(t *testing.T) {
		rootDir := t.TempDir()
		store, err := storage.New(rootDir)
		if err != nil {
			t.Fatalf("storage.New() error = %v", err)
		}
		metadataStore, err := metadata.New(rootDir)
		if err != nil {
			t.Fatalf("metadata.New() error = %v", err)
		}

		sourcePath := filepath.Join(t.TempDir(), "rootfs.img")
		source := []byte("restored standalone rootfs")
		if err := os.WriteFile(sourcePath, source, 0o644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		release := make(chan struct{})
		close(release)
		client := mocksdragonfly.NewClient(t)
		client.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
			blockingDownload(sourcePath, make(chan struct{}, 1), release),
		)

		s := &snapshotter{metadata: metadataStore, storage: store}
		descriptor := ContentDescriptor{
			Digest:   "xxh3:0123456789abcdef",
			Size:     int64(len(source) + 1),
			Encoding: ContentEncodingSparseV1,
			ReadOnly: true,
		}
		outputPath := filepath.Join(t.TempDir(), "checkpoint", "rootfs.img")
		err = s.restoreContent(context.Background(), descriptor, outputPath, client)
		if err == nil || !strings.Contains(err.Error(), "does not match descriptor size") {
			t.Fatalf("restoreContent() error = %v, want size mismatch", err)
		}
		if _, statErr := os.Stat(outputPath); !os.IsNotExist(statErr) {
			t.Fatalf("invalid restored output still exists: %v", statErr)
		}
	})

	t.Run("rejects unsupported encoding", func(t *testing.T) {
		client := mocksdragonfly.NewClient(t)
		s := &snapshotter{}
		err := s.restoreContent(context.Background(), ContentDescriptor{
			Digest:   "xxh3:0123456789abcdef",
			Encoding: "unknown",
		}, filepath.Join(t.TempDir(), "rootfs.img"), client)
		if err == nil || !strings.Contains(err.Error(), "unsupported content encoding") {
			t.Fatalf("restoreContent() error = %v, want unsupported encoding", err)
		}
	})

	t.Run("rejects unsafe digest", func(t *testing.T) {
		client := mocksdragonfly.NewClient(t)
		s := &snapshotter{}
		err := s.restoreContent(context.Background(), ContentDescriptor{
			Digest:   "xxh3:../../rootfs",
			Encoding: ContentEncodingSparseV1,
		}, filepath.Join(t.TempDir(), "rootfs.img"), client)
		if err == nil || !strings.Contains(err.Error(), "invalid content digest") {
			t.Fatalf("restoreContent() error = %v, want invalid digest", err)
		}
	})
}

func TestCleanupOrphanStagingDirs(t *testing.T) {
	t.Run("removes stale staging directories", func(t *testing.T) {
		// Given a content directory with a stale staging dir, a fresh
		// staging dir, and unrelated entries.
		dir := t.TempDir()
		staleDir := filepath.Join(dir, ".download-old")
		if err := os.Mkdir(staleDir, 0o755); err != nil {
			t.Fatalf("Mkdir() error = %v", err)
		}
		if err := os.Chtimes(staleDir, time.Now().Add(-2*time.Hour), time.Now().Add(-2*time.Hour)); err != nil {
			t.Fatalf("Chtimes() error = %v", err)
		}
		freshDir := filepath.Join(dir, ".download-new")
		if err := os.Mkdir(freshDir, 0o755); err != nil {
			t.Fatalf("Mkdir() error = %v", err)
		}
		keepFile := filepath.Join(dir, "keep.txt")
		if err := os.WriteFile(keepFile, []byte("keep"), 0o644); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		otherDir := filepath.Join(dir, "other")
		if err := os.Mkdir(otherDir, 0o755); err != nil {
			t.Fatalf("Mkdir() error = %v", err)
		}

		// When cleaning up staging directories older than one hour.
		if err := cleanupOrphanStagingDirs(dir, time.Hour); err != nil {
			t.Fatalf("cleanupOrphanStagingDirs() error = %v", err)
		}

		// Then only the stale staging directory is removed.
		if _, err := os.Stat(staleDir); !os.IsNotExist(err) {
			t.Fatalf("stale staging dir still exists, stat error = %v", err)
		}
		for _, path := range []string{freshDir, keepFile, otherDir} {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("expected %q to remain, stat error = %v", path, err)
			}
		}
	})
}
