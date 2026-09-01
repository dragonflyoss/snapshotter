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
