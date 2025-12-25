package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"d7y.io/snapshotter/internal/metadata"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if s == nil {
		t.Fatal("New() returned nil storage")
	}

	// Verify directories were created.
	contentDir := filepath.Join(tmpDir, "storage", "content", ContentHashAlgorithm)
	snapshotDir := filepath.Join(tmpDir, "storage", "snapshot", ContentHashAlgorithm)
	if _, err := os.Stat(contentDir); os.IsNotExist(err) {
		t.Errorf("content directory not created: %s", contentDir)
	}

	if _, err := os.Stat(snapshotDir); os.IsNotExist(err) {
		t.Errorf("snapshot directory not created: %s", snapshotDir)
	}
}

func TestStorage_WriteContent(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Create a test file.
	baseDir := t.TempDir()
	testFile := filepath.Join(baseDir, "test.txt")
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ctx := context.Background()
	file := File{
		RelativePath: "test.txt",
		ReadOnly:     false,
	}
	info, err := s.WriteContent(ctx, baseDir, file)
	if err != nil {
		t.Fatalf("WriteContent() error = %v", err)
	}

	if info == nil {
		t.Fatal("WriteContent() returned nil FileInfo")
	}

	if info.Size() == 0 {
		t.Error("WriteContent() created empty file")
	}
}

func TestStorage_StatContent(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Create a test file.
	baseDir := t.TempDir()
	testFile := filepath.Join(baseDir, "test.txt")
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ctx := context.Background()
	file := File{
		RelativePath: "test.txt",
		ReadOnly:     false,
	}
	// Write content first
	_, err = s.WriteContent(ctx, baseDir, file)
	if err != nil {
		t.Fatalf("WriteContent() error = %v", err)
	}
	// Stat content
	info, err := s.StatContent(ctx, baseDir, file)
	if err != nil {
		t.Fatalf("StatContent() error = %v", err)
	}

	if info == nil {
		t.Fatal("StatContent() returned nil FileInfo")
	}
}

func TestStorage_GetContentPath(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	filename := "test123"
	path := s.GetContentPath(ctx, filename)
	expectedPath := filepath.Join(tmpDir, "storage", "content", ContentHashAlgorithm, filename)
	if path != expectedPath {
		t.Errorf("GetContentPath() = %v, want %v", path, expectedPath)
	}
}

func TestStorage_RestoreSnapshotFromContent(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// Create a test file and write content.
	baseDir := t.TempDir()
	testFile := filepath.Join(baseDir, "test.txt")
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ctx := context.Background()
	file := File{
		RelativePath: "test.txt",
		ReadOnly:     false,
	}
	info, err := s.WriteContent(ctx, baseDir, file)
	if err != nil {
		t.Fatalf("WriteContent() error = %v", err)
	}
	// Restore snapshot from content.
	err = s.RestoreSnapshotFromContent(ctx, info.Name())
	if err != nil {
		t.Fatalf("RestoreSnapshotFromContent() error = %v", err)
	}
	// Verify snapshot exists.
	_, err = s.StatSnapshot(ctx, info.Name())
	if err != nil {
		t.Errorf("StatSnapshot() error = %v", err)
	}
}

func TestStorage_Export(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// Create a test file and write content.
	baseDir := t.TempDir()
	testFile := filepath.Join(baseDir, "test.txt")
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ctx := context.Background()
	file := File{
		RelativePath: "test.txt",
		ReadOnly:     false,
	}
	info, err := s.WriteContent(ctx, baseDir, file)
	if err != nil {
		t.Fatalf("WriteContent() error = %v", err)
	}
	// Export the file.
	outputDir := t.TempDir()
	metaFile := metadata.File{
		RelativePath: "exported.txt",
		ReadOnly:     false,
		Metadata: metadata.FileMetadata{
			Mode:    0o644,
			ModTime: time.Now(),
		},
	}
	err = s.Export(ctx, outputDir, metaFile, info.Name())
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	// Verify exported file exists.
	exportedPath := filepath.Join(outputDir, "exported.txt")
	if _, err := os.Stat(exportedPath); os.IsNotExist(err) {
		t.Errorf("exported file does not exist: %s", exportedPath)
	}
}
func TestStorage_Prune(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()
	// Prune non-existent file should not error.
	err = s.Prune(ctx, "nonexistent")
	if err != nil {
		t.Errorf("Prune() on non-existent file error = %v", err)
	}
	// Create a test file and write content.
	baseDir := t.TempDir()
	testFile := filepath.Join(baseDir, "test.txt")
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	file := File{
		RelativePath: "test.txt",
		ReadOnly:     false,
	}
	info, err := s.WriteContent(ctx, baseDir, file)
	if err != nil {
		t.Fatalf("WriteContent() error = %v", err)
	}
	// Prune the content.
	err = s.Prune(ctx, info.Name())
	if err != nil {
		t.Errorf("Prune() error = %v", err)
	}
	// Verify content is removed.
	_, err = s.StatContent(ctx, baseDir, file)
	if !os.IsNotExist(err) {
		t.Error("content file still exists after prune")
	}
}
func TestStorage_HardlinkSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// Create a test file.
	baseDir := t.TempDir()
	testFile := filepath.Join(baseDir, "test.txt")
	testContent := []byte("test content")
	if err := os.WriteFile(testFile, testContent, 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	ctx := context.Background()
	srcFile := File{
		RelativePath: "test.txt",
		ReadOnly:     true,
	}
	destFilename := "snapshot123"
	err = s.HardlinkSnapshot(ctx, baseDir, srcFile, destFilename)
	if err != nil {
		t.Fatalf("HardlinkSnapshot() error = %v", err)
	}
	// Verify snapshot exists.
	_, err = s.StatSnapshot(ctx, destFilename)
	if err != nil {
		t.Errorf("StatSnapshot() error = %v", err)
	}
}

func TestContentDigest(t *testing.T) {
	tests := []struct {
		name   string
		digest string
		want   string
	}{
		{
			name:   "simple digest",
			digest: "abc123",
			want:   "xxh3:abc123",
		},
		{
			name:   "empty digest",
			digest: "",
			want:   "xxh3:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ContentDigest(tt.digest)
			if got != tt.want {
				t.Errorf("ContentDigest() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestParseFilenameFromDigest(t *testing.T) {
	tests := []struct {
		name   string
		digest string
		want   string
	}{
		{
			name:   "valid digest",
			digest: "xxh3:abc123",
			want:   "abc123",
		},
		{
			name:   "no algorithm prefix",
			digest: "abc123",
			want:   "abc123",
		},
		{
			name:   "multiple colons",
			digest: "xxh3:abc:123",
			want:   "abc:123",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseFilenameFromDigest(tt.digest)
			if got != tt.want {
				t.Errorf("ParseFilenameFromDigest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateRelativePath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	tests := []struct {
		name    string
		relPath string
		wantErr bool
	}{
		{"valid simple path", "file.txt", false},
		{"valid subdirectory", "subdir/file.txt", false},
		{"valid with dot", "./file.txt", false},
		{"valid with parent then child", "subdir/../file.txt", false},
		{"invalid parent escape", "../escape.txt", true},
		{"invalid double parent", "../../escape.txt", true},
		{"invalid exactly dotdot", "..", true},
		{"empty path", "", false},
		{"dot path", ".", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := validateRelativePath(tmpDir, tt.relPath)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Verify result is under tmpDir.
			absBase, _ := filepath.Abs(tmpDir)
			if !strings.HasPrefix(result, absBase) {
				t.Errorf("result %q not under base %q", result, absBase)
			}
		})
	}
}
