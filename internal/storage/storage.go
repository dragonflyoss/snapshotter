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

package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/zeebo/xxh3"

	"d7y.io/snapshotter/internal/metadata"
	"d7y.io/snapshotter/pkg/fs"
	"d7y.io/snapshotter/pkg/sparsefile"
)

const (
	// ContentHashAlgorithm is the hash algorithm used for content addressing.
	ContentHashAlgorithm = "xxh3"

	// DirPerm is the default permission for directories.
	DirPerm = 0o755

	// TempContentPattern is the pattern for temporary content files.
	TempContentPattern = ".content-*"

	// TempSnapshotPattern is the pattern for temporary snapshot files.
	TempSnapshotPattern = ".snapshot-*"
)

// ContentDigest returns the digest by adding the hash algorithm prefix.
func ContentDigest(digest string) string {
	return fmt.Sprintf("%s:%s", ContentHashAlgorithm, digest)
}

// ParseFilenameFromDigest parses the filename from the digest.
func ParseFilenameFromDigest(digest string) string {
	parts := strings.SplitN(digest, ":", 2)
	if len(parts) != 2 {
		return digest
	}
	return parts[1]
}

// File defines the file information for snapshotting and restoring.
type File struct {
	// RelativePath is the relative path of the file.
	RelativePath string

	// ReadOnly indicates whether the file is read-only.
	ReadOnly bool
}

// Storage is an interface for storage.
type Storage interface {
	// StatContent returns the content info.
	StatContent(ctx context.Context, baseDir string, file File) (os.FileInfo, error)

	// WriteContent writes the content to the storage.
	WriteContent(ctx context.Context, baseDir string, file File) (os.FileInfo, error)

	// GetContentPath returns the path to the content.
	GetContentPath(ctx context.Context, filename string) string

	// StatSnapshot returns the snapshot info.
	StatSnapshot(ctx context.Context, filename string) (os.FileInfo, error)

	// HardlinkSnapshot creates a hardlink to the snapshot.
	HardlinkSnapshot(ctx context.Context, baseDir string, srcfile File, destFilename string) error

	// RestoreSnapshotFromContent restores the snapshot from content.
	RestoreSnapshotFromContent(ctx context.Context, filename string) error

	// Export outputs the content or snapshot to output directory.
	// It will copy the content if the file is not read-only, otherwise it will hardlink from snapshot.
	Export(ctx context.Context, outputDir string, file metadata.File, srcFilename string) error

	// Prune removes unused content and snapshot files.
	Prune(ctx context.Context, filename string) error
}

// validateRelativePath ensures the relative path does not escape the base directory.
func validateRelativePath(baseDir, relPath string) (string, error) {
	cleaned := filepath.Clean(relPath)
	if strings.HasPrefix(cleaned, "..") || cleaned == ".." {
		return "", fmt.Errorf("unsafe relative path: %q", relPath)
	}

	fullPath := filepath.Join(baseDir, cleaned)
	// Resolve symlinks and ensure it's under baseDir.
	realBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path of baseDir: %w", err)
	}
	realPath, err := filepath.Abs(fullPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve absolute path: %w", err)
	}

	if !strings.HasPrefix(realPath, realBase+string(filepath.Separator)) && realPath != realBase {
		return "", fmt.Errorf("path escapes base directory: %q", relPath)
	}

	return realPath, nil
}

// New creates a new storage instance.
func New(rootDir string) (Storage, error) {
	contentDir := filepath.Join(rootDir, "storage", "content", ContentHashAlgorithm)
	snapshotDir := filepath.Join(rootDir, "storage", "snapshot", ContentHashAlgorithm)

	if err := os.MkdirAll(contentDir, DirPerm); err != nil {
		return nil, fmt.Errorf("failed to create content directory: %w", err)
	}
	if err := os.MkdirAll(snapshotDir, DirPerm); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &storage{
		contentDir:  contentDir,
		snapshotDir: snapshotDir,
	}, nil
}

// storage is the implementation of Storage interface.
type storage struct {
	contentDir  string
	snapshotDir string
}

// computeContentHash computes the xxh3 hash of a file via sparse encoding.
func (s *storage) computeContentHash(baseDir, relPath string) (string, error) {
	fullPath, err := validateRelativePath(baseDir, relPath)
	if err != nil {
		return "", err
	}

	hasher := xxh3.New()
	if err := sparsefile.Encode(fullPath, hasher); err != nil {
		return "", fmt.Errorf("failed to encode file %q to sparse format: %w", fullPath, err)
	}

	return fmt.Sprintf("%x", hasher.Sum64()), nil
}

// StatContent implements the Storage.StatContent.
func (s *storage) StatContent(ctx context.Context, baseDir string, file File) (os.FileInfo, error) {
	hash, err := s.computeContentHash(baseDir, file.RelativePath)
	if err != nil {
		return nil, err
	}

	return os.Stat(filepath.Join(s.contentDir, hash))
}

// WriteContent implements the Storage.WriteContent.
func (s *storage) WriteContent(ctx context.Context, baseDir string, file File) (os.FileInfo, error) {
	fullPath, err := validateRelativePath(baseDir, file.RelativePath)
	if err != nil {
		return nil, err
	}

	tempFile, err := os.CreateTemp(s.contentDir, TempContentPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary content file: %w", err)
	}
	tempPath := tempFile.Name()
	defer func() {
		// Remove temp file only if it still exists (i.e., Rename hasn't succeeded).
		_ = os.Remove(tempPath)
	}()

	hasher := xxh3.New()
	writer := io.MultiWriter(tempFile, hasher)
	if err := sparsefile.Encode(fullPath, writer); err != nil {
		_ = tempFile.Close()
		return nil, fmt.Errorf("failed to encode file %q to sparse format: %w", fullPath, err)
	}

	if err := tempFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temporary file: %w", err)
	}

	hash := fmt.Sprintf("%x", hasher.Sum64())
	finalPath := filepath.Join(s.contentDir, hash)

	if err := os.Rename(tempPath, finalPath); err != nil {
		return nil, fmt.Errorf("failed to rename temp file to %s: %w", hash, err)
	}

	if err := os.Chmod(finalPath, DirPerm); err != nil {
		return nil, fmt.Errorf("failed to chmod content file %s: %w", hash, err)
	}

	return os.Stat(finalPath)
}

// GetContentPath implements the Storage.GetContentPath.
func (s *storage) GetContentPath(ctx context.Context, filename string) string {
	return filepath.Join(s.contentDir, filename)
}

// StatSnapshot implements the Storage.StatSnapshot.
func (s *storage) StatSnapshot(ctx context.Context, filename string) (os.FileInfo, error) {
	return os.Stat(filepath.Join(s.snapshotDir, filename))
}

// HardlinkSnapshot implements the Storage.HardlinkSnapshot.
func (s *storage) HardlinkSnapshot(ctx context.Context, baseDir string, srcfile File, destFilename string) error {
	srcPath, err := validateRelativePath(baseDir, srcfile.RelativePath)
	if err != nil {
		return err
	}

	destPath := filepath.Join(s.snapshotDir, destFilename)
	if err := fs.HardLink(srcPath, destPath); err != nil {
		return fmt.Errorf("failed to hardlink %q to %q: %w", srcPath, destPath, err)
	}

	return nil
}

// RestoreSnapshotFromContent implements the Storage.RestoreSnapshotFromContent.
func (s *storage) RestoreSnapshotFromContent(ctx context.Context, filename string) error {
	srcPath := filepath.Join(s.contentDir, filename)
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source content file %q: %w", srcPath, err)
	}
	defer srcFile.Close()

	tempFile, err := os.CreateTemp(s.snapshotDir, TempSnapshotPattern)
	if err != nil {
		return fmt.Errorf("failed to create temp snapshot file: %w", err)
	}
	tempPath := tempFile.Name()
	_ = tempFile.Close()

	defer func() { _ = os.Remove(tempPath) }()

	if err := sparsefile.Decode(srcFile, tempPath); err != nil {
		return fmt.Errorf("failed to decode sparse file to %q: %w", tempPath, err)
	}

	finalPath := filepath.Join(s.snapshotDir, filename)
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename temp snapshot to %s: %w", filename, err)
	}

	return nil
}

// Export implements the Storage.Export.
func (s *storage) Export(ctx context.Context, outputDir string, file metadata.File, srcFilename string) error {
	destPath := filepath.Join(outputDir, file.RelativePath)
	if err := os.MkdirAll(filepath.Dir(destPath), DirPerm); err != nil {
		return fmt.Errorf("failed to ensure destination directory for %q: %w", destPath, err)
	}

	var exportErr error
	if file.ReadOnly {
		srcPath := filepath.Join(s.snapshotDir, srcFilename)
		exportErr = fs.HardLink(srcPath, destPath)
	} else {
		srcPath := filepath.Join(s.contentDir, srcFilename)
		srcFile, err := os.Open(srcPath)
		if err != nil {
			return fmt.Errorf("failed to open source content file %q: %w", srcPath, err)
		}
		defer srcFile.Close()

		exportErr = sparsefile.Decode(srcFile, destPath)
	}
	if exportErr != nil {
		return fmt.Errorf("failed to export file %q: %w", destPath, exportErr)
	}

	// Apply file metadata only on successful export.
	if file.Metadata.Mode != 0 {
		if err := os.Chmod(destPath, os.FileMode(file.Metadata.Mode)); err != nil {
			slog.Warn("failed to chmod exported file", "path", destPath, "mode", file.Metadata.Mode, "err", err)
		}
	}
	if !file.Metadata.ModTime.IsZero() {
		if err := os.Chtimes(destPath, file.Metadata.ModTime, file.Metadata.ModTime); err != nil {
			slog.Warn("failed to set modtime on exported file", "path", destPath, "modtime", file.Metadata.ModTime, "err", err)
		}
	}

	return nil
}

// Prune implements the Storage.Prune.
func (s *storage) Prune(ctx context.Context, filename string) error {
	contentPath := filepath.Join(s.contentDir, filename)
	if err := os.Remove(contentPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("failed to remove content file", "path", contentPath, "err", err)
	}

	snapshotPath := filepath.Join(s.snapshotDir, filename)
	if err := os.Remove(snapshotPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("failed to remove snapshot file", "path", snapshotPath, "err", err)
	}

	// Always return nil: pruning is best-effort.
	return nil
}
