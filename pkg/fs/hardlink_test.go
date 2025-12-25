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
package fs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHardLink(t *testing.T) {
	t.Run("create hard link successfully", func(t *testing.T) {
		tmpDir := t.TempDir()
		src := filepath.Join(tmpDir, "source.txt")
		dst := filepath.Join(tmpDir, "dest.txt")

		// Create source file.
		if err := os.WriteFile(src, []byte("hello"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		// Create hard link.
		if err := HardLink(src, dst); err != nil {
			t.Fatalf("HardLink failed: %v", err)
		}

		// Verify destination exists and has same content.
		content, err := os.ReadFile(dst)
		if err != nil {
			t.Fatalf("failed to read destination file: %v", err)
		}
		if string(content) != "hello" {
			t.Errorf("expected content 'hello', got %q", string(content))
		}

		// Verify they are the same file (same inode).
		srcInfo, _ := os.Stat(src)
		dstInfo, _ := os.Stat(dst)
		if !os.SameFile(srcInfo, dstInfo) {
			t.Error("source and destination are not the same file")
		}
	})

	t.Run("source file does not exist", func(t *testing.T) {
		tmpDir := t.TempDir()
		src := filepath.Join(tmpDir, "nonexistent.txt")
		dst := filepath.Join(tmpDir, "dest.txt")

		err := HardLink(src, dst)
		if err == nil {
			t.Fatal("expected error when source file does not exist")
		}
	})

	t.Run("destination already exists and is same file", func(t *testing.T) {
		tmpDir := t.TempDir()
		src := filepath.Join(tmpDir, "source.txt")
		dst := filepath.Join(tmpDir, "dest.txt")

		// Create source file.
		if err := os.WriteFile(src, []byte("hello"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		// Create hard link first time.
		if err := HardLink(src, dst); err != nil {
			t.Fatalf("first HardLink failed: %v", err)
		}

		// Call again - should succeed without error.
		if err := HardLink(src, dst); err != nil {
			t.Fatalf("second HardLink failed: %v", err)
		}
	})

	t.Run("destination exists but is different file", func(t *testing.T) {
		tmpDir := t.TempDir()
		src := filepath.Join(tmpDir, "source.txt")
		dst := filepath.Join(tmpDir, "dest.txt")

		// Create source file.
		if err := os.WriteFile(src, []byte("hello"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		// Create different destination file.
		if err := os.WriteFile(dst, []byte("world"), 0644); err != nil {
			t.Fatalf("failed to create destination file: %v", err)
		}

		err := HardLink(src, dst)
		if err == nil {
			t.Fatal("expected error when destination is a different file")
		}
	})

	t.Run("parent directory of destination does not exist", func(t *testing.T) {
		tmpDir := t.TempDir()
		src := filepath.Join(tmpDir, "source.txt")
		dst := filepath.Join(tmpDir, "nonexistent_dir", "dest.txt")

		// Create source file.
		if err := os.WriteFile(src, []byte("hello"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		err := HardLink(src, dst)
		if err == nil {
			t.Fatal("expected error when parent directory does not exist")
		}
	})
}

func TestIsSameFile(t *testing.T) {
	t.Run("same file returns true", func(t *testing.T) {
		tmpDir := t.TempDir()
		src := filepath.Join(tmpDir, "source.txt")
		dst := filepath.Join(tmpDir, "dest.txt")

		// Create source file.
		if err := os.WriteFile(src, []byte("hello"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		// Create hard link.
		if err := os.Link(src, dst); err != nil {
			t.Fatalf("failed to create hard link: %v", err)
		}

		srcInfo, _ := os.Stat(src)
		if !isSameFile(srcInfo, dst) {
			t.Error("expected isSameFile to return true for hard-linked files")
		}
	})

	t.Run("different file returns false", func(t *testing.T) {
		tmpDir := t.TempDir()
		src := filepath.Join(tmpDir, "source.txt")
		dst := filepath.Join(tmpDir, "dest.txt")

		// Create two different files.
		if err := os.WriteFile(src, []byte("hello"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}
		if err := os.WriteFile(dst, []byte("world"), 0644); err != nil {
			t.Fatalf("failed to create destination file: %v", err)
		}

		srcInfo, _ := os.Stat(src)
		if isSameFile(srcInfo, dst) {
			t.Error("expected isSameFile to return false for different files")
		}
	})

	t.Run("destination does not exist returns false", func(t *testing.T) {
		tmpDir := t.TempDir()
		src := filepath.Join(tmpDir, "source.txt")
		dst := filepath.Join(tmpDir, "nonexistent.txt")

		// Create source file.
		if err := os.WriteFile(src, []byte("hello"), 0644); err != nil {
			t.Fatalf("failed to create source file: %v", err)
		}

		srcInfo, _ := os.Stat(src)
		if isSameFile(srcInfo, dst) {
			t.Error("expected isSameFile to return false when destination does not exist")
		}
	})
}
