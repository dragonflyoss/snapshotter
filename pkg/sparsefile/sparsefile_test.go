//go:build linux || darwin

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

package sparsefile

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeDecodeEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "empty.txt")
	dstPath := filepath.Join(tmpDir, "empty_out.txt")

	if err := os.WriteFile(srcPath, []byte{}, 0644); err != nil {
		t.Fatalf("create file: %v", err)
	}

	var buf bytes.Buffer
	if err := Encode(srcPath, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if err := Decode(&buf, dstPath); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	info, err := os.Stat(dstPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("size = %d, want 0", info.Size())
	}
}

func TestEncodeDecodeSmallFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "small.txt")
	dstPath := filepath.Join(tmpDir, "small_out.txt")

	content := []byte("Hello, Sparse File!")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("create file: %v", err)
	}

	var buf bytes.Buffer
	if err := Encode(srcPath, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if err := Decode(&buf, dstPath); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q, want %q", got, content)
	}
}

func TestEncodeDecodeSparseFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "sparse.bin")
	dstPath := filepath.Join(tmpDir, "sparse_out.bin")

	f, err := os.Create(srcPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	fileSize := int64(1024 * 1024)
	_ = f.Truncate(fileSize)
	_, _ = f.WriteAt([]byte("START"), 0)
	_, _ = f.WriteAt([]byte("MIDDLE"), 512*1024)
	_, _ = f.WriteAt([]byte("END"), fileSize-3)
	f.Close()

	var buf bytes.Buffer
	if err := Encode(srcPath, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if err := Decode(&buf, dstPath); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	info, _ := os.Stat(dstPath)
	if info.Size() != fileSize {
		t.Errorf("size = %d, want %d", info.Size(), fileSize)
	}

	outFile, _ := os.Open(dstPath)
	defer outFile.Close()

	buf1 := make([]byte, 5)
	_, _ = outFile.ReadAt(buf1, 0)
	if string(buf1) != "START" {
		t.Errorf("got %q, want START", buf1)
	}

	buf2 := make([]byte, 6)
	_, _ = outFile.ReadAt(buf2, 512*1024)
	if string(buf2) != "MIDDLE" {
		t.Errorf("got %q, want MIDDLE", buf2)
	}
}

func TestDecodeInvalidMagic(t *testing.T) {
	tmpDir := t.TempDir()
	dstPath := filepath.Join(tmpDir, "out.bin")

	buf := bytes.NewReader([]byte("XXXX"))
	err := Decode(buf, dstPath)
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestEncodeNonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	nonExistent := filepath.Join(tmpDir, "nonexistent", "path")

	var buf bytes.Buffer
	err := Encode(nonExistent, &buf)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}
