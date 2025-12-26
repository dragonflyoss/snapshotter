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
package metadata

import (
	"errors"
	"os"
	"syscall"
	"time"
)

// Config is the metadata config.
type Config struct {
	// Files is the collection of files.
	Files []File `json:"files"`
}

// File defines the definition of a file.
type File struct {
	// Digest is the digest of the file.
	Digest string `json:"digest"`

	// ReadOnly checks whether the file is read-only.
	ReadOnly bool `json:"readOnly"`

	// RelativePath is the relative path of the file.
	RelativePath string `json:"relativePath"`

	// Metadata is the metadata of the file.
	Metadata FileMetadata `json:"metadata"`
}

// FileMetadata defines the metadata of a file.
type FileMetadata struct {
	// File permission mode (e.g., Unix permission bits)
	Mode uint32 `json:"mode"`

	// User ID (identifier of the file owner)
	Uid uint32 `json:"uid"`

	// Group ID (identifier of the file's group)
	Gid uint32 `json:"gid"`

	// File size (in bytes)
	Size int64 `json:"size"`

	// File last modification time
	ModTime time.Time `json:"mtime"`

	// File type flag (e.g., regular file, directory, etc.)
	Typeflag byte `json:"typeflag"`
}

// GetFileMetadata returns the file metatdata by file info.
func GetFileMetadata(info os.FileInfo) (FileMetadata, error) {
	var metadata FileMetadata
	metadata.Mode = uint32(info.Mode().Perm())
	metadata.Size = info.Size()
	metadata.ModTime = info.ModTime()
	// Set Typeflag.
	switch {
	case info.Mode().IsRegular():
		metadata.Typeflag = 0 // Regular file
	case info.Mode().IsDir():
		metadata.Typeflag = 5 // Directory
	case info.Mode()&os.ModeSymlink != 0:
		metadata.Typeflag = 2 // Symlink
	default:
		return metadata, errors.New("unknown file typeflag")
	}

	// UID and GID (Unix-specific).
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		metadata.Uid = stat.Uid
		metadata.Gid = stat.Gid
	}

	return metadata, nil
}
