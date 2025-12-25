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
	"context"
	"fmt"
	"time"
)

// Key constructs the metadata key.
func Key(name, version string) string {
	return fmt.Sprintf("%s:%s", name, version)
}

// Metadata defines the common operations for metadata.
type Metadata interface {
	// GetMetadata retrieves the metadata value for the given key.
	GetMetadata(ctx context.Context, key string) (*MetadataEntry, error)

	// PutMetadata puts the metadata value for the given key.
	PutMetadata(ctx context.Context, key string, entry MetadataEntry) error

	// GetContent retrieves the content metadata for the given key.
	GetContent(ctx context.Context, key string) (*File, error)

	// PutContent puts the content metadata for the given key.
	PutContent(ctx context.Context, key string, file File) error

	// GetContentMetadata retrieves the content metadata for the given key.
	GetContentMetadata(ctx context.Context, key string) (*ContentMetadataEntry, error)

	// PutContentMetadata puts the content metadata for the given key.
	PutContentMetadata(ctx context.Context, metadataKey string, metadataEntry MetadataEntry) error

	// Touch touches the metadata entry for the given key.
	// It will update the metadata entry's lastAccessedAt timestamp.
	TouchMetadata(ctx context.Context, key string) error

	// IterateMetadata iterates over all metadata entries.
	IterateMetadata(ctx context.Context, fn func(key string, entry MetadataEntry) error) error

	// Prune will remove all related metadata and content info.
	Prune(ctx context.Context, key string) ([]File, error)
}

// MetadataEntry is the definition of metadata entry.
type MetadataEntry struct {
	// CreatedAt is the timestamp when the metadata entry was created.
	CreatedAt time.Time `json:"createdAt"`

	// LastAccessedAt is the timestamp when the metadata entry was last accessed.
	LastAccessedAt time.Time `json:"lastAccessedAt"`

	// Config is the configuration of the metadata entry.
	Config Config `json:"config"`
}

// ContentMetadataEntry is the definition of the content to metadata index.
type ContentMetadataEntry struct {
	// References is the list of references to the metadata entry.
	References []string `json:"references"`
}

// New creates a new metadata instance.
func New(rootDir string) (Metadata, error) {
	return newBbolt(rootDir)
}
