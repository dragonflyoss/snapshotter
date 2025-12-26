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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	bboltdb "go.etcd.io/bbolt"
)

// ErrKeyNotFound is returned when a key is not found.
var ErrKeyNotFound = errors.New("key not found")

const (
	// metadataBucketName is the name of the metadata bucket.
	metadataBucketName = "metadata"

	// contentBucketName is the name of the content bucket.
	contentBucketName = "content"

	// contentMetadataBucketName is the name of the content metadata bucket.
	contentMetadataBucketName = "content_metadata"

	// metadataDirName is the name of the metadata directory.
	metadataDirName = "metadata"

	// metadataDBName is the name of the metadata database.
	metadataDBName = "snapshotter.db"
)

// bbolt is a metadata storage implementation using bbolt db.
type bbolt struct {
	// db is the bbolt database instance.
	db *bboltdb.DB
}

// newBbolt creates a new bbolt metadata storage.
func newBbolt(rootDir string) (*bbolt, error) {
	// Ensure the metadata directory exists.
	if _, err := os.Stat(filepath.Join(rootDir, metadataDirName)); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Join(rootDir, metadataDirName), 0755); err != nil {
			return nil, fmt.Errorf("failed to ensure metadata directory: %w", err)
		}
	}

	// Open the database and config the dial timeout to avoid hang out.
	db, err := bboltdb.Open(filepath.Join(rootDir, metadataDirName, metadataDBName), 0755, &bboltdb.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata db: %w", err)
	}

	// Make sure the bucket exists.
	err = db.Update(func(tx *bboltdb.Tx) error {
		for _, name := range []string{metadataBucketName, contentBucketName, contentMetadataBucketName} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", name, err)
			}
		}

		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize bucket: %w", err)
	}

	return &bbolt{
		db: db,
	}, nil
}

func getFromBucket(tx *bboltdb.Tx, bucketName, key string, v any) error {
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return fmt.Errorf("bucket %s not found", bucketName)
	}

	val := bucket.Get([]byte(key))
	if val == nil {
		return ErrKeyNotFound
	}

	if err := json.Unmarshal(val, v); err != nil {
		return fmt.Errorf("unmarshal failed for key %s in %s: %w", key, bucketName, err)
	}

	return nil
}

func putToBucket(tx *bboltdb.Tx, bucketName, key string, v any) error {
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return fmt.Errorf("bucket %s not found", bucketName)
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal failed for key %s in %s: %w", key, bucketName, err)
	}

	return bucket.Put([]byte(key), data)
}

// GetMetadata retrieves the metadata value for the given key.
func (b *bbolt) GetMetadata(_ context.Context, key string) (*MetadataEntry, error) {
	var entry MetadataEntry
	if err := b.db.View(func(tx *bboltdb.Tx) error {
		return getFromBucket(tx, metadataBucketName, key, &entry)
	}); err != nil {
		return nil, err
	}
	return &entry, nil
}

// PutMetadata puts the metadata value for the given key.
func (b *bbolt) PutMetadata(_ context.Context, key string, entry MetadataEntry) error {
	return b.db.Update(func(tx *bboltdb.Tx) error {
		return putToBucket(tx, metadataBucketName, key, entry)
	})
}

// GetContent retrieves the content metadata for the given key.
func (b *bbolt) GetContent(_ context.Context, key string) (*File, error) {
	var file File
	if err := b.db.View(func(tx *bboltdb.Tx) error {
		return getFromBucket(tx, contentBucketName, key, &file)
	}); err != nil {
		return nil, err
	}
	return &file, nil
}

// PutContent puts the content metadata for the given key.
func (b *bbolt) PutContent(_ context.Context, key string, file File) error {
	return b.db.Update(func(tx *bboltdb.Tx) error {
		return putToBucket(tx, contentBucketName, key, file)
	})
}

// GetContentMetadata retrieves the content metadata for the given key.
func (b *bbolt) GetContentMetadata(_ context.Context, key string) (*ContentMetadataEntry, error) {
	var entry ContentMetadataEntry
	if err := b.db.View(func(tx *bboltdb.Tx) error {
		return getFromBucket(tx, contentMetadataBucketName, key, &entry)
	}); err != nil {
		return nil, err
	}
	return &entry, nil
}

// PutContentMetadata puts the content metadata for the given key.
func (b *bbolt) PutContentMetadata(_ context.Context, metadataKey string, metadataEntry MetadataEntry) error {
	return b.db.Update(func(tx *bboltdb.Tx) error {
		for _, file := range metadataEntry.Config.Files {
			digest := file.Digest
			var contentMetadata ContentMetadataEntry

			err := getFromBucket(tx, contentMetadataBucketName, digest, &contentMetadata)
			if err != nil && !errors.Is(err, ErrKeyNotFound) {
				return fmt.Errorf("failed to read content metadata for %s: %w", digest, err)
			}
			if errors.Is(err, ErrKeyNotFound) {
				contentMetadata = ContentMetadataEntry{References: make([]string, 0, len(metadataEntry.Config.Files))}
			}

			if !slices.Contains(contentMetadata.References, metadataKey) {
				contentMetadata.References = append(contentMetadata.References, metadataKey)
			}

			if err := putToBucket(tx, contentMetadataBucketName, digest, contentMetadata); err != nil {
				return fmt.Errorf("failed to update content metadata for %s: %w", digest, err)
			}
		}
		return nil
	})
}

// Touch touches the metadata entry for the given key.
// It will update the metadata entry's lastAccessedAt timestamp.
func (b *bbolt) TouchMetadata(_ context.Context, key string) error {
	return b.db.Update(func(tx *bboltdb.Tx) error {
		var entry MetadataEntry
		if err := getFromBucket(tx, metadataBucketName, key, &entry); err != nil {
			return fmt.Errorf("failed to get metadata for touch: %w", err)
		}

		entry.LastAccessedAt = time.Now()
		return putToBucket(tx, metadataBucketName, key, entry)
	})
}

// IterateMetadata iterates over all metadata entries.
func (b *bbolt) IterateMetadata(_ context.Context, fn func(key string, entry MetadataEntry) error) error {
	return b.db.View(func(tx *bboltdb.Tx) error {
		bucket := tx.Bucket([]byte(metadataBucketName))
		if bucket == nil {
			return fmt.Errorf("metadata bucket not found")
		}
		return bucket.ForEach(func(k, v []byte) error {
			if k == nil || v == nil {
				return nil
			}
			var entry MetadataEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return fmt.Errorf("unmarshal metadata entry for key %q: %w", string(k), err)
			}
			return fn(string(k), entry)
		})
	})
}

// Prune removes metadata and dereferences content. Returns list of fully unreferenced files.
func (b *bbolt) Prune(_ context.Context, key string) ([]File, error) {
	var prunedContent []File
	err := b.db.Update(func(tx *bboltdb.Tx) error {
		var metadataEntry MetadataEntry
		if err := getFromBucket(tx, metadataBucketName, key, &metadataEntry); err != nil {
			return fmt.Errorf("failed to get metadata for prune: %w", err)
		}

		// Delete metadata entry.
		if err := tx.Bucket([]byte(metadataBucketName)).Delete([]byte(key)); err != nil {
			return fmt.Errorf("failed to delete metadata entry: %w", err)
		}

		// Process each file.
		for _, file := range metadataEntry.Config.Files {
			digest := file.Digest
			var contentMetadata ContentMetadataEntry

			if err := getFromBucket(tx, contentMetadataBucketName, digest, &contentMetadata); err != nil {
				if errors.Is(err, ErrKeyNotFound) {
					// Already deleted? Skip.
					continue
				}
				return fmt.Errorf("failed to read content metadata for %s: %w", digest, err)
			}

			// Remove reference.
			newRefs := slices.DeleteFunc(contentMetadata.References, func(ref string) bool {
				return ref == key
			})

			if len(newRefs) == 0 {
				// No more references: delete content and metadata.
				if err := tx.Bucket([]byte(contentMetadataBucketName)).Delete([]byte(digest)); err != nil {
					return fmt.Errorf("failed to delete content metadata for %s: %w", digest, err)
				}
				if err := tx.Bucket([]byte(contentBucketName)).Delete([]byte(digest)); err != nil {
					return fmt.Errorf("failed to delete content entry for %s: %w", digest, err)
				}
				prunedContent = append(prunedContent, file)
			} else {
				contentMetadata.References = newRefs
				if err := putToBucket(tx, contentMetadataBucketName, digest, contentMetadata); err != nil {
					return fmt.Errorf("failed to update content metadata for %s: %w", digest, err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return prunedContent, nil
}

func (b *bbolt) Close() error {
	return b.db.Close()
}
