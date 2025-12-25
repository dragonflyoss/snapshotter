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
package oci

import (
	"encoding/json"
	"fmt"

	godigest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"d7y.io/snapshotter/internal/metadata"
)

const (
	// ArtifactTypeSnapshotterManifest specifies the artifact type for snapshotter.
	ArtifactTypeSnapshotterManifest = "application/vnd.cncf.dragonfly.snapshotter.manifest.v1+json"

	// MediaTypeSnapshotterConfig specifies the media type for snapshotter config.
	MediaTypeSnapshotterConfig = "application/vnd.cncf.dragonfly.snapshotter.config.v1+json"
)

// BuildManifest builds the OCI manifest by the metadata config.
func BuildManifest(config metadata.Config) (ocispec.Manifest, error) {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return ocispec.Manifest{}, fmt.Errorf("failed to marshal config: %w", err)
	}

	return ocispec.Manifest{
		Versioned: specs.Versioned{
			SchemaVersion: 2,
		},
		ArtifactType: ArtifactTypeSnapshotterManifest,
		Config: ocispec.Descriptor{
			MediaType: MediaTypeSnapshotterConfig,
			Digest:    godigest.FromBytes(configBytes),
			Size:      int64(len(configBytes)),
		},
		MediaType: ocispec.MediaTypeImageManifest,
	}, nil
}
