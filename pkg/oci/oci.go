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
	"context"
	"io"

	godigest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Client is the interface for OCI registry operations.
type Client interface {
	// PullBlob pulls a blob from the registry.
	PullBlob(ctx context.Context, name, reference string) (io.ReadCloser, error)

	// PullManifest pulls a manifest from the registry.
	PullManifest(ctx context.Context, name, reference string) (ocispec.Manifest, error)

	// PushBlob pushes a blob to the registry.
	PushBlob(ctx context.Context, name string, blob io.ReadCloser) (godigest.Digest, error)

	// PushManifest pushes a manifest to the registry.
	PushManifest(ctx context.Context, name, reference string, manifest ocispec.Manifest) (godigest.Digest, error)
}

func New(rootDir string, registry Registry) (Client, error) {
	return newClient(rootDir, registry)
}
