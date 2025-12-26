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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	godigest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
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
	u, err := url.Parse(registry.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid registry endpoint %q: %w", registry.Endpoint, err)
	}

	// Build HTTP client with retry and TLS config.
	httpClient := &http.Client{
		Transport: retry.NewTransport(&http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: registry.Insecure,
			},
		}),
	}

	authClient := &auth.Client{
		Cache: auth.NewCache(),
		Credential: auth.StaticCredential(u.Host, auth.Credential{
			Username: registry.Username,
			Password: registry.Password,
		}),
		Client: httpClient,
	}

	return &client{
		rootDir:   rootDir,
		registry:  registry,
		host:      u.Host,
		plainHTTP: u.Scheme == "http",
		client:    authClient,
	}, nil
}

// Registry defines the registry configuration for the snapshotter.
type Registry struct {
	Endpoint  string
	Namespace string
	Username  string
	Password  string
	Insecure  bool
}

type client struct {
	rootDir   string
	registry  Registry
	host      string
	plainHTTP bool
	client    *auth.Client
}

// repositoryName builds the OCI repository name (e.g., host/ns/name).
func (c *client) repositoryName(name string) string {
	return path.Join(c.host, c.registry.Namespace, name)
}

// repository returns the remote.Repository for the given image name.
func (c *client) repository(name string) (*remote.Repository, error) {
	repo, err := remote.NewRepository(c.repositoryName(name))
	if err != nil {
		return nil, fmt.Errorf("failed to create repository for %q: %w", name, err)
	}

	repo.Client = c.client
	repo.PlainHTTP = c.plainHTTP
	return repo, nil
}

// PullBlob pulls a blob by digest or tag reference.
func (c *client) PullBlob(ctx context.Context, name, reference string) (io.ReadCloser, error) {
	repo, err := c.repository(name)
	if err != nil {
		return nil, err
	}

	_, rc, err := repo.Blobs().FetchReference(ctx, reference)
	if err != nil {
		return nil, fmt.Errorf("failed to pull blob %q from %q: %w", reference, name, err)
	}

	return rc, nil
}

// PullManifest pulls and decodes an OCI manifest.
func (c *client) PullManifest(ctx context.Context, name, reference string) (ocispec.Manifest, error) {
	repo, err := c.repository(name)
	if err != nil {
		return ocispec.Manifest{}, err
	}

	_, rc, err := repo.Manifests().FetchReference(ctx, reference)
	if err != nil {
		return ocispec.Manifest{}, fmt.Errorf("failed to fetch manifest %q from %q: %w", reference, name, err)
	}
	defer rc.Close()

	var manifest ocispec.Manifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return ocispec.Manifest{}, fmt.Errorf("failed to decode manifest: %w", err)
	}
	return manifest, nil
}

// PushBlob pushes a blob and returns its digest. Currently reads entire blob into memory.
// Note: For large blobs, consider streaming via io.Reader with known digest/size.
func (c *client) PushBlob(ctx context.Context, name string, blob io.ReadCloser) (godigest.Digest, error) {
	defer blob.Close()

	blobBytes, err := io.ReadAll(blob)
	if err != nil {
		return "", fmt.Errorf("failed to read blob content: %w", err)
	}

	digest := godigest.FromBytes(blobBytes)
	desc := ocispec.Descriptor{
		Digest: digest,
		Size:   int64(len(blobBytes)),
	}

	repo, err := c.repository(name)
	if err != nil {
		return "", err
	}

	// Check existence (optional; ORAS Push is idempotent, but this avoids upload if already present).
	if exists, err := repo.Blobs().Exists(ctx, desc); err != nil {
		return "", fmt.Errorf("failed to check blob existence: %w", err)
	} else if exists {
		return digest, nil
	}

	if err := repo.Blobs().Push(ctx, desc, bytes.NewReader(blobBytes)); err != nil {
		return "", fmt.Errorf("failed to push blob: %w", err)
	}

	return digest, nil
}

// PushManifest pushes an OCI manifest and tags it.
func (c *client) PushManifest(ctx context.Context, name, reference string, manifest ocispec.Manifest) (godigest.Digest, error) {
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("failed to marshal manifest: %w", err)
	}

	digest := godigest.FromBytes(manifestBytes)
	desc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest,
		Size:      int64(len(manifestBytes)),
	}

	repo, err := c.repository(name)
	if err != nil {
		return "", err
	}

	// Check if already exists.
	if exists, err := repo.Manifests().Exists(ctx, desc); err != nil {
		return "", fmt.Errorf("failed to check manifest existence: %w", err)
	} else if exists {
		// Still tag it in case the tag is missing.
		if err := repo.Tag(ctx, desc, reference); err != nil {
			return "", fmt.Errorf("failed to tag existing manifest: %w", err)
		}
		return digest, nil
	}

	if err := repo.Manifests().Push(ctx, desc, bytes.NewReader(manifestBytes)); err != nil {
		return "", fmt.Errorf("failed to push manifest: %w", err)
	}

	if err := repo.Tag(ctx, desc, reference); err != nil {
		return "", fmt.Errorf("failed to tag manifest: %w", err)
	}

	return digest, nil
}
