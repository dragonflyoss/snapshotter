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
package dragonfly

import "context"

// Dragonfly defines the Dragonfly configuration for the snapshotter.
type Dragonfly struct {
	// Endpoint is the endpoint of the Dragonfly, such as unix:///var/run/dragonfly.sock.
	Endpoint string
}

// ContentProvider defines the content configuration for the snapshotter.
type ContentProvider struct {
	// Provider is the provider of the content.
	Provider string

	// Bucket is the bucket of the content.
	Bucket string

	// Region is the region of the content.
	Region string

	// Endpoint is the endpoint of the content.
	Endpoint string

	// AccessKeyID is the access key ID for the content.
	AccessKeyID string

	// AccessKeySecret is the access key secret for the content.
	AccessKeySecret string
}

// DownloadRequest defines the request for downloading a file from Dragonfly.
type DownloadRequest struct {
	// Digest is the digest of the file.
	Digest string

	// OutputPath is the path to the output file.
	OutputPath string
}

// UploadRequest defines the request for uploading a file to Dragonfly.
type UploadRequest struct {
	// Digest is the digest of the file.
	Digest string

	// SrcPath is the path to the source file.
	SrcPath string
}

// Client is the interface for interacting with Dragonfly.
type Client interface {
	// Download downloads the file from Dragonfly.
	Download(ctx context.Context, req *DownloadRequest) error

	// Upload uploads the file to Dragonfly.
	Upload(ctx context.Context, req *UploadRequest) error
}

// New creates a new client for Dragonfly.
func New(dragonfly Dragonfly, provider ContentProvider) (Client, error) {
	return newClient(dragonfly, provider)
}
