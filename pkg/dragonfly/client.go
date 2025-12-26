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

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"

	common "d7y.io/api/v2/pkg/apis/common/v2"
	dfdaemon "d7y.io/api/v2/pkg/apis/dfdaemon/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// defaultUploadPersistentReplicaCount is the default replica count for upload persistent task.
	defaultUploadPersistentReplicaCount = 3
)

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
	conn, err := grpc.NewClient(dragonfly.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &client{
		conn:     conn,
		provider: provider,
	}, nil
}

// client is the client for Dragonfly.
type client struct {
	// conn is the grpc connection.
	conn *grpc.ClientConn

	// provider is the content provider.
	provider ContentProvider
}

// objectStorageURL constructs the object storage url for the given digest.
// The digest is the unique identifier of the file, e.g. xxh3:1234567890abcdef.
// Transform the digest to a valid object storage url such as s3://content/xxh3/1234567890abcdef.
func (c *client) objectStorageURL(digest string) string {
	parts := strings.SplitN(digest, ":", 2)
	if len(parts) != 2 {
		return ""
	}

	return c.provider.Provider + "://" + filepath.Join(c.provider.Bucket, parts[0], parts[1])
}

// Download downloads the file from Dragonfly.
func (c *client) Download(ctx context.Context, req *DownloadRequest) error {
	if req == nil {
		return errors.New("invalid download request")
	}

	request := &dfdaemon.DownloadPersistentTaskRequest{
		Url: c.objectStorageURL(req.Digest),
		ObjectStorage: &common.ObjectStorage{
			Region:          &c.provider.Region,
			Endpoint:        &c.provider.Endpoint,
			AccessKeyId:     &c.provider.AccessKeyID,
			AccessKeySecret: &c.provider.AccessKeySecret,
		},
		OutputPath:    &req.OutputPath,
		ForceHardLink: true,
	}

	stream, err := dfdaemon.NewDfdaemonDownloadClient(c.conn).DownloadPersistentTask(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	// Process stream responses.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("failed to receive download response: %w", err)
		}

		switch taskResp := resp.Response.(type) {
		case *dfdaemon.DownloadPersistentTaskResponse_DownloadPersistentTaskStartedResponse:
			slog.Debug("download persistent task started", "response", taskResp.DownloadPersistentTaskStartedResponse.String())
		case *dfdaemon.DownloadPersistentTaskResponse_DownloadPieceFinishedResponse:
			slog.Debug("download persistent task piece finished", "response", taskResp.DownloadPieceFinishedResponse.String())
		}
	}

	return nil
}

// Upload uploads the file to Dragonfly.
func (c *client) Upload(ctx context.Context, req *UploadRequest) error {
	if req == nil {
		return errors.New("invalid upload request")
	}

	request := &dfdaemon.UploadPersistentTaskRequest{
		Url: c.objectStorageURL(req.Digest),
		ObjectStorage: &common.ObjectStorage{
			Region:          &c.provider.Region,
			Endpoint:        &c.provider.Endpoint,
			AccessKeyId:     &c.provider.AccessKeyID,
			AccessKeySecret: &c.provider.AccessKeySecret,
		},
		Path:                   req.SrcPath,
		PersistentReplicaCount: defaultUploadPersistentReplicaCount,
	}

	_, err := dfdaemon.NewDfdaemonDownloadClient(c.conn).UploadPersistentTask(ctx, request)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("failed to upload file %s: %w", req.SrcPath, err)
	}

	return nil
}

// Close closes the grpc connection.
func (c *client) Close() error {
	return c.conn.Close()
}
