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

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"

	"d7y.io/snapshotter"
	"d7y.io/snapshotter/pkg/dragonfly"
	"d7y.io/snapshotter/pkg/oci"
)

const (
	// defaultMemSize is the default size of memory dump file in bytes (10MB).
	defaultMemSize = 10 * 1024 * 1024

	// defaultStateSize is the default size of state file in bytes (256KB).
	defaultStateSize = 256 * 1024

	// defaultImgSize is the default size of image file in bytes (50MB).
	defaultImgSize = 50 * 1024 * 1024
)

// checkpointMetadata represents the metadata of a checkpoint.
type checkpointMetadata struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Version   string    `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	NumCPU    int       `json:"numCpu"`
	MemoryMB  int       `json:"memoryMb"`
}

// snapshotInfo represents the snapshot information.
type snapshotInfo struct {
	RootfsPath string   `json:"rootfsPath"`
	Layers     []string `json:"layers"`
	Config     string   `json:"config"`
}

// testCheckpoint holds the generated test checkpoint information.
type testCheckpoint struct {
	baseDir string
	name    string
	files   []snapshotter.File
}

// getEnv returns the environment variable value or the default.
func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// getConfig reads configuration from environment variables.
func getConfig() snapshotter.Config {
	return snapshotter.Config{
		RootDir: getEnv("SNAPSHOTTER_ROOT_DIR", filepath.Join(os.TempDir(), "snapshotter")),
		Metadata: snapshotter.Metadata{
			Registry: oci.Registry{
				Endpoint:  getEnv("REGISTRY_ENDPOINT", "http://localhost:5000"),
				Username:  getEnv("REGISTRY_USERNAME", "admin"),
				Password:  getEnv("REGISTRY_PASSWORD", "admin"),
				Namespace: getEnv("REGISTRY_NAMESPACE", "snapshotter"),
				Insecure:  getEnv("REGISTRY_INSECURE", "false") == "true",
			},
		},
		Dragonfly: dragonfly.Dragonfly{
			Endpoint: getEnv("DRAGONFLY_ENDPOINT", "unix:///var/run/dragonfly.sock"),
		},
		Content: dragonfly.ContentProvider{
			Provider:        getEnv("CONTENT_PROVIDER", "s3"),
			Bucket:          getEnv("CONTENT_BUCKET", "test-bucket"),
			Region:          getEnv("CONTENT_REGION", "us-east-1"),
			Endpoint:        getEnv("CONTENT_ENDPOINT", "localhost:9000"),
			AccessKeyID:     getEnv("CONTENT_ACCESS_KEY_ID", "minioadmin"),
			AccessKeySecret: getEnv("CONTENT_ACCESS_KEY_SECRET", "minioadmin"),
		},
	}
}

// generateTestCheckpoint creates a test checkpoint directory with mock files.
func generateTestCheckpoint(baseDir, name string, cpuCount, memoryMB int) (*testCheckpoint, error) {
	checkpointDir := filepath.Join(baseDir, name)
	if err := os.MkdirAll(checkpointDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	files := make([]snapshotter.File, 0, 5)

	// Generate memory dump file (read-only).
	memFilename := fmt.Sprintf("mem_%dC_%dM", cpuCount, memoryMB)
	if err := generateRandomFile(filepath.Join(checkpointDir, memFilename), defaultMemSize); err != nil {
		return nil, fmt.Errorf("failed to generate memory dump: %w", err)
	}
	files = append(files, snapshotter.File{
		RelativePath: filepath.Join(name, memFilename),
		ReadOnly:     true,
	})

	// Generate state file (read-only).
	stateFilename := fmt.Sprintf("state_%dC_%dM", cpuCount, memoryMB)
	if err := generateRandomFile(filepath.Join(checkpointDir, stateFilename), defaultStateSize); err != nil {
		return nil, fmt.Errorf("failed to generate state file: %w", err)
	}
	files = append(files, snapshotter.File{
		RelativePath: filepath.Join(name, stateFilename),
		ReadOnly:     true,
	})

	// Generate image file (not read-only).
	if err := generateRandomFile(filepath.Join(checkpointDir, "img"), defaultImgSize); err != nil {
		return nil, fmt.Errorf("failed to generate image file: %w", err)
	}
	files = append(files, snapshotter.File{
		RelativePath: filepath.Join(name, "img"),
		ReadOnly:     false,
	})

	// Generate metadata.json (read-only).
	metadata := checkpointMetadata{
		ID:        uuid.NewString(),
		Name:      name,
		Version:   "1.0.0",
		CreatedAt: time.Now(),
		NumCPU:    cpuCount,
		MemoryMB:  memoryMB,
	}
	if err := writeJSONFile(filepath.Join(checkpointDir, "metadata.json"), metadata); err != nil {
		return nil, fmt.Errorf("failed to generate metadata.json: %w", err)
	}
	files = append(files, snapshotter.File{
		RelativePath: filepath.Join(name, "metadata.json"),
		ReadOnly:     true,
	})

	// Generate snapshot.json (read-only).
	snapshot := snapshotInfo{
		RootfsPath: "/var/lib/containers/storage/overlay",
		Layers: []string{
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
		},
		Config: "config.json",
	}
	if err := writeJSONFile(filepath.Join(checkpointDir, "snapshot.json"), snapshot); err != nil {
		return nil, fmt.Errorf("failed to generate snapshot.json: %w", err)
	}
	files = append(files, snapshotter.File{
		RelativePath: filepath.Join(name, "snapshot.json"),
		ReadOnly:     true,
	})

	slog.Info("generated test checkpoint", "path", checkpointDir, "files", len(files))
	return &testCheckpoint{baseDir: baseDir, name: name, files: files}, nil
}

// generateRandomFile creates a file with random content.
func generateRandomFile(path string, size int) error {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// writeJSONFile writes a struct as JSON to the specified path.
func writeJSONFile(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func main() {
	// Load .env file (optional).
	err := godotenv.Load(".env")
	if err != nil {
		slog.Warn("failed to load env file", "err", err)
	}

	config := getConfig()
	slog.Info("initializing snapshotter", "root_dir", config.RootDir)

	s, err := snapshotter.New(config)
	if err != nil {
		slog.Error("failed to create snapshotter", "err", err)
		os.Exit(1)
	}

	// Create temporary directory for test checkpoints.
	testDir, err := os.MkdirTemp("", "snapshotter-test-*")
	if err != nil {
		slog.Error("failed to create test directory", "err", err)
		os.Exit(1)
	}
	defer os.RemoveAll(testDir)

	// Test cases with different configurations.
	testCases := []struct {
		name, version    string
		cpuCount, memory int
	}{
		{"ckpt1", "v1", 2, 1168},
		{"ckpt2", "v2", 2, 1168},
		{"ckpt3", "v3", 4, 2048},
	}

	var wg sync.WaitGroup
	started := time.Now()

	for _, tc := range testCases {
		wg.Add(1)
		go func() {
			defer wg.Done()

			checkpoint, err := generateTestCheckpoint(testDir, tc.name, tc.cpuCount, tc.memory)
			if err != nil {
				slog.Error("failed to generate checkpoint", "err", err)
				return
			}

			if err := s.Snapshot(context.Background(), &snapshotter.SnapshotRequest{
				Name:    tc.name,
				Version: tc.version,
				BaseDir: checkpoint.baseDir,
				Files:   checkpoint.files,
			}); err != nil {
				slog.Error("failed to snapshot", "name", tc.name, "version", tc.version, "err", err)
				return
			}

			outputDir := filepath.Join(os.TempDir(), "snapshotter-restore", tc.name, tc.version)
			if err := s.Restore(context.Background(), &snapshotter.RestoreRequest{
				Name:      tc.name,
				Version:   tc.version,
				OutputDir: outputDir,
			}); err != nil {
				slog.Error("failed to restore", "name", tc.name, "version", tc.version, "err", err)
				return
			}

			slog.Info("completed", "name", tc.name, "version", tc.version, "elapsed", time.Since(started))
		}()
	}

	wg.Wait()
	slog.Info("all operations completed", "total_elapsed", time.Since(started))
}
