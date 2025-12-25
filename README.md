# Snapshotter

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go Version](https://img.shields.io/badge/Go-1.24.6+-00ADD8?logo=go)](https://go.dev/)

Snapshotter is a high-performance file snapshotting and restoration library for Go, designed to work with [Dragonfly](https://d7y.io/) for P2P-accelerated file distribution. It provides efficient file deduplication using content-addressable storage and supports OCI-compatible registries for metadata management.

## Features

- **Snapshot & Restore**: Create snapshots of files and restore them efficiently
- **Content-Addressable Storage**: Automatic deduplication using content hashing (XXH3)
- **OCI Registry Integration**: Store and retrieve snapshot metadata from OCI-compatible registries
- **Dragonfly Integration**: P2P-accelerated file distribution for fast downloads
- **Sparse File Support**: Efficient handling of sparse files
- **Hardlink-Based Deduplication**: Minimize storage usage for read-only files
- **Automatic Garbage Collection**: Background cleanup of unused content
- **Concurrent Operations**: Safe for concurrent snapshot and restore operations

## Installation

```bas
go get d7y.io/snapshotter
```


## Quick Start

Refer to [examples](examples/main.go).

## Configuration

### Config Options

| Field | Type | Description |
|-------|------|-------------|
| `RootDir` | `string` | Root directory for local storage and metadata |
| `Metadata.Registry` | `oci.Registry` | OCI registry configuration for metadata storage |
| `Dragonfly` | `dragonfly.Dragonfly` | Dragonfly client configuration |
| `Content` | `dragonfly.ContentProvider` | Content storage provider configuration |

### OCI Registry Configuration

| Field | Type | Description |
|-------|------|-------------|
| `Endpoint` | `string` | Registry endpoint URL |
| `Username` | `string` | Authentication username |
| `Password` | `string` | Authentication password |
| `Namespace` | `string` | Registry namespace/project |

### Dragonfly Configuration

| Field | Type | Description |
|-------|------|-------------|
| `Endpoint` | `string` | Dragonfly daemon endpoint (e.g., `unix:///var/run/dragonfly.sock`) |

### Content Provider Configuration

| Field | Type | Description |
|-------|------|-------------|
| `Provider` | `string` | Storage provider type (e.g., `s3`, `oss`) |
| `Region` | `string` | Storage region |
| `Endpoint` | `string` | Storage endpoint URL |
| `AccessKeyID` | `string` | Access key ID |
| `AccessKeySecret` | `string` | Access key secret |

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────────—┐
│                              Snapshotter API                                    │
│                    ┌──────────────┐       ┌──────────────┐                      │
│                    │  Snapshot()  │       │  Restore()   │                      │
│                    └──────┬───────┘       └──────┬───────┘                      │
└───────────────────────────┼──────────────────────┼──────────────────────────────┘
                            │                      │
┌───────────────────────────┼──────────────────────┼──────────────────────────────┐
│                           │   Internal Layer     │                              │
│                           ▼                      ▼                              │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                        Metadata Manager                         │            │
│  │                         (bbolt DB)                              │            │
│  │  ┌───────────────┐  ┌──────────────┐  ┌──────────────────┐      │            │
│  │  │   Snapshots   │  │   Contents   │  │ Content Metadata │      │            │
│  │  │   Metadata    │  │   Metadata   │  │   (References)   │      │            │
│  │  └───────────────┘  └──────────────┘  └──────────────────┘      │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐            │
│  │                      Storage Manager                            │            │
│  │  ┌─────────────────┐           ┌──────────────────────┐         │            │
│  │  │ Content Storage │           │  Snapshot Storage    │         │            │
│  │  │  (Sparse Files) │◄─hardlink─│  (Hardlinked Files)  │         │            │
│  │  │   [xxh3 hash]   │           │    [read-only]       │         │            │
│  │  └─────────────────┘           └──────────────────────┘         │            │
│  └─────────────────────────────────────────────────────────────────┘            │
│                                                                                 │
│  ┌──────────────────────┐                                                       │
│  │  Garbage Collector   │ (Background)                                          │
│  │  - Disk monitoring   │                                                       │
│  │  - LRU eviction      │                                                       │
│  │  - Orphan cleanup    │                                                       │
│  └──────────────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
                            │                      │
┌───────────────────────────┼──────────────────────┼──────────────────────────────┐
│                           │  External Services   │                              │
│                           ▼                      ▼                              │
│  ┌──────────────────────────────┐    ┌─────────────────────────────┐            │
│  │      OCI Registry            │    │    Dragonfly P2P Network    │            │
│  │  ┌────────────────────────┐  │    │  ┌───────────────────────┐  │            │
│  │  │  Snapshot Manifests    │  │    │  │   Content Provider    │  │            │
│  │  │  (Metadata only)       │  │    │  │   (S3/OSS/etc.)       │  │            │
│  │  └────────────────────────┘  │    │  └───────────────────────┘  │            │
│  │  ┌────────────────────────┐  │    │  ┌───────────────────────┐  │            │
│  │  │  Config Blobs          │  │    │  │   P2P Distribution    │  │            │
│  │  │  (File metadata)       │  │    │  │   (Upload/Download)   │  │            │
│  │  └────────────────────────┘  │    │  └───────────────────────┘  │            │
│  └──────────────────────────────┘    └─────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Snapshot Workflow

```
┌─────────────┐
│  Snapshot   │
│  Request    │
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│ 1. Hash & Store Content                              │
│    - Compute XXH3 hash                               │
│    - Encode as sparse file                           │
│    - Store in content/xxh3/{hash}                    │
│    - Create hardlink to snapshot/ (if read-only)     │
└──────┬───────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│ 2. Upload to Dragonfly                               │
│    - Upload to object storage via Dragonfly          │
│    - Enable P2P distribution                         │
└──────┬───────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│ 3. Build & Push OCI Manifest                         │
│    - Marshal config (file list + metadata)           │
│    - Push config blob to registry                    │
│    - Build manifest with config digest               │
│    - Tag and push manifest                           │
└──────┬───────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────┐
│ 4. Store Local Metadata                              │
│    - Store snapshot metadata in bbolt                │
│    - Update content reference count                  │
│    - Track creation & access time                    │
└──────────────────────────────────────────────────────┘
```


## Storage Directory Structure

```
$RootDir/
├── metadata/               # bbolt database for metadata
├── storage/
│   ├── content/            # content-addressable storage
│   └── snapshot/           # zardlinked snapshots for read-only files
```

## Building

```bash
# Download dependencies
make mod-download

# Build the example
make build

# Run all checks
make check
```

## Testing

```bash
# Run all tests
make test

# Run tests with coverage
make test-coverage
```

## Development

```bash
# Format code
make fmt

# Run linter (requires golangci-lint)
make lint

# Run go vet
make vet

# Tidy modules
make mod-tidy
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
