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

```bash
go get d7y.io/snapshotter
```

## Quick Start

Refer to [examples](examples/main.go).

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

## Storage Directory Structure

```
$RootDir/
├── metadata/               # bbolt database for metadata
├── storage/
│   ├── content/            # content-addressable storage
│   └── snapshot/           # zardlinked snapshots for read-only files
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
