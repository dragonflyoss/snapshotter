//go:build linux || darwin

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

package sparsefile

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// magic is the file format identifier for the sparse packing format.
const magic = "SPF1"

// Extent represents a contiguous non‑hole data region in the sparse file.
type Extent struct {
	Offset int64 // Offset is the starting byte position in the original file.
	Length int64 // Length is the number of bytes in this extent.
}

// getExtents discovers all non‑hole extents in the given file using SEEK_DATA and SEEK_HOLE.
// It returns the list of extents and the logical file size.
// The caller must ensure that f refers to a regular file on a filesystem that supports sparse files.
func getExtents(f *os.File) ([]Extent, int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := fi.Size()
	if size == 0 {
		// Empty file has no extents.
		return nil, 0, nil
	}

	fd := int(f.Fd())
	var exts []Extent
	off := int64(0)

	for off < size {
		// Find the next data region starting from the current offset.
		dataOff, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			// ENXIO means there is no more data and the rest is a hole.
			if err == unix.ENXIO {
				break
			}

			// ENOTSUP or EINVAL indicates that this filesystem does not support SEEK_DATA.
			if err == unix.ENOTSUP || err == unix.EINVAL {
				return nil, 0, fmt.Errorf("SEEK_DATA not supported on this FS")
			}
			return nil, 0, fmt.Errorf("SEEK_DATA: %w", err)
		}
		if dataOff >= size {
			// Data offset beyond end of file means there is no more data.
			break
		}

		// From the data offset, find the following hole to determine the extent length.
		holeOff, err := unix.Seek(fd, dataOff, unix.SEEK_HOLE)
		if err != nil {
			// ENOTSUP or EINVAL indicates that this filesystem does not support SEEK_HOLE.
			if err == unix.ENOTSUP || err == unix.EINVAL {
				return nil, 0, fmt.Errorf("SEEK_HOLE not supported on this FS")
			}
			return nil, 0, fmt.Errorf("SEEK_HOLE: %w", err)
		}
		if holeOff > size {
			// Clamp hole offset to file size to avoid overshooting.
			holeOff = size
		}

		exts = append(exts, Extent{
			Offset: dataOff,
			Length: holeOff - dataOff,
		})
		// Continue from the end of this extent.
		off = holeOff
	}

	return exts, size, nil
}

// Encode reads a potentially sparse file from the given path and writes a compact
// representation of its non‑hole data extents to out using the custom SPF1 format.
// The output format is:
//
//	magic(4) | size(uint64) | count(uint64) |
//	repeated count times: offset(uint64) | length(uint64) | raw data bytes.
func Encode(path string, out io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Discover all extents and the logical file size.
	exts, size, err := getExtents(f)
	if err != nil {
		return err
	}

	// Write magic header.
	if _, err := out.Write([]byte(magic)); err != nil {
		return err
	}
	// Write original file size.
	if err := binary.Write(out, binary.BigEndian, uint64(size)); err != nil {
		return err
	}
	// Write number of extents.
	if err := binary.Write(out, binary.BigEndian, uint64(len(exts))); err != nil {
		return err
	}

	// Write each extent descriptor followed by its raw data.
	for _, e := range exts {
		if err := binary.Write(out, binary.BigEndian, uint64(e.Offset)); err != nil {
			return err
		}
		if err := binary.Write(out, binary.BigEndian, uint64(e.Length)); err != nil {
			return err
		}

		// Seek to the extent offset and stream its data.
		if _, err := f.Seek(e.Offset, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.CopyN(out, f, e.Length); err != nil {
			return err
		}
	}

	return nil
}

// Decode reads an SPF1‑encoded sparse file from in and reconstructs the original
// sparse layout at destPath.
// It recreates the file with the recorded logical size and writes only the stored extents
// at their original offsets so that holes remain sparse on supported filesystems.
func Decode(in io.Reader, destPath string) error {
	// Read and validate magic.
	m := make([]byte, len(magic))
	if _, err := io.ReadFull(in, m); err != nil {
		return err
	}
	if string(m) != magic {
		return fmt.Errorf("invalid magic: %q", m)
	}

	// Read logical size and extent count.
	var sizeU, nU uint64
	if err := binary.Read(in, binary.BigEndian, &sizeU); err != nil {
		return err
	}
	if err := binary.Read(in, binary.BigEndian, &nU); err != nil {
		return err
	}
	size, n := int64(sizeU), int(nU)

	// Create destination file and set its logical size to create holes.
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := f.Truncate(size); err != nil {
		return err
	}

	// For each extent, read its descriptor and write the raw data at the correct offset.
	for i := 0; i < n; i++ {
		var offU, lenU uint64
		if err := binary.Read(in, binary.BigEndian, &offU); err != nil {
			return err
		}
		if err := binary.Read(in, binary.BigEndian, &lenU); err != nil {
			return err
		}
		off := int64(offU)
		length := int64(lenU)

		if _, err := f.Seek(off, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.CopyN(f, in, length); err != nil {
			return err
		}
	}

	return nil
}
