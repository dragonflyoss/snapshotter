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
package fs

import (
	"errors"
	"fmt"
	"os"
)

// HardLink creates a hard link at dst pointing to the existing file src.
// If dst already exists and refers to the same inode as src (i.e., they are already hard-linked),
// the function returns nil without performing any action.
// If dst exists but is a different file, an error is returned to avoid unintended overwrites.
// The parent directory of dst must already exist; otherwise, an error is returned.
// This function is safe for concurrent use.
func HardLink(src, dst string) error {
	// Stat the source file to verify it exists and obtain its file info.
	srcInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat source file %q: %w", src, err)
	}

	// Check if the destination already exists.
	if isSameFile(srcInfo, dst) {
		return nil
	}

	// Attempt to create the hard link.
	if err := os.Link(src, dst); err != nil {
		// Handle race condition: another goroutine may have created the link concurrently.
		if errors.Is(err, os.ErrExist) {
			if isSameFile(srcInfo, dst) {
				return nil
			}
			// Destination was created by someone else but is a different file.
			return fmt.Errorf("destination %q already exists and is not the same file as %q", dst, src)
		}
		return fmt.Errorf("failed to create hard link from %q to %q: %w", src, dst, err)
	}

	return nil
}

// isSameFile checks if dst exists and refers to the same inode as srcInfo.
// Returns true if they are the same file, false otherwise (including if dst doesn't exist).
func isSameFile(srcInfo os.FileInfo, dst string) bool {
	dstInfo, err := os.Stat(dst)
	if err != nil {
		return false
	}

	return os.SameFile(srcInfo, dstInfo)
}
