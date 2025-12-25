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
	"fmt"

	"golang.org/x/sys/unix"
)

// DiskUsage is the information about disk usage.
type DiskUsage struct {
	Total       uint64
	Used        uint64
	Available   uint64
	UsedPercent float64
}

// GetDiskUsage returns the disk usage information for the specified path.
func GetDiskUsage(path string) (*DiskUsage, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return nil, fmt.Errorf("statfs failed: %w", err)
	}

	total := stat.Blocks * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	used := total - stat.Bfree*uint64(stat.Bsize)
	var usedPercent float64
	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}
	return &DiskUsage{
		Total:       total,
		Used:        used,
		Available:   available,
		UsedPercent: usedPercent,
	}, nil
}
