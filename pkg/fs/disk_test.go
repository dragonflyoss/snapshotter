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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDiskUsage(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		expectError bool
	}{
		{
			name:        "valid root path",
			path:        "/",
			expectError: false,
		},
		{
			name:        "valid tmp path",
			path:        "/tmp",
			expectError: false,
		},
		{
			name:        "non-existent path",
			path:        "/non/existent/path",
			expectError: true,
		},
		{
			name:        "empty path",
			path:        "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			usage, err := GetDiskUsage(tt.path)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, usage)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, usage)

			// Validate disk usage values.
			assert.Greater(t, usage.Total, uint64(0))
			assert.GreaterOrEqual(t, usage.Total, usage.Used)
			assert.GreaterOrEqual(t, usage.UsedPercent, float64(0))
			assert.LessOrEqual(t, usage.UsedPercent, float64(100))
		})
	}
}
