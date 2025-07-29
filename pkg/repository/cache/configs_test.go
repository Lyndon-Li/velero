/*
Copyright The Velero Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	corev1api "k8s.io/api/core/v1"

	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

func TestParseCacheConfigs(t *testing.T) {
	tests := []struct {
		name     string
		repoType string
		config   *corev1api.ConfigMap
		expected *CacheConfigs
	}{
		{
			name: "nil config",
		},
		{
			name:     "nil data",
			repoType: "fake-repo-type",
			config:   builder.ForConfigMap(velerov1api.DefaultNamespace, "fake-config").Result(),
		},
		{
			name:     "no config for repo type",
			repoType: "fake-repo-type",
			config:   builder.ForConfigMap(velerov1api.DefaultNamespace, "fake-config").Data("fake-repo-type-1", "{\"enableCompression\": false}").Result(),
		},
		{
			name:     "failed to unmarshall data",
			repoType: "fake-repo-type",
			config:   builder.ForConfigMap(velerov1api.DefaultNamespace, "fake-config").Data("fake-repo-type", "{").Result(),
		},
		{
			name:     "no expected values",
			repoType: "fake-repo-type",
			config:   builder.ForConfigMap(velerov1api.DefaultNamespace, "fake-config").Data("fake-repo-type", "{\"enableCompression\": false}").Result(),
			expected: &CacheConfigs{Limit: DefaultCacheLimitMB << 20},
		},
		{
			name:     "unexpected limit type",
			repoType: "fake-repo-type",
			config:   builder.ForConfigMap(velerov1api.DefaultNamespace, "fake-config").Data("fake-repo-type", "{\"cacheLimitMB\": \"abc\", \"cacheStorageClass\": \"fake-storage-class\", \"enableCompression\": false}").Result(),
			expected: &CacheConfigs{Limit: DefaultCacheLimitMB << 20, StorageClass: "fake-storage-class"},
		},
		{
			name:     "unexpected threshold type",
			repoType: "fake-repo-type",
			config:   builder.ForConfigMap(velerov1api.DefaultNamespace, "fake-config").Data("fake-repo-type", "{\"cacheLimitMB\": 123, \"residentCacheThresholdMB\": \"abc\", \"enableCompression\": false}").Result(),
			expected: &CacheConfigs{Limit: 123 << 20},
		},
		{
			name:     "unexpected storage class type",
			repoType: "fake-repo-type",
			config:   builder.ForConfigMap(velerov1api.DefaultNamespace, "fake-config").Data("fake-repo-type", "{\"cacheLimitMB\": 123, \"cacheStorageClass\": 123, \"enableCompression\": false}").Result(),
			expected: &CacheConfigs{Limit: 123 << 20},
		},
		{
			name:     "success",
			repoType: "fake-repo-type",
			config:   builder.ForConfigMap(velerov1api.DefaultNamespace, "fake-config").Data("fake-repo-type", "{\"cacheLimitMB\": 123, \"residentCacheThresholdMB\": 100, \"cacheStorageClass\": \"fake-storage-class\", \"enableCompression\": false}").Result(),
			expected: &CacheConfigs{Limit: 123 << 20, ResidentThreshold: 100 << 20, StorageClass: "fake-storage-class"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := ParseCacheConfigs(test.config, test.repoType, velerotest.NewLogger())
			if test.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, *test.expected, *result)
			}
		})
	}
}
