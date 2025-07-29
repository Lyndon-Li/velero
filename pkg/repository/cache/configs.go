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
	"encoding/json"

	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
)

type CacheConfigs struct {
	// StorageClass specifies the storage class for cache volumes
	StorageClass string

	// Limit specifies the maximum size of the cache data
	Limit int64

	// ResidentThreshold specifies the minimum size of the cache data to create a cache volume
	ResidentThreshold int64
}

const (
	DefaultCacheLimitMB = 5 << 10

	limitKey             = "cacheLimitMB"
	residentThresholdKey = "residentCacheThresholdMB"
	storageClassKey      = "cacheStorageClass"
)

func ParseCacheConfigs(repoConfigs *corev1api.ConfigMap, repoType string, log logrus.FieldLogger) *CacheConfigs {
	if repoConfigs == nil || repoConfigs.Data == nil {
		return nil
	}

	jsonData, found := repoConfigs.Data[repoType]
	if !found {
		log.Info("No data for repo type %s in config map", repoType)
		return nil
	}

	var unmarshalled map[string]any
	if err := json.Unmarshal([]byte(jsonData), &unmarshalled); err != nil {
		log.WithError(err).Warnf("error unmarshalling config data for repo type %s from data %v", repoType, jsonData)
		return nil
	}

	cacheConfigs := &CacheConfigs{}

	var limit int64 = DefaultCacheLimitMB << 20
	if v, found := unmarshalled[limitKey]; found {
		if iv, ok := v.(float64); ok {
			limit = int64(iv) << 20
		} else {
			log.Warnf("ignore cache limit for repo type %s from data %v", repoType, v)
		}
	}
	cacheConfigs.Limit = limit

	var threshold int64
	if v, found := unmarshalled[residentThresholdKey]; found {
		if iv, ok := v.(float64); ok {
			threshold = int64(iv) << 20
		} else {
			log.Warnf("ignore cache threshold for repo type %s from data %v", repoType, v)
		}
	}
	cacheConfigs.ResidentThreshold = threshold

	if v, found := unmarshalled[storageClassKey]; found {
		if iv, ok := v.(string); ok {
			cacheConfigs.StorageClass = iv
		} else {
			log.Warnf("ignore cache storage class for repo type %s from data %v", repoType, v)
		}
	}

	return cacheConfigs
}
