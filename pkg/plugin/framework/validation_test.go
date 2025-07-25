/*
Copyright 2019 the Velero contributors.

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

package framework

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateConfigKeys(t *testing.T) {
	require.NoError(t, validateConfigKeys(nil))
	require.NoError(t, validateConfigKeys(map[string]string{}))
	require.NoError(t, validateConfigKeys(map[string]string{"foo": "bar"}, "foo"))
	require.NoError(t, validateConfigKeys(map[string]string{"foo": "bar", "bar": "baz"}, "foo", "bar"))

	require.Error(t, validateConfigKeys(map[string]string{"foo": "bar"}))
	require.Error(t, validateConfigKeys(map[string]string{"foo": "bar"}, "Foo"))
	require.Error(t, validateConfigKeys(map[string]string{"foo": "bar", "boo": ""}, "foo"))

	require.NoError(t, ValidateObjectStoreConfigKeys(map[string]string{"bucket": "foo"}))
	assert.Error(t, ValidateVolumeSnapshotterConfigKeys(map[string]string{"bucket": "foo"}))
}
