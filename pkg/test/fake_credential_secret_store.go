/*
Copyright the Velero contributors.

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

package test

import (
	corev1api "k8s.io/api/core/v1"
)

// SecretStore defines operations for interacting with credentials
// that are stored in Secret.
type SecretStore interface {
	// Buffer returns the secret key defined by the given selector
	Buffer(selector *corev1api.SecretKeySelector) (string, error)
}

type fakeCredentialsSecretStore struct {
	password string
	err      error
}

// Buffer returns the secret key defined by the given selector.
func (f *fakeCredentialsSecretStore) Buffer(selector *corev1api.SecretKeySelector) (string, error) {
	return f.password, f.err
}

// NewNamespacedSecretStore returns a SecretStore which can interact with credentials
// for the given namespace.
func NewNamespacedSecretStore(password string, err error) SecretStore {
	return &fakeCredentialsSecretStore{
		password: password,
		err:      err,
	}
}
