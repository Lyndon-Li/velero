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

// Code generated by client-gen. DO NOT EDIT.

package v1

import (
	"context"
	"time"

	v1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	scheme "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// SnapshotRestoresGetter has a method to return a SnapshotRestoreInterface.
// A group's client should implement this interface.
type SnapshotRestoresGetter interface {
	SnapshotRestores(namespace string) SnapshotRestoreInterface
}

// SnapshotRestoreInterface has methods to work with SnapshotRestore resources.
type SnapshotRestoreInterface interface {
	Create(ctx context.Context, snapshotRestore *v1.SnapshotRestore, opts metav1.CreateOptions) (*v1.SnapshotRestore, error)
	Update(ctx context.Context, snapshotRestore *v1.SnapshotRestore, opts metav1.UpdateOptions) (*v1.SnapshotRestore, error)
	UpdateStatus(ctx context.Context, snapshotRestore *v1.SnapshotRestore, opts metav1.UpdateOptions) (*v1.SnapshotRestore, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.SnapshotRestore, error)
	List(ctx context.Context, opts metav1.ListOptions) (*v1.SnapshotRestoreList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.SnapshotRestore, err error)
	SnapshotRestoreExpansion
}

// snapshotRestores implements SnapshotRestoreInterface
type snapshotRestores struct {
	client rest.Interface
	ns     string
}

// newSnapshotRestores returns a SnapshotRestores
func newSnapshotRestores(c *VeleroV1Client, namespace string) *snapshotRestores {
	return &snapshotRestores{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the snapshotRestore, and returns the corresponding snapshotRestore object, and an error if there is any.
func (c *snapshotRestores) Get(ctx context.Context, name string, options metav1.GetOptions) (result *v1.SnapshotRestore, err error) {
	result = &v1.SnapshotRestore{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("snapshotrestores").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of SnapshotRestores that match those selectors.
func (c *snapshotRestores) List(ctx context.Context, opts metav1.ListOptions) (result *v1.SnapshotRestoreList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1.SnapshotRestoreList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("snapshotrestores").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested snapshotRestores.
func (c *snapshotRestores) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("snapshotrestores").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a snapshotRestore and creates it.  Returns the server's representation of the snapshotRestore, and an error, if there is any.
func (c *snapshotRestores) Create(ctx context.Context, snapshotRestore *v1.SnapshotRestore, opts metav1.CreateOptions) (result *v1.SnapshotRestore, err error) {
	result = &v1.SnapshotRestore{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("snapshotrestores").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(snapshotRestore).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a snapshotRestore and updates it. Returns the server's representation of the snapshotRestore, and an error, if there is any.
func (c *snapshotRestores) Update(ctx context.Context, snapshotRestore *v1.SnapshotRestore, opts metav1.UpdateOptions) (result *v1.SnapshotRestore, err error) {
	result = &v1.SnapshotRestore{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("snapshotrestores").
		Name(snapshotRestore.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(snapshotRestore).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *snapshotRestores) UpdateStatus(ctx context.Context, snapshotRestore *v1.SnapshotRestore, opts metav1.UpdateOptions) (result *v1.SnapshotRestore, err error) {
	result = &v1.SnapshotRestore{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("snapshotrestores").
		Name(snapshotRestore.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(snapshotRestore).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the snapshotRestore and deletes it. Returns an error if one occurs.
func (c *snapshotRestores) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("snapshotrestores").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *snapshotRestores) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("snapshotrestores").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched snapshotRestore.
func (c *snapshotRestores) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result *v1.SnapshotRestore, err error) {
	result = &v1.SnapshotRestore{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("snapshotrestores").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
