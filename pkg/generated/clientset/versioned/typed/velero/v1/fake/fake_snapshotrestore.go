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

package fake

import (
	"context"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeSnapshotRestores implements SnapshotRestoreInterface
type FakeSnapshotRestores struct {
	Fake *FakeVeleroV1
	ns   string
}

var snapshotrestoresResource = schema.GroupVersionResource{Group: "velero.io", Version: "v1", Resource: "snapshotrestores"}

var snapshotrestoresKind = schema.GroupVersionKind{Group: "velero.io", Version: "v1", Kind: "SnapshotRestore"}

// Get takes name of the snapshotRestore, and returns the corresponding snapshotRestore object, and an error if there is any.
func (c *FakeSnapshotRestores) Get(ctx context.Context, name string, options v1.GetOptions) (result *velerov1.SnapshotRestore, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(snapshotrestoresResource, c.ns, name), &velerov1.SnapshotRestore{})

	if obj == nil {
		return nil, err
	}
	return obj.(*velerov1.SnapshotRestore), err
}

// List takes label and field selectors, and returns the list of SnapshotRestores that match those selectors.
func (c *FakeSnapshotRestores) List(ctx context.Context, opts v1.ListOptions) (result *velerov1.SnapshotRestoreList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(snapshotrestoresResource, snapshotrestoresKind, c.ns, opts), &velerov1.SnapshotRestoreList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &velerov1.SnapshotRestoreList{ListMeta: obj.(*velerov1.SnapshotRestoreList).ListMeta}
	for _, item := range obj.(*velerov1.SnapshotRestoreList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested snapshotRestores.
func (c *FakeSnapshotRestores) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(snapshotrestoresResource, c.ns, opts))

}

// Create takes the representation of a snapshotRestore and creates it.  Returns the server's representation of the snapshotRestore, and an error, if there is any.
func (c *FakeSnapshotRestores) Create(ctx context.Context, snapshotRestore *velerov1.SnapshotRestore, opts v1.CreateOptions) (result *velerov1.SnapshotRestore, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(snapshotrestoresResource, c.ns, snapshotRestore), &velerov1.SnapshotRestore{})

	if obj == nil {
		return nil, err
	}
	return obj.(*velerov1.SnapshotRestore), err
}

// Update takes the representation of a snapshotRestore and updates it. Returns the server's representation of the snapshotRestore, and an error, if there is any.
func (c *FakeSnapshotRestores) Update(ctx context.Context, snapshotRestore *velerov1.SnapshotRestore, opts v1.UpdateOptions) (result *velerov1.SnapshotRestore, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(snapshotrestoresResource, c.ns, snapshotRestore), &velerov1.SnapshotRestore{})

	if obj == nil {
		return nil, err
	}
	return obj.(*velerov1.SnapshotRestore), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeSnapshotRestores) UpdateStatus(ctx context.Context, snapshotRestore *velerov1.SnapshotRestore, opts v1.UpdateOptions) (*velerov1.SnapshotRestore, error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(snapshotrestoresResource, "status", c.ns, snapshotRestore), &velerov1.SnapshotRestore{})

	if obj == nil {
		return nil, err
	}
	return obj.(*velerov1.SnapshotRestore), err
}

// Delete takes name of the snapshotRestore and deletes it. Returns an error if one occurs.
func (c *FakeSnapshotRestores) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(snapshotrestoresResource, c.ns, name), &velerov1.SnapshotRestore{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeSnapshotRestores) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(snapshotrestoresResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &velerov1.SnapshotRestoreList{})
	return err
}

// Patch applies the patch and returns the patched snapshotRestore.
func (c *FakeSnapshotRestores) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *velerov1.SnapshotRestore, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(snapshotrestoresResource, c.ns, name, pt, data, subresources...), &velerov1.SnapshotRestore{})

	if obj == nil {
		return nil, err
	}
	return obj.(*velerov1.SnapshotRestore), err
}
