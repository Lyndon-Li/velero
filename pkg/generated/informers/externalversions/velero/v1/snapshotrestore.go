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

// Code generated by informer-gen. DO NOT EDIT.

package v1

import (
	"context"
	time "time"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	versioned "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
	internalinterfaces "github.com/vmware-tanzu/velero/pkg/generated/informers/externalversions/internalinterfaces"
	v1 "github.com/vmware-tanzu/velero/pkg/generated/listers/velero/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// SnapshotRestoreInformer provides access to a shared informer and lister for
// SnapshotRestores.
type SnapshotRestoreInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1.SnapshotRestoreLister
}

type snapshotRestoreInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewSnapshotRestoreInformer constructs a new informer for SnapshotRestore type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewSnapshotRestoreInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredSnapshotRestoreInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredSnapshotRestoreInformer constructs a new informer for SnapshotRestore type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredSnapshotRestoreInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.VeleroV1().SnapshotRestores(namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.VeleroV1().SnapshotRestores(namespace).Watch(context.TODO(), options)
			},
		},
		&velerov1.SnapshotRestore{},
		resyncPeriod,
		indexers,
	)
}

func (f *snapshotRestoreInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredSnapshotRestoreInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *snapshotRestoreInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&velerov1.SnapshotRestore{}, f.defaultInformer)
}

func (f *snapshotRestoreInformer) Lister() v1.SnapshotRestoreLister {
	return v1.NewSnapshotRestoreLister(f.Informer().GetIndexer())
}
