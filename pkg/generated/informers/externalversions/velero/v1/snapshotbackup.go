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

// SnapshotBackupInformer provides access to a shared informer and lister for
// SnapshotBackups.
type SnapshotBackupInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1.SnapshotBackupLister
}

type snapshotBackupInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewSnapshotBackupInformer constructs a new informer for SnapshotBackup type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewSnapshotBackupInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredSnapshotBackupInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredSnapshotBackupInformer constructs a new informer for SnapshotBackup type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredSnapshotBackupInformer(client versioned.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.VeleroV1().SnapshotBackups(namespace).List(context.TODO(), options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.VeleroV1().SnapshotBackups(namespace).Watch(context.TODO(), options)
			},
		},
		&velerov1.SnapshotBackup{},
		resyncPeriod,
		indexers,
	)
}

func (f *snapshotBackupInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredSnapshotBackupInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *snapshotBackupInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&velerov1.SnapshotBackup{}, f.defaultInformer)
}

func (f *snapshotBackupInformer) Lister() v1.SnapshotBackupLister {
	return v1.NewSnapshotBackupLister(f.Informer().GetIndexer())
}
