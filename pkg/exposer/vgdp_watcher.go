package exposer

import (
	"context"
	"encoding/json"
	"math"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	"github.com/vmware-tanzu/velero/pkg/util/kube"

	ctlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	coordinationv1 "k8s.io/api/coordination/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	scheduler "k8s.io/component-helpers/scheduling/corev1"

	"github.com/hashicorp/golang-lru/v2/expirable"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

type cacheElem struct {
	changeID     int64
	selectedNode string
	selectedOS   string
	affinity     string
}

type vgdpWatcher struct {
	namespace   string
	client      ctlclient.Client
	concurrency nodeagent.LoadConcurrency
	holdingLock *coordinationv1.Lease
	changeID    int64
	lru         *expirable.LRU[cacheElem, bool]
	intialized  bool
}

var (
	dataPathWatcher    vgdpWatcher
	ErrDataPathNoQuota = errors.New("no quota from data path")
	ErrLockNotHold     = errors.New("Lease lock is held by others")
	leaseLockName      = "vgdp-lease-lock"
	leaseLockTimeout   = int32(10)
)

func StartVgdpWatcher(ctx context.Context, clientConfig *rest.Config, namespace string, concurrency nodeagent.LoadConcurrency, log logrus.FieldLogger) error {
	dataPathWatcher.namespace = namespace
	dataPathWatcher.concurrency = concurrency
	dataPathWatcher.lru = expirable.NewLRU[cacheElem, bool](1024, nil, time.Minute*5)

	return dataPathWatcher.initCacheClient(ctx, clientConfig, namespace, log)

}

func (w *vgdpWatcher) initCacheClient(ctx context.Context, clientConfig *rest.Config, namespace string, log logrus.FieldLogger) error {
	scheme := runtime.NewScheme()
	err := corev1api.AddToScheme(scheme)
	if err != nil {
		return errors.Wrap(err, "error to add corev1 scheme")
	}

	err = coordinationv1.AddToScheme(scheme)
	if err != nil {
		return errors.Wrap(err, "error to add coordinationv1 scheme")
	}

	cacheOption := ctlcache.Options{
		Scheme: scheme,
		ByObject: map[ctlclient.Object]ctlcache.ByObject{
			&corev1api.Pod{}: {
				Namespaces: map[string]ctlcache.Config{
					namespace: {
						LabelSelector: labels.SelectorFromSet(map[string]string{exposerPodLabel: "true"}),
					},
				},
			},
			&corev1api.Node{}: {},
		},
	}

	cluster, err := cluster.New(clientConfig, func(clusterOptions *cluster.Options) {
		clusterOptions.Scheme = scheme
		clusterOptions.Cache = cacheOption
	})
	if err != nil {
		return errors.Wrap(err, "error creating cluster")
	}

	podInformer, err := cluster.GetCache().GetInformer(ctx, &corev1api.Pod{})
	if err != nil {
		return errors.Wrap(err, "error getting pod informer")
	}

	if _, err := podInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				atomic.AddInt64(&w.changeID, 1)
			},
			DeleteFunc: func(obj any) {
				atomic.AddInt64(&w.changeID, 1)
			},
		},
	); err != nil {
		return errors.Wrap(err, "error registering pod handler")
	}

	nodeInformer, err := cluster.GetCache().GetInformer(ctx, &corev1api.Node{})
	if err != nil {
		return errors.Wrap(err, "error getting node informer")
	}

	if _, err := nodeInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				atomic.AddInt64(&dataPathWatcher.changeID, 1)
			},
			DeleteFunc: func(obj any) {
				atomic.AddInt64(&dataPathWatcher.changeID, 1)
			},
		},
	); err != nil {
		return errors.Wrap(err, "error registering node handler")
	}

	w.client = cluster.GetClient()

	go func() {
		if err := cluster.GetCache().Start(ctx); err != nil {
			log.WithError(err).Warn("Failed to start cluster cache")
		}
	}()

	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced, nodeInformer.HasSynced) {
		return errors.New("error waiting informer synced")
	}

	if err := w.client.Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseLockName,
			Namespace: namespace,
		},
		Spec: coordinationv1.LeaseSpec{},
	}); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "error creating lease lock")
	}

	w.intialized = true

	return nil
}

func (w *vgdpWatcher) AccquirehLock(ctx context.Context, holder string) (bool, error) {
	if !w.intialized {
		return false, errors.New("VGDP watcher is not initialized")
	}

	lease := &coordinationv1.Lease{}
	if err := w.client.Get(ctx, ctlclient.ObjectKey{Namespace: w.namespace, Name: leaseLockName}, lease); err != nil {
		return false, errors.Wrap(err, "error getting lease lock")
	}

	if lease.Spec.AcquireTime != nil && lease.Spec.LeaseDurationSeconds != nil && lease.Spec.HolderIdentity != nil {
		if !time.Now().After(lease.Spec.AcquireTime.Add(time.Second * time.Duration(*lease.Spec.LeaseDurationSeconds))) {
			return false, nil
		}
	}

	lease.Spec.AcquireTime = &metav1.MicroTime{Time: time.Now()}
	lease.Spec.HolderIdentity = &holder
	lease.Spec.LeaseDurationSeconds = &leaseLockTimeout

	if err := w.client.Update(ctx, lease); err != nil {
		if apierrors.IsConflict(err) {
			return false, nil
		}

		return false, errors.Wrap(err, "error updating lease lock")
	}

	w.holdingLock = lease

	return true, nil
}

func (w *vgdpWatcher) ReleaseLock(ctx context.Context, holder string) error {
	lease := w.holdingLock
	w.holdingLock = nil

	if lease == nil {
		return errors.New("no lease lock is being held")
	}

	lease.Spec.AcquireTime = nil
	lease.Spec.HolderIdentity = nil
	lease.Spec.LeaseDurationSeconds = &leaseLockTimeout

	if err := w.client.Update(ctx, lease); err != nil {
		return errors.Wrap(err, "error updating lease lock")
	}

	return nil
}

var funcIsDataPathConstrained = (*vgdpWatcher).isDataPathConstrained

func (w *vgdpWatcher) IsDataPathConstrained(ctx context.Context, selectedNode string, selectedOS string, affinity *kube.LoadAffinity, log logrus.FieldLogger) bool {
	if !w.intialized {
		return false
	}

	affinityStr := ""
	if affinity != nil {
		s, err := json.Marshal(affinity)
		if err != nil {
			log.WithError(err).Warnf("Failed to marshal affinity %v", affinity)
			return false
		}

		affinityStr = string(s)
	}

	lruCache := cacheElem{
		selectedNode: selectedNode,
		selectedOS:   selectedOS,
		affinity:     affinityStr,
		changeID:     atomic.LoadInt64(&w.changeID),
	}

	if w.lru.Contains(lruCache) {
		return true
	}

	constrained := funcIsDataPathConstrained(w, ctx, selectedNode, selectedOS, affinity, log)

	if constrained {
		w.lru.Add(lruCache, true)
	}

	return constrained
}

func (w *vgdpWatcher) isDataPathConstrained(ctx context.Context, selectedNode string, selectedOS string, affinity *kube.LoadAffinity, log logrus.FieldLogger) bool {
	nodes := []corev1api.Node{}

	if selectedNode != "" {
		nd := corev1api.Node{}
		if err := w.client.Get(ctx, ctlclient.ObjectKey{Name: selectedNode}, &nd); err != nil {
			log.WithError(err).Warnf("Failed to get selected node for data path, name %s", selectedNode)
			return false
		}

		nodes = append(nodes, nd)
	} else {
		var selector labels.Selector
		if affinity != nil {
			s, err := metav1.LabelSelectorAsSelector(&affinity.NodeSelector)
			if err != nil {
				log.WithError(err).Warnf("Failed to get selector from affinity %v", affinity.NodeSelector)
				return false
			}

			selector = s
		}

		if selectedOS != "" {
			r, err := labels.NewRequirement(kube.NodeOSLabel, selection.Equals, []string{selectedOS})
			if err != nil {
				log.WithError(err).Warnf("Failed to get selector from os %s", selectedOS)
				return false
			}

			if selector == nil {
				selector = labels.NewSelector()
			}

			selector = selector.Add(*r)
		}

		listOpt := ctlclient.ListOptions{}
		if selector != nil {
			listOpt = ctlclient.ListOptions{LabelSelector: selector}
		}

		ndList := &corev1api.NodeList{}
		if err := w.client.List(ctx, ndList, &listOpt); err != nil {
			log.WithError(err).Warn("Failed to list nodes for data path")
			return false
		}

		nodes = ndList.Items
	}

	pods := &corev1api.PodList{}
	if err := w.client.List(ctx, pods, &ctlclient.ListOptions{}); err != nil {
		log.WithError(err).Warn("Failed to list pods for data path")
		return false
	}

	scheduled, unscheduled := countExistingPods(pods.Items, nodes, log)
	quota := countQuotaInNodes(nodes, w.concurrency)
	constrained := quota <= scheduled+unscheduled

	log.Debugf("Quota is %v, scheduled is %v, unscheduled is %v, constrained %v", quota, scheduled, unscheduled, constrained)

	return constrained
}

func countQuotaInNodes(nodes []corev1api.Node, concurrency nodeagent.LoadConcurrency) int {
	if concurrency.PerNodeConfig == nil {
		return concurrency.GlobalConfig * len(nodes)
	}

	quota := 0
	for _, node := range nodes {
		concurrentNum := math.MaxInt32

		for _, rule := range concurrency.PerNodeConfig {
			selector, err := metav1.LabelSelectorAsSelector(&rule.NodeSelector)
			if err != nil {
				continue
			}

			if rule.Number <= 0 {
				continue
			}

			if selector.Matches(labels.Set(node.GetLabels())) {
				if concurrentNum > rule.Number {
					concurrentNum = rule.Number
				}
			}
		}

		if concurrentNum == math.MaxInt32 {
			quota += concurrency.GlobalConfig
		} else {
			quota += concurrentNum
		}
	}

	return quota
}

func countExistingPods(pods []corev1api.Pod, nodes []corev1api.Node, log logrus.FieldLogger) (int, int) {
	scheduled := 0
	unscheduled := 0
	for _, po := range pods {
		for _, n := range nodes {
			if po.Spec.NodeName == n.Name {
				scheduled++
				break
			}

			if po.Spec.NodeName == "" {
				if !labels.SelectorFromSet(po.Spec.NodeSelector).Matches(labels.Set(n.GetLabels())) {
					continue
				}

				match, err := scheduler.MatchNodeSelectorTerms(&n, po.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
				if err != nil {
					log.WithError(err).Warnf("Failed to check affinity of pod %s against node %s", po.Name, n.Name)
					continue
				}

				if !match {
					continue
				}

				unscheduled++
				break
			}
		}
	}

	return scheduled, unscheduled
}
