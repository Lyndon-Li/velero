package exposer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"

	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	"github.com/vmware-tanzu/velero/pkg/util/kube"

	ctlclient "sigs.k8s.io/controller-runtime/pkg/client"

	coordinationv1 "k8s.io/api/coordination/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/hashicorp/golang-lru/v2/expirable"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"

	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type cacheElem struct {
	nodeSelections
	changeID int64
}

type nodeSelections struct {
	SelectedNode string `json:"selectedNode"`
	SelectedOS   string `json:"selecetdOS"`
	Affinity     string `json:"affinity"`
}

type selectionInfo struct {
	nodeSelections
	rawAffinity *kube.LoadAffinity
}

type quotaCacheElem struct {
	digest   string
	changeID int64
}

type vgdpWatcher struct {
	client           ctlclient.Client
	mgr              manager.Manager
	concurrency      nodeagent.LoadConcurrency
	intialized       bool
	changeID         int64
	nodeChangeID     int64
	resultCache      *expirable.LRU[cacheElem, bool]
	quotaCache       *expirable.LRU[quotaCacheElem, int]
	dataPathLoad     map[string]string
	dataPathLoadLock *sync.Mutex
}

var (
	dataPathWatcher    vgdpWatcher
	ErrDataPathNoQuota = errors.New("no quota from data path")
	leaseLockName      = "vgdp-load-sync-lock"
	leaseLockTimeout   = int32(10)
)

func StartVgdpWatcher(ctx context.Context, mgr manager.Manager, namespace string, concurrency nodeagent.LoadConcurrency) error {
	dataPathWatcher.concurrency = concurrency
	dataPathWatcher.mgr = mgr
	dataPathWatcher.client = mgr.GetClient()
	dataPathWatcher.resultCache = expirable.NewLRU[cacheElem, bool](1024, nil, time.Minute*5)
	dataPathWatcher.quotaCache = expirable.NewLRU[quotaCacheElem, int](1024, nil, time.Hour)
	dataPathWatcher.dataPathLoad = make(map[string]string)
	dataPathWatcher.dataPathLoadLock = &sync.Mutex{}

	return dataPathWatcher.initCacheClient(ctx, mgr, namespace)
}

func (w *vgdpWatcher) initCacheClient(ctx context.Context, mgr manager.Manager, namespace string) error {
	nodeInformer, err := mgr.GetCache().GetInformer(ctx, &corev1api.Node{})
	if err != nil {
		return errors.Wrap(err, "error getting node informer")
	}

	if _, err := nodeInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				atomic.AddInt64(&dataPathWatcher.changeID, 1)
				atomic.AddInt64(&dataPathWatcher.nodeChangeID, 1)
			},
			DeleteFunc: func(obj any) {
				atomic.AddInt64(&dataPathWatcher.changeID, 1)
				atomic.AddInt64(&dataPathWatcher.nodeChangeID, 1)
			},
		},
	); err != nil {
		return errors.Wrap(err, "error registering node handler")
	}

	duInformer, err := mgr.GetCache().GetInformer(ctx, &velerov2alpha1api.DataUpload{})
	if err != nil {
		return errors.Wrap(err, "error getting du informer")
	}

	if _, err := duInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj any) {
				oldDu := oldObj.(*velerov2alpha1api.DataUpload)
				newDu := newObj.(*velerov2alpha1api.DataUpload)

				if oldDu.Status.Phase == newDu.Status.Phase {
					return
				}

				if newDu.Status.Phase == velerov2alpha1api.DataUploadPhaseAccepted {
					w.dataPathLoadLock.Lock()
					if newDu.Annotations != nil {
						w.dataPathLoad[newDu.Name] = newDu.Annotations[DataPathLoadDigestAnno]
					}
					w.dataPathLoadLock.Unlock()

					return
				}

				if newDu.Status.Phase == velerov2alpha1api.DataUploadPhaseCompleted ||
					newDu.Status.Phase == velerov2alpha1api.DataUploadPhaseCanceled ||
					newDu.Status.Phase == velerov2alpha1api.DataUploadPhaseFailed {
					w.dataPathLoadLock.Lock()
					delete(w.dataPathLoad, newDu.Name)
					w.dataPathLoadLock.Unlock()

					atomic.AddInt64(&dataPathWatcher.changeID, 1)
					return
				}
			},
		},
	); err != nil {
		return errors.Wrap(err, "error registering du handler")
	}

	ddInformer, err := mgr.GetCache().GetInformer(ctx, &velerov2alpha1api.DataDownload{})
	if err != nil {
		return errors.Wrap(err, "error getting dd informer")
	}

	if _, err := ddInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj any) {
				oldDd := oldObj.(*velerov2alpha1api.DataDownload)
				newDd := newObj.(*velerov2alpha1api.DataDownload)

				if oldDd.Status.Phase == newDd.Status.Phase {
					return
				}

				if newDd.Status.Phase == velerov2alpha1api.DataDownloadPhaseAccepted {
					w.dataPathLoadLock.Lock()
					if newDd.Annotations != nil {
						w.dataPathLoad[newDd.Name] = newDd.Annotations[DataPathLoadDigestAnno]
					}
					w.dataPathLoadLock.Unlock()

					return
				}

				if newDd.Status.Phase == velerov2alpha1api.DataDownloadPhaseCompleted ||
					newDd.Status.Phase == velerov2alpha1api.DataDownloadPhaseCanceled ||
					newDd.Status.Phase == velerov2alpha1api.DataDownloadPhaseFailed {
					w.dataPathLoadLock.Lock()
					delete(w.dataPathLoad, newDd.Name)
					w.dataPathLoadLock.Unlock()

					atomic.AddInt64(&dataPathWatcher.changeID, 1)
					return
				}
			},
		},
	); err != nil {
		return errors.Wrap(err, "error registering dd handler")
	}

	pvbInformer, err := mgr.GetCache().GetInformer(ctx, &velerov1api.PodVolumeBackup{})
	if err != nil {
		return errors.Wrap(err, "error getting PVB informer")
	}

	if _, err := pvbInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj any) {
				oldPvb := oldObj.(*velerov1api.PodVolumeBackup)
				newPvb := newObj.(*velerov1api.PodVolumeBackup)

				if oldPvb.Status.Phase == newPvb.Status.Phase {
					return
				}

				if newPvb.Status.Phase == velerov1api.PodVolumeBackupPhaseAccepted {
					w.dataPathLoadLock.Lock()
					if newPvb.Annotations != nil {
						w.dataPathLoad[newPvb.Name] = newPvb.Annotations[DataPathLoadDigestAnno]
					}
					w.dataPathLoadLock.Unlock()

					return
				}

				if newPvb.Status.Phase == velerov1api.PodVolumeBackupPhaseCompleted ||
					newPvb.Status.Phase == velerov1api.PodVolumeBackupPhaseCanceled ||
					newPvb.Status.Phase == velerov1api.PodVolumeBackupPhaseFailed {
					w.dataPathLoadLock.Lock()
					delete(w.dataPathLoad, newPvb.Name)
					w.dataPathLoadLock.Unlock()

					atomic.AddInt64(&dataPathWatcher.changeID, 1)
					return
				}
			},
		},
	); err != nil {
		return errors.Wrap(err, "error registering PVB handler")
	}

	pvrInformer, err := mgr.GetCache().GetInformer(ctx, &velerov1api.PodVolumeRestore{})
	if err != nil {
		return errors.Wrap(err, "error getting PVR informer")
	}

	if _, err := pvrInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj any) {
				oldPvr := oldObj.(*velerov1api.PodVolumeRestore)
				newPvr := newObj.(*velerov1api.PodVolumeRestore)

				if oldPvr.Status.Phase == newPvr.Status.Phase {
					return
				}

				if newPvr.Status.Phase == velerov1api.PodVolumeRestorePhaseAccepted {
					w.dataPathLoadLock.Lock()
					if newPvr.Annotations != nil {
						w.dataPathLoad[newPvr.Name] = newPvr.Annotations[DataPathLoadDigestAnno]
					}
					w.dataPathLoadLock.Unlock()

					return
				}

				if newPvr.Status.Phase == velerov1api.PodVolumeRestorePhaseCompleted ||
					newPvr.Status.Phase == velerov1api.PodVolumeRestorePhaseCanceled ||
					newPvr.Status.Phase == velerov1api.PodVolumeRestorePhaseFailed {
					w.dataPathLoadLock.Lock()
					delete(w.dataPathLoad, newPvr.Name)
					w.dataPathLoadLock.Unlock()

					atomic.AddInt64(&dataPathWatcher.changeID, 1)
					return
				}
			},
		},
	); err != nil {
		return errors.Wrap(err, "error registering dd handler")
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

func AccquireExposeCheckLock(ctx context.Context, client ctlclient.Client, namespace string, holder string, timeout time.Duration) (any, error) {
	var lock *coordinationv1.Lease

	err := wait.PollUntilContextTimeout(ctx, time.Millisecond*500, timeout, true, func(ctx context.Context) (bool, error) {
		lease := &coordinationv1.Lease{}
		if err := client.Get(ctx, ctlclient.ObjectKey{Namespace: namespace, Name: leaseLockName}, lease); err != nil {
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

		if err := client.Update(ctx, lease); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}

			return false, errors.Wrap(err, "error updating lease lock")
		}

		lock = lease
		return true, nil
	})

	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, nil
		}

		return nil, errors.Wrap(err, "failed to wait holding lock")
	}

	return lock, nil
}

func ReleaseExposeCheckLock(ctx context.Context, client ctlclient.Client, lock any) error {
	lease := lock.(*coordinationv1.Lease)
	if lease == nil {
		return errors.New("no lease lock is being held")
	}

	lease.Spec.AcquireTime = nil
	lease.Spec.HolderIdentity = nil
	lease.Spec.LeaseDurationSeconds = &leaseLockTimeout

	if err := client.Update(ctx, lease); err != nil {
		return errors.Wrap(err, "error updating lease lock")
	}

	return nil
}

var funcIsDataPathConstrained = (*vgdpWatcher).isDataPathConstrained

func IsDataPathConstrained(ctx context.Context, selectedNode string, nodeOS string, affinity *kube.LoadAffinity, log logrus.FieldLogger) (bool, string) {
	if !dataPathWatcher.intialized {
		return false, ""
	}

	si, err := getSelectionInfo(selectedNode, nodeOS, affinity)
	if err != nil {
		log.WithError(err).Warn("Failed to get selection info")
		return false, ""
	}

	resultCache := cacheElem{
		nodeSelections: si.nodeSelections,
		changeID:       atomic.LoadInt64(&dataPathWatcher.changeID),
	}

	if dataPathWatcher.resultCache.Contains(resultCache) {
		return true, ""
	}

	constrained, digest := funcIsDataPathConstrained(&dataPathWatcher, ctx, si, log)

	if constrained {
		dataPathWatcher.resultCache.Add(resultCache, true)
	}

	return constrained, digest
}

func getSelectionInfo(selectedNode string, nodeOS string, affinity *kube.LoadAffinity) (*selectionInfo, error) {
	si := &selectionInfo{}
	if selectedNode != "" {
		si.SelectedNode = selectedNode

		return si, nil
	}

	affinityStr := ""
	if affinity != nil {
		s, err := json.Marshal(affinity)
		if err != nil {
			return nil, errors.Wrapf(err, "error marshaling affinity %v", affinity)
		}

		affinityStr = string(s)
	}

	si.SelectedOS = nodeOS
	si.Affinity = affinityStr
	si.rawAffinity = affinity

	return si, nil
}

func (w *vgdpWatcher) isDataPathConstrained(ctx context.Context, si *selectionInfo, log logrus.FieldLogger) (bool, string) {
	marshaled, err := json.Marshal(si.nodeSelections)
	if err != nil {
		log.WithError(err).Warn("Failed to marshal node selections")
		return false, ""
	}

	hash := sha256.Sum256(marshaled)
	digest := hex.EncodeToString(hash[:])

	quotaCache := quotaCacheElem{
		digest:   digest,
		changeID: atomic.LoadInt64(&w.nodeChangeID),
	}

	quota := 0
	if q, found := w.quotaCache.Get(quotaCache); found {
		quota = q
		log.Infof("Got quota %d for %v", quota, si.nodeSelections)
	} else {
		nodes, err := w.getCandidiateNodes(ctx, si.SelectedNode, si.SelectedOS, si.rawAffinity)
		if err != nil {
			log.WithError(err).Warn("Failed to get candidate nodes")
			return false, ""
		}

		quota = countQuotaInNodes(nodes, w.concurrency)

		w.quotaCache.Add(quotaCache, quota)

		log.Infof("Calculate quota %d for %v", quota, si.nodeSelections)
	}

	w.dataPathLoadLock.Lock()
	existing := countExisting(w.dataPathLoad, digest)
	w.dataPathLoadLock.Unlock()

	constrained := quota <= existing

	log.Infof("Quota is %v, exsiting is %v, constrained %v", quota, existing, constrained)

	if constrained {
		digest = ""
	}

	return constrained, digest
}

func (w *vgdpWatcher) getCandidiateNodes(ctx context.Context, selectedNode string, selectedOS string, affinity *kube.LoadAffinity) ([]corev1api.Node, error) {
	if selectedNode != "" {
		nd := corev1api.Node{}
		if err := w.client.Get(ctx, ctlclient.ObjectKey{Name: selectedNode}, &nd); err != nil {
			return nil, errors.Wrapf(err, "error getting selected node for data path, name %s", selectedNode)
		}

		return []corev1api.Node{nd}, nil
	}

	var selector labels.Selector
	if affinity != nil {
		s, err := metav1.LabelSelectorAsSelector(&affinity.NodeSelector)
		if err != nil {
			return nil, errors.Wrapf(err, "error getting selector from affinity %v", affinity.NodeSelector)
		}

		selector = s
	}

	if selectedOS != "" {
		r, err := labels.NewRequirement(kube.NodeOSLabel, selection.Equals, []string{selectedOS})
		if err != nil {
			return nil, errors.Wrapf(err, "error getting selector from os %s", selectedOS)
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
		return nil, errors.Wrap(err, "error listing nodes for data path")
	}

	return ndList.Items, nil
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

func countExisting(load map[string]string, digest string) int {
	existing := 0
	for _, d := range load {
		if d == digest {
			existing++
		}
	}

	return existing
}
