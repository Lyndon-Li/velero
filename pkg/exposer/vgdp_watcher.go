package exposer

import (
	"context"
	"crypto/sha256"
	"math"
	"slices"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
	corev1api "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"

	ctlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
)

type vgdpWatcher struct {
	client      ctlclient.Client
	concurrency nodeagent.LoadConcurrency
}

var (
	dataPathWatcher    vgdpWatcher
	ErrDataPathNoQuota = errors.New("no quota from data path")
)

func StartVgdpWatcher(ctx context.Context, clientConfig *rest.Config, namespace string, affinity *kube.LoadAffinity, concurrency nodeagent.LoadConcurrency) error {
	return dataPathWatcher.Init(ctx, clientConfig, namespace, affinity, concurrency)
}

func (w *vgdpWatcher) Init(ctx context.Context, clientConfig *rest.Config, namespace string, affinity *kube.LoadAffinity, concurrency nodeagent.LoadConcurrency) error {
	scheme := runtime.NewScheme()
	err := corev1api.AddToScheme(scheme)
	if err != nil {
		return errors.Wrap(err, "error to add velero v1 scheme")
	}

	var selector labels.Selector
	if affinity != nil {
		selector, err = v1.LabelSelectorAsSelector(&affinity.NodeSelector)
		if err != nil {
			return errors.Wrap(err, "error converting node selector")
		}
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
			&corev1api.Node{}: {
				Label: selector,
			},
		},
	}

	cluster, err := cluster.New(clientConfig, func(clusterOptions *cluster.Options) {
		clusterOptions.Scheme = scheme
		clusterOptions.Cache = cacheOption
	})
	if err != nil {
		return err
	}

	w.client = cluster.GetClient()
	w.concurrency = concurrency

	go func() {
		if err := cluster.GetCache().Start(ctx); err != nil {

		}
	}()

	return nil
}

func (w *vgdpWatcher) IsDataPathConstrained(ctx context.Context, selectedNode string, selectedOS string, log logrus.FieldLogger) bool {
	pods := &corev1api.PodList{}
	if err := w.client.List(ctx, pods, &ctlclient.ListOptions{}); err != nil {
		log.WithError(err).Warn("Failed to list pods for data path")
		return true
	}

	hash, err := getPodListHash(pods.Items, log)
	if err != nil {
		log.WithError(err).Warn("Failed to calc pod hash")
		return true
	}

	goon, err := enterCriticalSection(ctx, w.client, hash)
	if err != nil {
		log.WithError(err).Warn("Failed to check data path critical section")
		return true
	}

	if !goon {
		log.WithError(err).Warn("Failed to check data path critical section")
		return true
	}

	nodes := []corev1api.Node{}

	if selectedNode != "" {
		nd := corev1api.Node{}
		if err := w.client.Get(ctx, ctlclient.ObjectKey{Name: selectedNode}, &nd); err != nil {
			log.WithError(err).Warnf("Failed to get selected node for data path, name %s", selectedNode)
			return true
		}

		nodes = append(nodes, nd)
	} else {
		selector := ctlclient.ListOptions{}
		if selectedOS != "" {
			selector = ctlclient.ListOptions{LabelSelector: labels.SelectorFromSet(labels.Set{kube.NodeOSLabel: selectedOS})}
		}

		ndList := &corev1api.NodeList{}
		if err := w.client.List(ctx, ndList, &selector); err != nil {
			log.WithError(err).Warn("Failed to list nodes for data path")
			return true
		}

		nodes = ndList.Items

		for _, n := range nodes {
			log.Infof("Data path selected node %s", n.Name)
		}
	}

	scheduled, unscheduled := countExistingPods(pods.Items, nodes)
	quota := countQuotaInNodes(nodes, w.concurrency)

	log.Infof("Quota is %v, scheduled is %v, unscheduled is %v, constrained %v", quota, scheduled, unscheduled, quota <= scheduled+unscheduled)

	return quota <= scheduled+unscheduled
}

func countQuotaInNodes(nodes []corev1api.Node, concurrency nodeagent.LoadConcurrency) int {
	if concurrency.PerNodeConfig == nil {
		return concurrency.GlobalConfig * len(nodes)
	}

	quota := 0
	for _, node := range nodes {
		concurrentNum := math.MaxInt32

		for _, rule := range concurrency.PerNodeConfig {
			selector, err := v1.LabelSelectorAsSelector(&rule.NodeSelector)
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

func countExistingPods(pods []corev1api.Pod, nodes []corev1api.Node) (int, int) {
	ndSet := sets.Set[string]{}
	for _, n := range nodes {
		ndSet.Insert(n.Name)
	}

	scheduled := 0
	unscheduled := 0
	for _, po := range pods {
		if po.Spec.NodeName == "" {
			unscheduled++
		} else if ndSet.Has(po.Spec.NodeName) {
			scheduled++
		}
	}

	return scheduled, unscheduled
}

func getPodListHash(pods []corev1api.Pod, log logrus.FieldLogger) (string, error) {
	podNames := []string{}
	for _, p := range pods {
		log.Infof("Data path pod %s", p.Name)
		podNames = append(podNames, p.Name)
	}

	slices.Sort(podNames)

	h := sha256.New()

	for _, s := range podNames {
		if _, err := h.Write([]byte(s)); err != nil {
			return "", errors.Wrapf(err, "error writing to hash for pod name %s", s)
		}
	}

	return string(h.Sum(nil)), nil
}
