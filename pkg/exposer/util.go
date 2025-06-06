package exposer

import (
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/client"
	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
	corev1api "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"

	clientcache "k8s.io/client-go/tools/cache"
	ctlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type dataPathPodWatcher struct {
}

func (w *dataPathPodWatcher) Init(ctx context.Context, factory client.Factory) error {
	clientConfig, err := factory.ClientConfig()
	if err != nil {
		return errors.Wrap(err, "error to create client config")
	}

	scheme := runtime.NewScheme()
	if err := corev1api.AddToScheme(scheme); err != nil {
		return errors.Wrap(err, "error to add velero v1 scheme")
	}

	cacheOption := ctlcache.Options{
		Scheme: scheme,
		ByObject: map[ctlclient.Object]ctlcache.ByObject{
			&corev1api.Pod{}: {
				Label: labels.SelectorFromSet(map[string]string{ExposePodLabel: "true"}),
			},
			&corev1api.Node{}: {},
		},
	}

	cache, err := ctlcache.New(clientConfig, cacheOption)
	if err != nil {
		return errors.Wrap(err, "error to create client cache")
	}

	go func() {
		if err := cache.Start(ctx); err != nil {

		}
	}()

	nodeInformer, err := cache.GetInformer(ctx, &corev1api.Node{})
	if err != nil {
		return errors.Wrap(err, "error getting node informer")
	}

	nodeHandler, err := nodeInformer.AddEventHandler(
		clientcache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
			},
			DeleteFunc: func(obj any) {
			},
		},
	)
	if err != nil {
		return errors.Wrap(err, "error registering pod handler")
	}

	podInformer, err := cache.GetInformer(ctx, &corev1api.Pod{})
	if err != nil {
		return errors.Wrap(err, "error getting pod informer")
	}

	podHandler, err := podInformer.AddEventHandler(
		clientcache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
			},
			DeleteFunc: func(obj any) {
			},
		},
	)
	if err != nil {
		return errors.Wrap(err, "error registering pod handler")
	}
}

func IsDataPathConstrained(ctx context.Context, kubeClient kubernetes.Interface, namespace string, concurrency nodeagent.LoadConcurrency, affinity *kube.LoadAffinity, selectedNode string, log logrus.FieldLogger) bool {
	nodes := []*corev1api.Node{}
	nodeName := []string{}

	if selectedNode != "" {
		nd, err := kubeClient.CoreV1().Nodes().Get(ctx, selectedNode, v1.GetOptions{})
		if err != nil {
			log.WithError(err).Warn("Failed to get selected node for data path")
			return false
		}

		nodes = append(nodes, nd)
		nodeName = append(nodeName, nd.Name)
	} else {
		listOption := v1.ListOptions{}
		if affinity != nil {
			listOption.LabelSelector = v1.FormatLabelSelector(&affinity.NodeSelector)
		}

		nds, err := kubeClient.CoreV1().Nodes().List(ctx, listOption)
		if err != nil {
			log.WithError(err).Warn("Failed to get list nodes for data path")
			return false
		}

		for i, nd := range nds.Items {
			nodes = append(nodes, &nds.Items[i])
			nodeName = append(nodeName, nd.Name)
		}
	}

	existing, err := kube.CountPodsInNodes(ctx, kubeClient.CoreV1(), namespace, exposePodLabel+"=true", nodeName)
	if err != nil {
		log.WithError(err).Warn("Failed to count existing data path pods")
		return false
	}

	quota := countQuotaInNodes(nodes, concurrency)

	log.Infof("Quota is %v, existing is %v", quota, existing)

	return quota <= existing
}

func countQuotaInNodes(nodes []*corev1api.Node, concurrency nodeagent.LoadConcurrency) int {
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
