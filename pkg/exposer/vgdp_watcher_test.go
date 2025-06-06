package exposer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmware-tanzu/velero/pkg/builder"
	"github.com/vmware-tanzu/velero/pkg/nodeagent"
	testutil "github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/util/kube"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestIsDataPathConstrained(t *testing.T) {
	tests := []struct {
		name              string
		selectedNode      string
		selectedOS        string
		clientErr         bool
		kubeClientObj     []client.Object
		countExistingFunc func([]corev1api.Pod, []corev1api.Node) int
		expected          bool
		expectedLog       string
	}{
		{
			name:         "with selected node but get fail",
			selectedNode: "node1",
			clientErr:    true,
			expectedLog:  "Failed to get selected node for data path",
		},
		{
			name:         "with selected node, no pod",
			selectedNode: "node1",
			kubeClientObj: []client.Object{
				builder.ForNode("node1").Result(),
			},
		},
		{
			name:         "with selected node, one pod",
			selectedNode: "node1",
			kubeClientObj: []client.Object{
				builder.ForNode("node1").Result(),
				builder.ForPod("test1", "test").NodeName("node1").Result(),
			},
			expected: true,
		},
		{
			name:        "without selected node, list node fail",
			clientErr:   true,
			expectedLog: "Failed to list nodes for data path",
		},
		{
			name: "without selected node, without selected os 1",
			kubeClientObj: []client.Object{
				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForPod("test1", "test").NodeName("node1").Result(),
				builder.ForPod("test2", "test").NodeName("node3").Result(),
			},
		},
		{
			name: "without selected node, without selected os 2",
			kubeClientObj: []client.Object{
				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
				builder.ForPod("test1", "test").NodeName("node1").Result(),
				builder.ForPod("test2", "test").NodeName("node2").Result(),
			},
			expected: true,
		},
		{
			name: "with selected os, none match",
			kubeClientObj: []client.Object{
				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
			},
			selectedOS: "windows",
			expected:   true,
		},
		{
			name: "without selected node, partially match 1",
			kubeClientObj: []client.Object{
				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
				builder.ForPod("test1", "test").NodeName("node1").Result(),
				builder.ForPod("test2", "test").NodeName("node2").Result(),
			},
			selectedOS: "linux",
			expected:   true,
		},
		{
			name: "without selected node, partially match 2",
			kubeClientObj: []client.Object{
				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
				builder.ForPod("test1", "test").NodeName("node1").Result(),
				builder.ForPod("test2", "test").NodeName("node2").Result(),
				builder.ForPod("test3", "test").NodeName("node2").Result(),
			},
			selectedNode: "windows",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme1 := runtime.NewScheme()
			err := corev1api.AddToScheme(scheme1)
			require.NoError(t, err)

			scheme2 := runtime.NewScheme()

			var scheme *runtime.Scheme
			if test.clientErr {
				scheme = scheme2
			} else {
				scheme = scheme1
			}

			dataPathWatcher = vgdpWatcher{
				client:      fake.NewClientBuilder().WithScheme(scheme).WithObjects(test.kubeClientObj...).Build(),
				concurrency: nodeagent.LoadConcurrency{GlobalConfig: 1},
			}

			buffer := ""
			log := testutil.NewSingleLogger(&buffer)

			result := dataPathWatcher.IsDataPathConstrained(context.TODO(), test.selectedNode, test.selectedOS, log)
			assert.Equal(t, test.expected, result)

			if test.expectedLog != "" {
				assert.Contains(t, buffer, test.expectedLog)
			}
		})
	}
}

func TestCountQuotaInNodes(t *testing.T) {
	globalNum := 6
	node1 := builder.ForNode("node-agent-node").Result()
	node2 := builder.ForNode("node-agent-node").Labels(map[string]string{
		"host-name": "node-1",
		"xxxx":      "yyyyy",
	}).Result()
	node3 := builder.ForNode("node-agent-node").Labels(map[string]string{
		"host-name": "node-2",
		"xxxx":      "yyyyy",
	}).Result()
	node4 := builder.ForNode("node-agent-node").Labels(map[string]string{
		"host-name": "node-3",
	}).Result()

	invalidLabelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			"inva/lid": "inva/lid",
		},
	}
	validLabelSelector1 := metav1.LabelSelector{
		MatchLabels: map[string]string{
			"host-name": "node-1",
		},
	}
	validLabelSelector2 := metav1.LabelSelector{
		MatchLabels: map[string]string{
			"xxxx": "yyyyy",
		},
	}
	validLabelSelector3 := metav1.LabelSelector{
		MatchLabels: map[string]string{
			"host-name": "node-2",
		},
	}

	tests := []struct {
		name        string
		concurrency *nodeagent.LoadConcurrency
		nodes       []corev1api.Node
		expectNum   int
	}{
		{
			name: "no node",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
			},
			expectNum: 0,
		},
		{
			name:  "global number with one node",
			nodes: []corev1api.Node{*node1},
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
			},
			expectNum: globalNum,
		},
		{
			name:  "global number with two node",
			nodes: []corev1api.Node{*node1, *node2},
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
			},
			expectNum: globalNum * 2,
		},
		{
			name: "failed to get selector",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: invalidLabelSelector,
						Number:       100,
					},
				},
			},
			nodes:     []corev1api.Node{*node1},
			expectNum: globalNum,
		},
		{
			name: "rule number is invalid",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: validLabelSelector1,
						Number:       -1,
					},
				},
			},
			nodes:     []corev1api.Node{*node1},
			expectNum: globalNum,
		},
		{
			name: "label doesn't match",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: validLabelSelector1,
						Number:       -1,
					},
				},
			},
			nodes:     []corev1api.Node{*node1},
			expectNum: globalNum,
		},
		{
			name: "match one rule",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: validLabelSelector1,
						Number:       66,
					},
				},
			},
			nodes:     []corev1api.Node{*node2},
			expectNum: 66,
		},
		{
			name: "match one rule, multiple nodes",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: validLabelSelector2,
						Number:       66,
					},
				},
			},
			nodes:     []corev1api.Node{*node1, *node2, *node3},
			expectNum: 66*2 + globalNum,
		},
		{
			name: "match multiple rules",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: validLabelSelector1,
						Number:       66,
					},
					{
						NodeSelector: validLabelSelector2,
						Number:       36,
					},
				},
			},
			nodes:     []corev1api.Node{*node2},
			expectNum: 36,
		},
		{
			name: "match multiple rules, multiple nodes",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: validLabelSelector1,
						Number:       66,
					},
					{
						NodeSelector: validLabelSelector2,
						Number:       36,
					},
					{
						NodeSelector: validLabelSelector3,
						Number:       16,
					},
				},
			},
			nodes:     []corev1api.Node{*node1, *node2, *node3},
			expectNum: 36 + 16 + globalNum,
		},
		{
			name: "match multiple rules 2",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: validLabelSelector1,
						Number:       36,
					},
					{
						NodeSelector: validLabelSelector2,
						Number:       66,
					},
				},
			},
			nodes:     []corev1api.Node{*node2},
			expectNum: 36,
		},
		{
			name: "match multiple rules 2, multiple nodes",
			concurrency: &nodeagent.LoadConcurrency{
				GlobalConfig: globalNum,
				PerNodeConfig: []nodeagent.RuledConfigs{
					{
						NodeSelector: validLabelSelector1,
						Number:       66,
					},
					{
						NodeSelector: validLabelSelector2,
						Number:       36,
					},
				},
			},
			nodes:     []corev1api.Node{*node1, *node2, *node4},
			expectNum: 36 + globalNum*2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			num := countQuotaInNodes(test.nodes, *test.concurrency)
			assert.Equal(t, test.expectNum, num)
		})
	}
}

func TestCountPodsInNodes(t *testing.T) {
	tests := []struct {
		name      string
		pods      []corev1api.Pod
		nodes     []corev1api.Node
		expectNum int
	}{
		{
			name: "test",
			pods: []corev1api.Pod{
				*builder.ForPod("test1", "test").NodeName("node1").Result(),
				*builder.ForPod("test2", "test").NodeName("node1").Result(),
				*builder.ForPod("test3", "test").NodeName("node2").Result(),
				*builder.ForPod("test4", "test").NodeName("node3").Result(),
			},
			nodes: []corev1api.Node{
				*builder.ForNode("node1").Result(),
				*builder.ForNode("node2").Result(),
				*builder.ForNode("node3").Result(),
				*builder.ForNode("node4").Result(),
			},
			expectNum: 4,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			num := countPodsInNodes(test.pods, test.nodes)
			assert.Equal(t, test.expectNum, num)
		})
	}
}
