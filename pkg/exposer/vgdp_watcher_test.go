package exposer

// import (
// 	"context"
// 	"testing"
// 	"time"

// 	"github.com/hashicorp/golang-lru/v2/expirable"
// 	"github.com/sirupsen/logrus"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/require"
// 	corev1api "k8s.io/api/core/v1"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	"k8s.io/apimachinery/pkg/runtime"
// 	"sigs.k8s.io/controller-runtime/pkg/client"
// 	"sigs.k8s.io/controller-runtime/pkg/client/fake"

// 	"github.com/vmware-tanzu/velero/pkg/builder"
// 	"github.com/vmware-tanzu/velero/pkg/nodeagent"
// 	testutil "github.com/vmware-tanzu/velero/pkg/test"
// 	"github.com/vmware-tanzu/velero/pkg/util/kube"
// )

// func TestIsDataPathConstrained(t *testing.T) {
// 	tests := []struct {
// 		name          string
// 		selectedNode  string
// 		selectedOS    string
// 		affinity      *kube.LoadAffinity
// 		clientErr     bool
// 		kubeClientObj []client.Object
// 		expected      bool
// 		expectedLog   string
// 	}{
// 		{
// 			name:         "with selected node but get fail",
// 			selectedNode: "node1",
// 			clientErr:    true,
// 			expectedLog:  "Failed to get selected node for data path",
// 		},
// 		{
// 			name:         "with selected node, no pod",
// 			selectedNode: "node1",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Result(),
// 			},
// 		},
// 		{
// 			name:         "with selected node, one pod",
// 			selectedNode: "node1",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Result(),
// 				builder.ForPod("test1", "test").NodeName("node1").Result(),
// 			},
// 			expected: true,
// 		},
// 		{
// 			name:        "without selected node, list node fail",
// 			clientErr:   true,
// 			expectedLog: "Failed to list nodes for data path",
// 		},
// 		{
// 			name: "without selected node, without selected os 1",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
// 				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForPod("test1", "test").NodeName("node1").Result(),
// 				builder.ForPod("test2", "test").NodeName("node3").Result(),
// 			},
// 		},
// 		{
// 			name: "without selected node, without selected os 2",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
// 				builder.ForPod("test1", "test").NodeName("node1").Result(),
// 				builder.ForPod("test2", "test").NodeName("node2").Result(),
// 			},
// 			expected: true,
// 		},
// 		{
// 			name: "with selected os, none match",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 			},
// 			selectedOS: "windows",
// 			expected:   true,
// 		},
// 		{
// 			name: "with selected os, partially match 1",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
// 				builder.ForPod("test1", "test").NodeName("node1").Result(),
// 				builder.ForPod("test2", "test").NodeName("node2").Result(),
// 			},
// 			selectedOS: "linux",
// 			expected:   true,
// 		},
// 		{
// 			name: "with selected os, partially match 2",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux"}).Result(),
// 				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
// 				builder.ForPod("test1", "test").NodeName("node1").Result(),
// 				builder.ForPod("test2", "test").NodeName("node2").Result(),
// 				builder.ForPod("test3", "test").NodeName("node2").Result(),
// 			},
// 			selectedNode: "windows",
// 		},
// 		{
// 			name: "with affinity, partially match 1",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux", "host-usage": "dm"}).Result(),
// 				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux", "host-usage": "dm"}).Result(),
// 				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "windows"}).Result(),
// 				builder.ForPod("test1", "test").NodeName("node1").Result(),
// 				builder.ForPod("test2", "test").NodeName("node2").Result(),
// 			},
// 			affinity: &kube.LoadAffinity{
// 				NodeSelector: metav1.LabelSelector{
// 					MatchLabels: map[string]string{
// 						"host-usage": "dm",
// 					},
// 				},
// 			},
// 			expected: true,
// 		},
// 		{
// 			name: "with affinity, partially match 2",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux", "host-usage": "dm"}).Result(),
// 				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux", "host-usage": "dm"}).Result(),
// 				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "windows", "host-usage": "dm"}).Result(),
// 				builder.ForPod("test1", "test").NodeName("node1").Result(),
// 				builder.ForPod("test2", "test").NodeName("node2").Result(),
// 			},
// 			affinity: &kube.LoadAffinity{
// 				NodeSelector: metav1.LabelSelector{
// 					MatchLabels: map[string]string{
// 						"host-usage": "dm",
// 					},
// 				},
// 			},
// 		},
// 		{
// 			name: "with affinity and selected os",
// 			kubeClientObj: []client.Object{
// 				builder.ForNode("node1").Labels(map[string]string{kube.NodeOSLabel: "linux", "host-usage": "dm"}).Result(),
// 				builder.ForNode("node2").Labels(map[string]string{kube.NodeOSLabel: "linux", "host-usage": "dm"}).Result(),
// 				builder.ForNode("node3").Labels(map[string]string{kube.NodeOSLabel: "windows", "host-usage": "dm"}).Result(),
// 				builder.ForPod("test1", "test").NodeName("node1").Result(),
// 				builder.ForPod("test2", "test").NodeName("node2").Result(),
// 			},
// 			affinity: &kube.LoadAffinity{
// 				NodeSelector: metav1.LabelSelector{
// 					MatchLabels: map[string]string{
// 						"host-usage": "dm",
// 					},
// 				},
// 			},
// 			selectedOS: "linux",
// 			expected:   true,
// 		},
// 	}
// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			scheme1 := runtime.NewScheme()
// 			err := corev1api.AddToScheme(scheme1)
// 			require.NoError(t, err)

// 			scheme2 := runtime.NewScheme()

// 			var scheme *runtime.Scheme
// 			if test.clientErr {
// 				scheme = scheme2
// 			} else {
// 				scheme = scheme1
// 			}

// 			dataPathWatcher = vgdpWatcher{
// 				client:      fake.NewClientBuilder().WithScheme(scheme).WithObjects(test.kubeClientObj...).Build(),
// 				concurrency: nodeagent.LoadConcurrency{GlobalConfig: 1},
// 			}

// 			buffer := ""
// 			log := testutil.NewSingleLogger(&buffer)

// 			result := dataPathWatcher.isDataPathConstrained(context.TODO(), test.selectedNode, test.selectedOS, test.affinity, log)
// 			assert.Equal(t, test.expected, result)

// 			if test.expectedLog != "" {
// 				assert.Contains(t, buffer, test.expectedLog)
// 			}
// 		})
// 	}
// }

// func TestIsDataPathConstrained2(t *testing.T) {
// 	var changeID int64 = 11
// 	tests := []struct {
// 		name          string
// 		selectedNode  string
// 		selectedOS    string
// 		affinity      *kube.LoadAffinity
// 		initialized   bool
// 		constrained   bool
// 		existing      *cacheElem
// 		expected      bool
// 		expectedCache *cacheElem
// 	}{
// 		{
// 			name: "not initialized",
// 		},
// 		{
// 			name:         "no affinity, not constrained",
// 			initialized:  true,
// 			selectedNode: "node1",
// 			selectedOS:   "linux",
// 		},
// 		{
// 			name:         "no affinity",
// 			initialized:  true,
// 			selectedNode: "node1",
// 			selectedOS:   "linux",
// 			constrained:  true,
// 			expectedCache: &cacheElem{
// 				selectedNode: "node1",
// 				selectedOS:   "linux",
// 				changeID:     changeID,
// 			},
// 			expected: true,
// 		},
// 		{
// 			name:         "with affinity",
// 			initialized:  true,
// 			selectedNode: "node1",
// 			selectedOS:   "linux",
// 			affinity:     &kube.LoadAffinity{},
// 			constrained:  true,
// 			expectedCache: &cacheElem{
// 				selectedNode: "node1",
// 				selectedOS:   "linux",
// 				affinity:     "{\"nodeSelector\":{}}",
// 				changeID:     changeID,
// 			},
// 			expected: true,
// 		},
// 		{
// 			name:         "cached",
// 			selectedNode: "node1",
// 			selectedOS:   "linux",
// 			affinity:     &kube.LoadAffinity{},
// 			initialized:  true,
// 			existing: &cacheElem{
// 				selectedNode: "node1",
// 				selectedOS:   "linux",
// 				affinity:     "{\"nodeSelector\":{}}",
// 				changeID:     changeID,
// 			},
// 			expectedCache: &cacheElem{
// 				selectedNode: "node1",
// 				selectedOS:   "linux",
// 				affinity:     "{\"nodeSelector\":{}}",
// 				changeID:     changeID,
// 			},
// 			expected: true,
// 		},
// 	}
// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			dataPathWatcher = vgdpWatcher{
// 				intialized: test.initialized,
// 				changeID:   changeID,
// 				lru:        expirable.NewLRU[cacheElem, bool](1024, nil, time.Minute*5),
// 			}

// 			if test.existing != nil {
// 				dataPathWatcher.lru.Add(*test.existing, true)
// 			}

// 			funcIsDataPathConstrained = func(*vgdpWatcher, context.Context, string, string, *kube.LoadAffinity, logrus.FieldLogger) bool {
// 				return test.constrained
// 			}

// 			result := dataPathWatcher.IsDataPathConstrained(context.TODO(), test.selectedNode, test.selectedOS, test.affinity, testutil.NewLogger())
// 			assert.Equal(t, test.expected, result)

// 			if test.expectedCache != nil {
// 				assert.True(t, dataPathWatcher.lru.Contains(*test.expectedCache))
// 			}
// 		})
// 	}
// }

// func TestCountQuotaInNodes(t *testing.T) {
// 	globalNum := 6
// 	node1 := builder.ForNode("node-agent-node").Result()
// 	node2 := builder.ForNode("node-agent-node").Labels(map[string]string{
// 		"host-name": "node-1",
// 		"xxxx":      "yyyyy",
// 	}).Result()
// 	node3 := builder.ForNode("node-agent-node").Labels(map[string]string{
// 		"host-name": "node-2",
// 		"xxxx":      "yyyyy",
// 	}).Result()
// 	node4 := builder.ForNode("node-agent-node").Labels(map[string]string{
// 		"host-name": "node-3",
// 	}).Result()

// 	invalidLabelSelector := metav1.LabelSelector{
// 		MatchLabels: map[string]string{
// 			"inva/lid": "inva/lid",
// 		},
// 	}
// 	validLabelSelector1 := metav1.LabelSelector{
// 		MatchLabels: map[string]string{
// 			"host-name": "node-1",
// 		},
// 	}
// 	validLabelSelector2 := metav1.LabelSelector{
// 		MatchLabels: map[string]string{
// 			"xxxx": "yyyyy",
// 		},
// 	}
// 	validLabelSelector3 := metav1.LabelSelector{
// 		MatchLabels: map[string]string{
// 			"host-name": "node-2",
// 		},
// 	}

// 	tests := []struct {
// 		name        string
// 		concurrency *nodeagent.LoadConcurrency
// 		nodes       []corev1api.Node
// 		expectNum   int
// 	}{
// 		{
// 			name: "no node",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 			},
// 			expectNum: 0,
// 		},
// 		{
// 			name:  "global number with one node",
// 			nodes: []corev1api.Node{*node1},
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 			},
// 			expectNum: globalNum,
// 		},
// 		{
// 			name:  "global number with two node",
// 			nodes: []corev1api.Node{*node1, *node2},
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 			},
// 			expectNum: globalNum * 2,
// 		},
// 		{
// 			name: "failed to get selector",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: invalidLabelSelector,
// 						Number:       100,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node1},
// 			expectNum: globalNum,
// 		},
// 		{
// 			name: "rule number is invalid",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: validLabelSelector1,
// 						Number:       -1,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node1},
// 			expectNum: globalNum,
// 		},
// 		{
// 			name: "label doesn't match",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: validLabelSelector1,
// 						Number:       -1,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node1},
// 			expectNum: globalNum,
// 		},
// 		{
// 			name: "match one rule",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: validLabelSelector1,
// 						Number:       66,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node2},
// 			expectNum: 66,
// 		},
// 		{
// 			name: "match one rule, multiple nodes",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: validLabelSelector2,
// 						Number:       66,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node1, *node2, *node3},
// 			expectNum: 66*2 + globalNum,
// 		},
// 		{
// 			name: "match multiple rules",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: validLabelSelector1,
// 						Number:       66,
// 					},
// 					{
// 						NodeSelector: validLabelSelector2,
// 						Number:       36,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node2},
// 			expectNum: 36,
// 		},
// 		{
// 			name: "match multiple rules, multiple nodes",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: validLabelSelector1,
// 						Number:       66,
// 					},
// 					{
// 						NodeSelector: validLabelSelector2,
// 						Number:       36,
// 					},
// 					{
// 						NodeSelector: validLabelSelector3,
// 						Number:       16,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node1, *node2, *node3},
// 			expectNum: 36 + 16 + globalNum,
// 		},
// 		{
// 			name: "match multiple rules 2",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: validLabelSelector1,
// 						Number:       36,
// 					},
// 					{
// 						NodeSelector: validLabelSelector2,
// 						Number:       66,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node2},
// 			expectNum: 36,
// 		},
// 		{
// 			name: "match multiple rules 2, multiple nodes",
// 			concurrency: &nodeagent.LoadConcurrency{
// 				GlobalConfig: globalNum,
// 				PerNodeConfig: []nodeagent.RuledConfigs{
// 					{
// 						NodeSelector: validLabelSelector1,
// 						Number:       66,
// 					},
// 					{
// 						NodeSelector: validLabelSelector2,
// 						Number:       36,
// 					},
// 				},
// 			},
// 			nodes:     []corev1api.Node{*node1, *node2, *node4},
// 			expectNum: 36 + globalNum*2,
// 		},
// 	}
// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			num := countQuotaInNodes(test.nodes, *test.concurrency)
// 			assert.Equal(t, test.expectNum, num)
// 		})
// 	}
// }

// func TestCountExistingPods(t *testing.T) {
// 	tests := []struct {
// 		name                string
// 		pods                []corev1api.Pod
// 		nodes               []corev1api.Node
// 		expectScheduled     int
// 		expectedUnscheduled int
// 	}{
// 		{
// 			name: "test",
// 			pods: []corev1api.Pod{
// 				*builder.ForPod("test1", "test").NodeName("node1").Result(),
// 				*builder.ForPod("test2", "test").NodeName("node1").Result(),
// 				*builder.ForPod("test3", "test").NodeName("node2").Result(),
// 				*builder.ForPod("test4", "test").NodeName("node3").Result(),
// 				*builder.ForPod("test5", "test").Result(),
// 				*builder.ForPod("test6", "test").Result(),
// 			},
// 			nodes: []corev1api.Node{
// 				*builder.ForNode("node1").Result(),
// 				*builder.ForNode("node2").Result(),
// 				*builder.ForNode("node3").Result(),
// 				*builder.ForNode("node4").Result(),
// 			},
// 			expectScheduled:     4,
// 			expectedUnscheduled: 2,
// 		},
// 	}
// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			scheduled, unScheduled := countExistingPods(test.pods, test.nodes, testutil.NewLogger())
// 			assert.Equal(t, test.expectScheduled, scheduled)
// 			assert.Equal(t, test.expectedUnscheduled, unScheduled)
// 		})
// 	}
// }
