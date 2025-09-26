package jdhpc

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"

	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/hypernode/utils"

	codingcore "coding.jd.com/jcloud-api-gateway/jcloud-sdk-go/core"
	sdkcommon "coding.jd.com/jcloud-api-gateway/jcloud-sdk-go/services/common/models"
	hpcclapis "coding.jd.com/jcloud-api-gateway/jcloud-sdk-go/services/hpc/apis"
	hpcclient "coding.jd.com/jcloud-api-gateway/jcloud-sdk-go/services/hpc/client"
	hpcmodels "coding.jd.com/jcloud-api-gateway/jcloud-sdk-go/services/hpc/models"
	"volcano.sh/volcano/pkg/controllers/hypernode/api"
)

const (
	HPCSuccessCode = 0

	// HyperNode tier constants
	TierSU      = 1 // Storage Unit tier (leaf nodes)
	TierPOD     = 2 // POD tier (intermediate)
	TierCluster = 3 // Cluster tier (root)
)

func init() {
	api.RegisterDiscoverer("jdhpc", NewJDHPCDiscoverer)
}

// jdHPCDiscoverer implements the Discoverer interface for JD HPC API
type jdHPCDiscoverer struct {
	endpoint          string
	scheme            string
	regionId          string
	vpcId             string
	zones             []string
	kubeClient        clientset.Interface
	nodeLister        listersv1.NodeLister
	discoveryInterval time.Duration
	timeout           time.Duration
	client            *hpcclient.HpcClient
	stopCh            chan struct{}
}

// NewJDHPCDiscoverer creates a new JD HPC topology discoverer
func NewJDHPCDiscoverer(cfg api.DiscoveryConfig, kubeClient clientset.Interface, nodeLister listersv1.NodeLister) api.Discoverer {
	var endpoint, regionId, vpcId, scheme string
	var zones []string
	var timeout time.Duration

	if cfg.Config["endpoint"] != nil {
		endpoint = cfg.Config["endpoint"].(string)
	}
	if cfg.Config["regionId"] != nil {
		regionId = cfg.Config["regionId"].(string)
	}
	if cfg.Config["vpcId"] != nil {
		vpcId = cfg.Config["vpcId"].(string)
	}
	if cfg.Config["zone"] != nil {
		zones = cfg.Config["zone"].([]string)
	}
	if cfg.Config["timeout"] != nil {
		timeout = cfg.Config["timeout"].(time.Duration)
	}
	if cfg.Config["scheme"] != nil {
		scheme = cfg.Config["scheme"].(time.Duration)
	}

	jdhpc := &jdHPCDiscoverer{
		endpoint:          endpoint,
		scheme:            scheme,
		regionId:          regionId,
		vpcId:             vpcId,
		zones:             zones,
		discoveryInterval: cfg.Interval,
		timeout:           timeout,
		kubeClient:        kubeClient,
		nodeLister:        nodeLister,
		stopCh:            make(chan struct{}),
	}

	var accessKey, secretAccessKey string
	if cfg.Credentials != nil && cfg.Credentials.SecretRef != nil {
		var err error
		accessKey, secretAccessKey, err = jdhpc.getCredentialsFromSecret(cfg.Credentials.SecretRef.Name, cfg.Credentials.SecretRef.Namespace)
		if err != nil {
			klog.ErrorS(err, "Failed to get credentials from secret", "secretName", cfg.Credentials.SecretRef.Name)
		}
	} else {
		klog.ErrorS(nil, "JD HPC authentication credentials are not configured")
	}

	hpcClient := hpcclient.NewHpcClient(codingcore.NewCredential(accessKey, secretAccessKey))
	if jdhpc.endpoint == "" {
		jdhpc.endpoint = hpcClient.Config.Endpoint
	}
	if jdhpc.timeout != 0 {
		jdhpc.timeout = hpcClient.Config.Timeout
	}
	if jdhpc.scheme == "" {
		jdhpc.scheme = hpcClient.Config.Scheme
	}
	hpcClient.Config.SetEndpoint(jdhpc.endpoint)
	hpcClient.Config.SetTimeout(jdhpc.timeout)
	hpcClient.Config.SetScheme(jdhpc.scheme)
	jdhpc.client = hpcClient

	klog.InfoS("JD HPC discoverer initialized")

	return jdhpc
}

// Start begins the topology discovery process and returns the channel for receiving discovered topology
func (jd *jdHPCDiscoverer) Start() (chan []*topologyv1alpha1.HyperNode, error) {
	if jd.endpoint == "" || jd.regionId == "" {
		return nil, errors.New("JD HPC endpoint or regionId is not configured")
	}

	klog.InfoS("Starting JD HPC network topology discovery",
		"endpoint", jd.endpoint,
		"interval", jd.discoveryInterval,
		"timeout", jd.timeout,
		"scheme", jd.scheme,
	)

	// Create the output channel that this discoverer will manage
	outputCh := make(chan []*topologyv1alpha1.HyperNode, 10)

	// Start periodic discovery in a separate goroutine
	go jd.periodicDiscovery(outputCh)

	return outputCh, nil
}

// Stop halts the discovery process
func (jd *jdHPCDiscoverer) Stop() error {
	close(jd.stopCh)
	return nil
}

// Name returns the discoverer name
func (jd *jdHPCDiscoverer) Name() string {
	return "jdhpc"
}

type instanceNodeMap struct {
	instanceIDs          []string
	instanceIDToNodeName map[string]string
}

func (jd *jdHPCDiscoverer) getCredentialsFromSecret(name, namespace string) (string, string, error) {
	secret, err := jd.kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to get secret %s/%s: %v", namespace, name, err)
	}

	accessKeyData, ok := secret.Data["accessKey"]
	if !ok {
		return "", "", fmt.Errorf("username not found in secret %s/%s", namespace, name)
	}
	accessKey := string(accessKeyData)

	secretAccessKeyData, ok := secret.Data["secretAccessKey"]
	if !ok {
		return "", "", fmt.Errorf("password not found in secret %s/%s", namespace, name)
	}
	secretAccessKey := string(secretAccessKeyData)

	return accessKey, secretAccessKey, nil
}

// periodicDiscovery periodically discovers network topology
func (jd *jdHPCDiscoverer) periodicDiscovery(outputCh chan []*topologyv1alpha1.HyperNode) {
	// Perform immediate discovery first
	jd.discoverAndSend(outputCh)

	// Set up ticker for periodic discovery
	var ticker *time.Ticker
	if jd.discoveryInterval > 0 {
		ticker = time.NewTicker(jd.discoveryInterval)
		defer ticker.Stop()
	}

	if ticker != nil {
		for {
			select {
			case <-ticker.C:
				jd.discoverAndSend(outputCh)
			case <-jd.stopCh:
				klog.InfoS("JD HPC network topology discovery stopped, closing output channel")
				close(outputCh)
				return
			}
		}
	} else {
		// If no ticker, just wait for stop signal
		<-jd.stopCh
		klog.InfoS("JD HPC network topology discovery stopped, closing output channel")
		close(outputCh)
		return
	}
}

// discoverAndSend discovers the topology and sends it through the channel
func (jd *jdHPCDiscoverer) discoverAndSend(outputCh chan []*topologyv1alpha1.HyperNode) {
	insNodeMap, err := jd.getInstanceNodeMap()
	if err != nil {
		klog.ErrorS(err, "Failed to get instance node map")
		return
	}

	// Fetch HPC data
	hpcNetworkTopology, err := jd.fetchJDHPCData(insNodeMap)
	if err != nil {
		klog.ErrorS(err, "Failed to fetch JD HPC data")
		return
	}

	// Process HPC data into HyperNodes
	hyperNodes := jd.buildHyperNodes(hpcNetworkTopology, insNodeMap)

	// Send discovered nodes through the channel
	select {
	case outputCh <- hyperNodes:
		klog.InfoS("Sent network topology data", "hyperNodeCount", len(hyperNodes))
	case <-jd.stopCh:
		// Discovery stopped, don't attempt to send
		return
	default:
		klog.InfoS("Failed to send network topology data, channel might be full")
	}
}

// fetchJDHPCData retrieves network topology data from the JDCloud API
func (jd *jdHPCDiscoverer) fetchJDHPCData(insNodeMap *instanceNodeMap) (*hpcmodels.NetworkTopology, error) {
	filter := jd.genFilters(insNodeMap.instanceIDs)
	req := hpcclapis.NewDescribeNetworkTopologyRequestWithAllParams(jd.regionId, filter)
	resp, err := jd.client.DescribeNetworkTopology(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	if resp.Error.Code != HPCSuccessCode {
		return nil, fmt.Errorf(
			"unexpected status code: %d, message: %s, requestID: %s",
			resp.Error.Code,
			resp.Error.Message,
			resp.RequestID)
	}
	klog.InfoS("Successfully retrieved JD HPC data")

	return &resp.Result.NetworkTopology, nil
}

func (jd *jdHPCDiscoverer) getInstanceNodeMap() (*instanceNodeMap, error) {
	if jd.nodeLister == nil {
		return &instanceNodeMap{}, nil
	}
	nodes, err := jd.nodeLister.List(labels.Everything())
	if err != nil {
		return &instanceNodeMap{}, err
	}
	instanceIDs := []string{}
	instanceIDToNodeName := make(map[string]string, len(nodes))
	for _, node := range nodes {
		if node.Spec.ProviderID == "" {
			klog.ErrorS(nil, "Node provider ID is empty", "nodeName", node.Name)
			continue
		}
		instanceID := extractID(node.Spec.ProviderID)
		instanceIDs = append(instanceIDs, instanceID)
		instanceIDToNodeName[instanceID] = node.Name
	}

	return &instanceNodeMap{
		instanceIDs:          instanceIDs,
		instanceIDToNodeName: instanceIDToNodeName,
	}, nil
}

func (jd *jdHPCDiscoverer) genFilters(instanceIDs []string) []sdkcommon.Filter {
	filter := []sdkcommon.Filter{{
		Name:   "instanceId",
		Values: instanceIDs,
	}}
	if jd.vpcId != "" {
		filter = append(filter, sdkcommon.Filter{
			Name:   "vpcId",
			Values: []string{jd.vpcId},
		})
	}
	if jd.zones != nil {
		filter = append(filter, sdkcommon.Filter{
			Name:   "az",
			Values: jd.zones,
		})
	}
	return filter
}

// buildHyperNodes converts JD HPC data to HyperNode resources
func (jd *jdHPCDiscoverer) buildHyperNodes(hpcNetworkTopology *hpcmodels.NetworkTopology, insNodeMap *instanceNodeMap) []*topologyv1alpha1.HyperNode {
	hyperNodes := make([]*topologyv1alpha1.HyperNode, 0, len(hpcNetworkTopology.ClusterLayers)*3+1)

	// Build hypernodes in hierarchical order: SU -> POD -> CLUSTER
	for _, cluster := range hpcNetworkTopology.ClusterLayers {
		// Create SU hypernodes (tier 1 - leaf nodes) grouped by POD
		suHyperNodes, suHNNameByPod := jd.buildSUHyperNodes(&cluster, insNodeMap)
		hyperNodes = append(hyperNodes, suHyperNodes...)

		// Create POD hypernodes (tier 2 - intermediate) using pre-built SU hypernodes
		podHyperNodes, podHNNames := jd.buildPODHyperNodes(&cluster, suHNNameByPod)
		hyperNodes = append(hyperNodes, podHyperNodes...)

		// Create CLUSTER hypernode (tier 3 - root)
		clusterHyperNode := jd.buildClusterHyperNode(&cluster, podHNNames)
		hyperNodes = append(hyperNodes, clusterHyperNode)
	}

	return hyperNodes
}

// buildSUHyperNodes creates SU (Storage Unit) hypernodes from cluster data
// Returns a map grouped by POD layer name for efficient lookup
func (jd *jdHPCDiscoverer) buildSUHyperNodes(cluster *hpcmodels.ClusterLayer, insNodeMap *instanceNodeMap) ([]*topologyv1alpha1.HyperNode, map[string][]string) {
	suHNNameByPod := make(map[string][]string)
	var suHyperNodes []*topologyv1alpha1.HyperNode

	for _, podLayer := range cluster.PodLayers {
		podHNName := genHyperNodeKey(podLayer.LayerType, podLayer.Id, podLayer.Name)

		for _, suLayer := range podLayer.SuLayers {
			suHNName := genHyperNodeKey(suLayer.LayerType, suLayer.Id, suLayer.Name)
			nodeNames := jd.extractNodeNames(&suLayer.InstanceLayers, insNodeMap)
			suHNNameByPod[podHNName] = append(suHNNameByPod[podHNName], suHNName)
			if len(nodeNames) == 0 {
				klog.InfoS("No valid nodes found for SU layer", "suName", suHNName)
				continue
			}

			members := utils.BuildMembers(nodeNames, topologyv1alpha1.MemberTypeNode)
			suHyperNode := jd.createHyperNode(suHNName, TierSU, members)
			suHyperNodes = append(suHyperNodes, suHyperNode)

			klog.InfoS("Created SU HyperNode", "name", suHNName, "nodeCount", len(members))
		}
	}

	return suHyperNodes, suHNNameByPod
}

// buildPODHyperNodes creates POD hypernodes from pre-built SU hypernodes
func (jd *jdHPCDiscoverer) buildPODHyperNodes(cluster *hpcmodels.ClusterLayer, suHyperNodesByPod map[string][]string) ([]*topologyv1alpha1.HyperNode, []string) {
	var podHyperNodes []*topologyv1alpha1.HyperNode
	var podHNNames []string

	for _, podLayer := range cluster.PodLayers {
		podHNName := genHyperNodeKey(podLayer.LayerType, podLayer.Id, podLayer.Name)
		podHNNames = append(podHNNames, podHNName)

		// Get pre-built SU hypernodes for this POD
		members := utils.BuildMembers(suHyperNodesByPod[podHNName], topologyv1alpha1.MemberTypeHyperNode)
		podHyperNode := jd.createHyperNode(podHNName, TierPOD, members)
		podHyperNodes = append(podHyperNodes, podHyperNode)

		klog.InfoS("Created POD HyperNode", "name", podHNName, "suCount", len(members))
	}

	return podHyperNodes, podHNNames
}

// buildClusterHyperNode creates CLUSTER hypernode from POD hypernodes
func (jd *jdHPCDiscoverer) buildClusterHyperNode(cluster *hpcmodels.ClusterLayer, podHNNames []string) *topologyv1alpha1.HyperNode {
	clusterHNName := genHyperNodeKey(cluster.LayerType, cluster.Id, cluster.Name)
	members := utils.BuildMembers(podHNNames, topologyv1alpha1.MemberTypeHyperNode)

	clusterHyperNode := jd.createHyperNode(clusterHNName, TierCluster, members)

	klog.InfoS("Created CLUSTER HyperNode", "name", clusterHNName, "podCount", len(members))
	return clusterHyperNode
}

// extractNodeNames extracts node names from instance layers
func (jd *jdHPCDiscoverer) extractNodeNames(instanceLayers *[]hpcmodels.InstanceLayer, insNodeMap *instanceNodeMap) []string {
	var nodeNames []string

	for _, instance := range *instanceLayers {
		if nodeName, exists := insNodeMap.instanceIDToNodeName[instance.InstanceId]; exists {
			nodeNames = append(nodeNames, nodeName)
		} else {
			klog.ErrorS(nil, "Failed to find node name for instance", "instanceID", instance.InstanceId)
		}
	}

	return nodeNames
}

// createHyperNode creates a HyperNode with common labels
func (jd *jdHPCDiscoverer) createHyperNode(name string, tier int, members []topologyv1alpha1.MemberSpec) *topologyv1alpha1.HyperNode {
	labels := map[string]string{
		api.NetworkTopologySourceLabelKey: jd.Name(),
	}
	return utils.BuildHyperNode(name, tier, members, labels)
}

func extractID(uri string) string {
	return path.Base(uri)
}

func genHyperNodeKey(layerType, id, name string) string {
	return fmt.Sprintf("%s-%s-%s", layerType, id, name)
}
