package binding

import (
	"fmt"
	"math"
	"strings"
	"time"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	gocache "github.com/patrickmn/go-cache"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

const defaultRsourceBingdingControllerCacheExpiration time.Duration = time.Duration(30 * time.Minute)

type ExpansionProgress struct {
	ClusterName              string    `json:"clusterName"`              // cluster name
	Namespace                string    `json:"namespace"`                // namespace
	Name                     string    `json:"name"`                     // name
	Progress                 int       `json:"progress"`                 // progress：0-100
	CurrentAvailableReplicas int       `json:"currentAvailableReplicas"` // current available replicas
	BeginAvailableReplicas   int       `json:"beginAvailableReplicas"`   // begin available replicas
	FinalMinReplicas         int       `json:"finalMinReplicas"`         // final min replicas
	ReplicasChangeStatus     string    `json:"replicasChangeStatus"`     // replicas change status
	LastUpdate               time.Time `json:"lastUpdate"`               // last update time
}

const ATMSNodeCmPrefix string = "atms-node-conf-"

func GetDeploymentName(name string) string {
	if strings.HasPrefix(name, ATMSNodeCmPrefix) {
		name = strings.Replace(name, ATMSNodeCmPrefix, "", 1)
	}
	return name
}

func SyncTargetClusterToCache(goCache *gocache.Cache, namespace, name string, targetClusters []workv1alpha2.TargetCluster) {
	name = GetDeploymentName(name)
	key := fmt.Sprintf("rb-clusters-%s-%s", namespace, name)
	goCache.Set(key, targetClusters, gocache.NoExpiration)
}

func GetTargetClusterFromCache(goCache *gocache.Cache, namespace, name string) ([]workv1alpha2.TargetCluster, error) {
	name = GetDeploymentName(name)
	key := fmt.Sprintf("rb-clusters-%s-%s", namespace, name)
	it, found := goCache.Get(key)
	if !found {
		return nil, fmt.Errorf("target cluster %s not found", key)
	}
	targetClusters, ok := it.([]workv1alpha2.TargetCluster)
	if !ok {
		return nil, fmt.Errorf("target cluster %s is not []workv1alpha2.TargetCluster", key)
	}
	return targetClusters, nil
}

func SyncReplicasProgressMapToCache(goCache *gocache.Cache, namespace, name string, progress map[string]*ExpansionProgress) {
	name = GetDeploymentName(name)
	key := fmt.Sprintf("replicas-progress-%s-%s", namespace, name)
	goCache.Set(key, progress, defaultRsourceBingdingControllerCacheExpiration)
	var progressStatus string
	for cluster, progress := range progress {
		progressStatus += fmt.Sprintf("cluster %s progress: %+v; ", cluster, *progress)
	}
	klog.Infof("Sync replicas progress map to go cache for workload %s/%s: %s", namespace, name, progressStatus)
}

func GetReplicasProgressFromCache(goCache *gocache.Cache, namespace, name string) (map[string]*ExpansionProgress, error) {
	name = GetDeploymentName(name)
	key := fmt.Sprintf("replicas-progress-%s-%s", namespace, name)
	it, found := goCache.Get(key)
	if !found {
		return nil, fmt.Errorf("replicas progress %s not found", key)
	}
	progress, ok := it.(map[string]*ExpansionProgress)
	if !ok {
		return nil, fmt.Errorf("replicas progress %s is not map[string]*ExpansionProgress", key)
	}
	var progressStatus string
	for cluster, clusterReplicasProgress := range progress {
		progressStatus += fmt.Sprintf("cluster %s progress: %+v; ", cluster, *clusterReplicasProgress)
	}
	klog.Infof("Get replicas progress from go cache for workload %s/%s: %s", namespace, name, progressStatus)
	return progress, nil
}

// IsOtherReachScaleUpThreshold checks if it's safe to proceed with scaling operations for the current cluster.
//
// This function determines whether other clusters have completed their scale-up operations enough
// to allow the current cluster to proceed with its scaling (typically scale-down).
//
// Return true (safe to proceed) when:
// 1. No clusters are scaling up (all are stable or scaling down)
// 2. All scaling down clusters have 0 current replicas (already scaled down)
// 3. Any scaling up cluster (including current if it's the only one) has reached threshold
//
// Return false (wait) when:
// - There are scaling up clusters but none have reached the threshold yet
func IsOtherReachScaleUpThreshold(goCache *gocache.Cache, karmadaSearchCli *SKarmadaSearch, currCluster, namespace, name string) (bool, error) {
	name = GetDeploymentName(name)

	// Step 1: Get cached progress and latest cluster status
	progressMap, err := GetReplicasProgressFromCache(goCache, namespace, name)
	if err != nil {
		return false, fmt.Errorf("failed to get replicas progress from cache: %w", err)
	}

	startTime := time.Now()
	latestDeployments, err := karmadaSearchCli.GetDeploymentsFromKarmadaSearch(types.NamespacedName{Namespace: namespace, Name: name})
	if err != nil {
		return false, fmt.Errorf("failed to get deployments from karmadaSearch: %w", err)
	}
	latestClusterAvailableReplicasMap, err := GetClusterAvailableReplicasMap(latestDeployments)
	if err != nil {
		return false, fmt.Errorf("failed to get cluster available replicas: %w", err)
	}
	klog.Infof("Retrieved deployments from KarmadaSearch for %s/%s in %v, available replicas: %+v",
		namespace, name, time.Since(startTime), latestClusterAvailableReplicasMap)

	// Step 2: Update progress and collect scaling up/down clusters
	var scalingUpClusters []string
	var scalingDownClusters []string
	for clusterName, progress := range progressMap {
		// Update current available replicas
		if availableReplicas, ok := latestClusterAvailableReplicasMap[clusterName]; ok {
			progress.CurrentAvailableReplicas = availableReplicas
			progress.LastUpdate = time.Now()
		}

		// Collect scaling up and down clusters
		if progress.ReplicasChangeStatus == workv1alpha2.ReplicaChangeStatusScalingUp {
			scalingUpClusters = append(scalingUpClusters, clusterName)
		} else if progress.ReplicasChangeStatus == workv1alpha2.ReplicaChangeStatusScalingDown {
			scalingDownClusters = append(scalingDownClusters, clusterName)
		}
	}

	// Always sync progress back to cache
	defer SyncReplicasProgressMapToCache(goCache, namespace, name, progressMap)

	// Step 3: No scaling up clusters - safe to proceed
	if len(scalingUpClusters) == 0 {
		klog.Infof("Cluster %s/%s/%s: No clusters are scaling up, allowing operation to proceed, return true",
			currCluster, namespace, name)
		return true, nil
	}

	// Step 3.5: All scaling down clusters have 0 replicas - safe to proceed
	if len(scalingDownClusters) > 0 {
		allScaledDown := true
		for _, clusterName := range scalingDownClusters {
			if progressMap[clusterName].CurrentAvailableReplicas > 0 {
				allScaledDown = false
				break
			}
		}
		if allScaledDown {
			klog.Infof("Cluster %s/%s/%s: All scaling down clusters %v have 0 replicas (already scaled down), allowing operation to proceed, return true",
				currCluster, namespace, name, scalingDownClusters)
			return true, nil
		}
	}

	// Step 4: Check if any scaling up cluster has reached threshold
	// For single cluster scaling: check the current cluster itself
	// For multi-cluster scaling: check other clusters (excluding current)
	shouldCheckCurrent := len(scalingUpClusters) == 1

	for _, clusterName := range scalingUpClusters {
		// Skip current cluster if there are multiple scaling up clusters
		if clusterName == currCluster && !shouldCheckCurrent {
			klog.Infof("Cluster %s/%s/%s: Skipping current cluster check (multi-cluster scaling)",
				currCluster, namespace, name)
			continue
		}

		progress := progressMap[clusterName]
		reached, reason := checkScaleUpThreshold(progress)

		if reached {
			klog.Infof("Cluster %s/%s/%s: Scale up threshold reached in %s, %s, return true",
				currCluster, namespace, name, clusterName, reason)
			return true, nil
		}

		klog.Infof("Cluster %s/%s/%s: Cluster %s has not reached threshold yet, %s",
			currCluster, namespace, name, clusterName, reason)
	}

	// Step 5: Scaling up clusters exist but none reached threshold
	klog.Infof("Cluster %s/%s/%s: Waiting for scaling up clusters %v to reach threshold, return false",
		currCluster, namespace, name, scalingUpClusters)
	return false, nil
}

// checkScaleUpThreshold determines if a cluster has reached the scale-up threshold.
// Returns (reached bool, reason string) explaining the result.
func checkScaleUpThreshold(progress *ExpansionProgress) (bool, string) {
	// Calculate percentage-based threshold (configured via SCALE_UP_THRESHOLD_RATIO)
	percentageThreshold := int(math.Ceil(float64(progress.FinalMinReplicas) * EnvScaleUpThresholdRatio))

	// Calculate increment-based threshold (at least 1 more replica)
	incrementThreshold := progress.BeginAvailableReplicas + 1

	current := progress.CurrentAvailableReplicas

	// Check if either threshold is met
	if current >= percentageThreshold {
		return true, fmt.Sprintf("current replicas %d >= percentage threshold %d (%.0f%% of %d)",
			current, percentageThreshold, EnvScaleUpThresholdRatio*100, progress.FinalMinReplicas)
	}

	if current >= incrementThreshold {
		return true, fmt.Sprintf("current replicas %d >= increment threshold %d (begin %d + 1)",
			current, incrementThreshold, progress.BeginAvailableReplicas)
	}

	// Threshold not reached
	return false, fmt.Sprintf("current replicas %d < thresholds (percentage: %d, increment: %d)",
		current, percentageThreshold, incrementThreshold)
}
