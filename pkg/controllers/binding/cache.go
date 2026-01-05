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
		progressStatus += fmt.Sprintf("\ncluster %s progress: %+v; ", cluster, *progress)
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

func IsOtherReachScaleUpThreshold(goCache *gocache.Cache, karmadaSearchCli *SKarmadaSearch, currCluster, namespace, name string) (bool, error) {
	isHasScalingUpCluster := false
	scalingUpClusters := []string{}
	scalingDownClusters := []string{}
	allScalingDownReplicasZero := true

	name = GetDeploymentName(name)
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
	klog.Infof("Success get deployments from karmadaSearch for %s/%s took %v, latestClusterAvailableReplicasMap: %+v", namespace, name, time.Since(startTime), latestClusterAvailableReplicasMap)

	// First pass: collect scaling down clusters and check if all their replicas are zero
	for cluster, progress := range progressMap {
		availableReplicas, ok := latestClusterAvailableReplicasMap[cluster]
		if ok {
			progress.CurrentAvailableReplicas = availableReplicas
		}

		if progress.ReplicasChangeStatus == workv1alpha2.ReplicaChangeStatusScalingDown {
			scalingDownClusters = append(scalingDownClusters, cluster)
			if progress.CurrentAvailableReplicas != 0 {
				allScalingDownReplicasZero = false
			}
		}
		progressMap[cluster] = progress
	}

	// If all scaling down clusters have zero replicas, return true
	if len(scalingDownClusters) > 0 && allScalingDownReplicasZero {
		klog.Infof("ensure work retry goroutine for %s/%s/%s all scaling down clusters %v have zero replicas, return true",
			currCluster, namespace, name, scalingDownClusters)
		SyncReplicasProgressMapToCache(goCache, namespace, name, progressMap)
		return true, nil
	}

	// Second pass: check scaling up clusters
	for cluster, progress := range progressMap {
		if cluster == currCluster {
			continue
		}

		if progress.ReplicasChangeStatus == workv1alpha2.ReplicaChangeStatusScalingUp {
			scalingUpClusters = append(scalingUpClusters, cluster)
			isHasScalingUpCluster = true
			// Check if available replicas reached threshold ratio of target replicas (not final min replicas)
			// This ensures we wait for actual scale up completion, not just minimum threshold
			// Condition 1: Threshold ratio can be configured via SCALE_UP_THRESHOLD_RATIO env var (default: 0.5)
			// Condition 2: Current replicas >= BeginAvailableReplicas + 1
			// Either condition can trigger the threshold
			threshold := int(math.Ceil(float64(progress.FinalMinReplicas) * EnvScaleUpThresholdRatio))
			incrementThreshold := progress.BeginAvailableReplicas + 1
			if progress.CurrentAvailableReplicas >= threshold || progress.CurrentAvailableReplicas >= incrementThreshold {
				klog.Infof("ensure work retry goroutine for %s/%s/%s is scaling up, current available replicas: %d, final min replicas: %d, threshold (%.0f%%): %d, increment threshold (begin+1): %d, ensure work retry goroutine will reached scale up threshold",
					currCluster, namespace, name, progress.CurrentAvailableReplicas, progress.FinalMinReplicas, EnvScaleUpThresholdRatio*100, threshold, incrementThreshold)
				return true, nil
			} else {
				klog.Infof("ensure work retry goroutine for %s/%s/%s is scaling up but not reached threshold yet, current: %d, final min replicas: %d, threshold (%.0f%%): %d, increment threshold (begin+1): %d",
					cluster, namespace, name, progress.CurrentAvailableReplicas, progress.FinalMinReplicas, EnvScaleUpThresholdRatio*100, threshold, incrementThreshold)
			}
		}
	}
	SyncReplicasProgressMapToCache(goCache, namespace, name, progressMap)
	if !isHasScalingUpCluster {
		klog.Infof("%s/%s/%s no other cluster is scaling up, return true", currCluster, namespace, name)
		for cluster, progress := range progressMap {
			klog.Infof("%s/%s/%s no other cluster is scaling up, return true, cluster %s progress: %+v", currCluster, namespace, name, cluster, *progress)
		}
		return true, nil
	}
	klog.Infof("%s/%s/%s is scaling up in cluster %v but not reach scale up threshold, return false", currCluster, namespace, name, scalingUpClusters)
	return false, nil
}
