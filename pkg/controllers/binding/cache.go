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
	Namespace                string    `json:"namespace"`                // namespace
	Name                     string    `json:"name"`                     // name
	Progress                 int       `json:"progress"`                 // progress：0-100
	CurrentAvailableReplicas int       `json:"currentAvailableReplicas"` // current available replicas
	BeginAvailableReplicas   int       `json:"beginAvailableReplicas"`   // begin available replicas
	TargetReplicas           int       `json:"targetReplicas"`           // target replicas for this cluster
	FinalMinReplicas         int       `json:"finalMinReplicas"`         // final min replicas
	ReplicasChangeStatus     string    `json:"replicasChangeStatus"`     // replicas change status
	LastUpdate               time.Time `json:"lastUpdate"`               // last update time
}

const ATMSNodeCmPrefix string = "atms-node-conf-"

func GetEndpointName(name string) string {
	if strings.HasPrefix(name, ATMSNodeCmPrefix) {
		name = strings.Replace(name, ATMSNodeCmPrefix, "", 1)
	}
	return name
}

func SyncTargetClusterToCache(goCache *gocache.Cache, namespace, name string, targetClusters []workv1alpha2.TargetCluster) {
	name = GetEndpointName(name)
	key := fmt.Sprintf("rb-clusters-%s-%s", namespace, name)
	goCache.Set(key, targetClusters, gocache.NoExpiration)
}

func GetTargetClusterFromCache(goCache *gocache.Cache, namespace, name string) ([]workv1alpha2.TargetCluster, error) {
	name = GetEndpointName(name)
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
	name = GetEndpointName(name)
	key := fmt.Sprintf("replicas-progress-%s-%s", namespace, name)
	goCache.Set(key, progress, defaultRsourceBingdingControllerCacheExpiration)
	var progressStatus string
	for cluster, progress := range progress {
		progressStatus += fmt.Sprintf("\ncluster %s progress: %+v; ", cluster, *progress)
	}
	klog.Infof("Sync replicas progress map to go cache for workload %s/%s: %s", namespace, name, progressStatus)
}

func GetReplicasProgressFromCache(goCache *gocache.Cache, namespace, name string) (map[string]*ExpansionProgress, error) {
	name = GetEndpointName(name)
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

	name = GetEndpointName(name)
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

	for cluster, progress := range progressMap {
		if cluster == currCluster {
			// if current available replicas is 0, return true
			if progress.CurrentAvailableReplicas == 0 || progress.BeginAvailableReplicas == 0 {
				klog.Infof("%s/%s/%s current available replicas is 0, return true, progress: %+v", cluster, namespace, name, *progress)
				return true, nil
			}
			continue
		}
		availableReplicas, ok := latestClusterAvailableReplicasMap[cluster]
		if !ok {
			continue
		}
		progress.CurrentAvailableReplicas = availableReplicas
		if progress.ReplicasChangeStatus == workv1alpha2.ReplicaChangeStatusScalingUp {
			scalingUpClusters = append(scalingUpClusters, cluster)
			isHasScalingUpCluster = true
			// Check if available replicas reached threshold ratio of target replicas (not final min replicas)
			// This ensures we wait for actual scale up completion, not just minimum threshold
			// Condition 1: Threshold ratio can be configured via SCALE_UP_THRESHOLD_RATIO env var (default: 0.5)
			// Condition 2: Current replicas >= BeginAvailableReplicas + 1
			// Either condition can trigger the threshold
			threshold := int(math.Ceil(float64(progress.TargetReplicas) * EnvScaleUpThresholdRatio))
			incrementThreshold := progress.BeginAvailableReplicas + 1
			if progress.CurrentAvailableReplicas >= threshold || progress.CurrentAvailableReplicas >= incrementThreshold {
				klog.Infof("%s/%s/%s is scaling up, current available replicas: %d, target replicas: %d, threshold (%.0f%%): %d, increment threshold (begin+1): %d, ensure work retry goroutine will reached scale up threshold",
					currCluster, namespace, name, progress.CurrentAvailableReplicas, progress.TargetReplicas, EnvScaleUpThresholdRatio*100, threshold, incrementThreshold)
				return true, nil
			} else {
				klog.Infof("%s/%s/%s is scaling up but not reached threshold yet, current: %d, target: %d, threshold (%.0f%%): %d, increment threshold (begin+1): %d",
					cluster, namespace, name, progress.CurrentAvailableReplicas, progress.TargetReplicas, EnvScaleUpThresholdRatio*100, threshold, incrementThreshold)
			}
		}
		progressMap[cluster] = progress
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
