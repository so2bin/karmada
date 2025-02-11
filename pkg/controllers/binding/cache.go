package binding

import (
	"fmt"
	"strings"
	"time"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	gocache "github.com/patrickmn/go-cache"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const defaultRsourceBingdingControllerCacheExpiration time.Duration = time.Duration(10 * time.Minute)

type ExpansionProgress struct {
	Namespace            string    `json:"namespace"`            // namespace
	Name                 string    `json:"name"`                 // name
	Progress             int       `json:"progress"`             // progress：0-100
	CurrentEndpoints     int       `json:"currentEndpoints"`     // current endpoints length
	BeginEndpoints       int       `json:"beginEndpoints"`       // begin endpoints length
	FinMinReplicas       int       `json:"finalMinReplicas"`     // final min replicas
	ReplicasChangeStatus string    `json:"replicasChangeStatus"` // replicas change status
	LastUpdate           time.Time `json:"lastUpdate"`           // last update time
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

func SyncEndpointProgressToCache(goCache *gocache.Cache, cluster, namespace, name string, progress *ExpansionProgress) {
	name = GetEndpointName(name)
	key := fmt.Sprintf("endpoints-progress-%s-%s-%s", cluster, namespace, name)
	goCache.Set(key, progress, defaultRsourceBingdingControllerCacheExpiration)
}

func GetEndpointProgressFromCache(goCache *gocache.Cache, cluster, namespace, name string) (*ExpansionProgress, error) {
	name = GetEndpointName(name)
	key := fmt.Sprintf("endpoints-progress-%s-%s-%s", cluster, namespace, name)
	it, found := goCache.Get(key)
	if !found {
		return nil, fmt.Errorf("endpoint %s not found", key)
	}
	progress, ok := it.(*ExpansionProgress)
	if !ok {
		return nil, fmt.Errorf("endpoint %s is not *ExpansionProgress", key)
	}
	return progress, nil
}

func IsOtherReachScaleUpThreshold(goCache *gocache.Cache, client client.Client, currCluster, namespace, name string) (bool, error) {
	name = GetEndpointName(name)
	targetCluster, err := GetTargetClusterFromCache(goCache, namespace, name)
	if err != nil {
		return false, fmt.Errorf("failed to get target cluster from cache: %w", err)
	}

	var containScaleUpFlag bool
	for _, cluster := range targetCluster {
		if cluster.Name == currCluster {
			continue
		}

		// Get current endpoints count
		endpointsCount, err := getEndpointsFromClient(client, cluster.Name, namespace, name)
		if err != nil {
			klog.Errorf("Failed to get endpoint for cluster %s, workload: %s/%s, error: %v",
				cluster.Name, namespace, name, err)
			continue
		}

		// Get and update progress
		progress, err := GetEndpointProgressFromCache(goCache, cluster.Name, namespace, name)
		if err != nil {
			return false, fmt.Errorf("failed to get progress for cluster %s: %w", cluster.Name, err)
		}

		progress.CurrentEndpoints = endpointsCount
		SyncEndpointProgressToCache(goCache, cluster.Name, namespace, name, progress)

		klog.Infof("Cluster %s/%s/%s status: %s, endpoints: %d/%d/%d (current/begin/minReplicas)",
			cluster.Name, namespace, name, cluster.ReplicaChangeStatus,
			progress.CurrentEndpoints, progress.BeginEndpoints, progress.FinMinReplicas)

		if cluster.ReplicaChangeStatus == workv1alpha2.ReplicaChangeStatusScalingUp {
			containScaleUpFlag = true
			if progress.CurrentEndpoints > progress.BeginEndpoints {
				klog.Infof("cluster %s is scaling up, current endpoints: %d, begin endpoints: %d, reach scale up threshold",
					cluster.Name, progress.CurrentEndpoints, progress.BeginEndpoints)
				return true, nil
			}
			if progress.CurrentEndpoints >= progress.FinMinReplicas {
				klog.Infof("cluster %s is scaling up, current endpoints: %d, final min replicas: %d, reach scale up threshold",
					cluster.Name, progress.CurrentEndpoints, progress.FinMinReplicas)
				return true, nil
			}
		}
	}
	if containScaleUpFlag {
		return false, nil
	}
	klog.Infof("no other cluster is scaling up, return true")
	return true, nil
}
