package binding

import (
	"fmt"
	"strings"
	"time"

	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	gocache "github.com/patrickmn/go-cache"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
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

func SyncEndpointProgressMapToCache(goCache *gocache.Cache, namespace, name string, progress map[string]*ExpansionProgress) {
	name = GetEndpointName(name)
	key := fmt.Sprintf("endpoints-progress-%s-%s", namespace, name)
	goCache.Set(key, progress, defaultRsourceBingdingControllerCacheExpiration)
	klog.Infof("Recorded endpoint for workload: %s/%s, progress: %v", namespace, name, progress)
	for cluster, progress := range progress {
		klog.Infof("Recorded endpoint for workload %s/%s, cluster: %s, progress: %v", namespace, name, cluster, progress)
	}
}

func GetEndpointProgressFromCache(goCache *gocache.Cache, namespace, name string) (map[string]*ExpansionProgress, error) {
	name = GetEndpointName(name)
	key := fmt.Sprintf("endpoints-progress-%s-%s", namespace, name)
	it, found := goCache.Get(key)
	if !found {
		return nil, fmt.Errorf("endpoint %s not found", key)
	}
	progress, ok := it.(map[string]*ExpansionProgress)
	if !ok {
		return nil, fmt.Errorf("endpoint %s is not map[string]*ExpansionProgress", key)
	}
	return progress, nil
}

func IsNeedCheckScaleUpThreshold(goCache *gocache.Cache, karmadaSearchCli *SKarmadaSearch, currCluster, namespace, name string) (bool, error) {
	name = GetEndpointName(name)
	progressMap, err := GetEndpointProgressFromCache(goCache, namespace, name)
	if err != nil {
		klog.Warningf("Get is need check scale up threshold failed, failed to get endpoint progress from cache: %w", err)
		return false, fmt.Errorf("failed to get endpoint progress from cache: %w", err)
	}

	for _, progress := range progressMap {
		if progress.ReplicasChangeStatus == workv1alpha2.ReplicaChangeStatusScalingUp {
			return true, nil
		}
	}
	return false, nil
}

func IsOtherReachScaleUpThreshold(goCache *gocache.Cache, karmadaSearchCli *SKarmadaSearch, currCluster, namespace, name string) (bool, error) {
	name = GetEndpointName(name)
	progressMap, err := GetEndpointProgressFromCache(goCache, namespace, name)
	if err != nil {
		return false, fmt.Errorf("failed to get target cluster from cache: %w", err)
	}

	startTime := time.Now()
	latestEndpoints, err := karmadaSearchCli.GetEndpointsFromKarmadaSearch(types.NamespacedName{Namespace: namespace, Name: name})
	if err != nil {
		return false, fmt.Errorf("failed to get endpoints from karmadaSearch: %w", err)
	}
	latestClusterEndpointsMap, err := GetClusterEndpointMap(latestEndpoints)
	if err != nil {
		return false, fmt.Errorf("failed to get cluster endpoint: %w", err)
	}
	klog.Infof("Success get endpoints from karmadaSearch for %s/%s took %v, latestClusterEndpointsMap: %+v", namespace, name, time.Since(startTime), latestClusterEndpointsMap)

	for cluster, progress := range progressMap {
		if cluster == currCluster {
			continue
		}
		endpointsCount, ok := latestClusterEndpointsMap[cluster]
		if !ok {
			continue
		}
		progress.CurrentEndpoints = endpointsCount
		if progress.ReplicasChangeStatus == workv1alpha2.ReplicaChangeStatusScalingUp {
			if progress.CurrentEndpoints > progress.BeginEndpoints {
				klog.Infof("%s/%s cluster %s is scaling up, current endpoints: %d, begin endpoints: %d, reach scale up threshold",
					namespace, name, cluster, progress.CurrentEndpoints, progress.BeginEndpoints)
				return true, nil
			}
			if progress.CurrentEndpoints >= progress.FinMinReplicas {
				klog.Infof("%s/%s cluster %s is scaling up, current endpoints: %d, final min replicas: %d, reach scale up threshold",
					namespace, name, cluster, progress.CurrentEndpoints, progress.FinMinReplicas)
				return true, nil
			}
		}
		progressMap[cluster] = progress
	}
	SyncEndpointProgressMapToCache(goCache, namespace, name, progressMap)
	klog.Infof("no other cluster is scaling up, return true")
	return true, nil
}
