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
	Namespace            string    `json:"namespace"`            // namespace
	Name                 string    `json:"name"`                 // name
	Progress             int       `json:"progress"`             // progress：0-100
	CurrentEndpoints     int       `json:"currentEndpoints"`     // current endpoints length
	BeginEndpoints       int       `json:"beginEndpoints"`       // begin endpoints length
	FinalMinReplicas     int       `json:"finalMinReplicas"`     // final min replicas
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
	for cluster, progress := range progress {
		klog.Infof("SyncEndpointProgressMapToCache for workload %s/%s cluster %s progress: %+v", namespace, name, cluster, *progress)
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
	klog.Infof("GetEndpointProgressFromCache for workload %s/%s success, progress: %+v", namespace, name, progress)
	return progress, nil
}

func IsOtherReachScaleUpThreshold(goCache *gocache.Cache, karmadaSearchCli *SKarmadaSearch, currCluster, namespace, name string) (bool, error) {
	isHasScalingUpCluster := false
	scalingUpClusters := []string{}

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
			scalingUpClusters = append(scalingUpClusters, cluster)
			isHasScalingUpCluster = true
			if progress.CurrentEndpoints > int(math.Ceil(float64(progress.FinalMinReplicas)*0.25)) {
				klog.Infof("%s/%s/%s is scaling up, current endpoints: %d, final min replicas: %d, reach scale up threshold",
					currCluster, namespace, name, progress.CurrentEndpoints, progress.FinalMinReplicas)
				return true, nil
			}
		}
		progressMap[cluster] = progress
	}
	SyncEndpointProgressMapToCache(goCache, namespace, name, progressMap)
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
