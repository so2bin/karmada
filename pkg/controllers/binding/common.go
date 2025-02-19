/*
Copyright 2021 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package binding

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configv1alpha1 "github.com/karmada-io/karmada/pkg/apis/config/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/names"
	"github.com/karmada-io/karmada/pkg/util/overridemanager"
	gocache "github.com/patrickmn/go-cache"
	k8scorev1 "k8s.io/api/core/v1"
	k8syaml "sigs.k8s.io/yaml"
)

// ensureWork ensure Work to be created or updated.
func ensureWork(
	cache *gocache.Cache, client client.Client, resourceInterpreter resourceinterpreter.ResourceInterpreter, workload *unstructured.Unstructured,
	overrideManager overridemanager.OverrideManager, binding metav1.Object, scope apiextensionsv1.ResourceScope,
) error {
	var targetClusters []workv1alpha2.TargetCluster
	var placement *policyv1alpha1.Placement
	var requiredByBindingSnapshot []workv1alpha2.BindingSnapshot
	var replicas int32
	var conflictResolutionInBinding policyv1alpha1.ConflictResolution
	switch scope {
	case apiextensionsv1.NamespaceScoped:
		bindingObj := binding.(*workv1alpha2.ResourceBinding)
		targetClusters = bindingObj.Spec.Clusters
		requiredByBindingSnapshot = bindingObj.Spec.RequiredBy
		placement = bindingObj.Spec.Placement
		replicas = bindingObj.Spec.Replicas
		conflictResolutionInBinding = bindingObj.Spec.ConflictResolution
	case apiextensionsv1.ClusterScoped:
		bindingObj := binding.(*workv1alpha2.ClusterResourceBinding)
		targetClusters = bindingObj.Spec.Clusters
		requiredByBindingSnapshot = bindingObj.Spec.RequiredBy
		placement = bindingObj.Spec.Placement
		replicas = bindingObj.Spec.Replicas
		conflictResolutionInBinding = bindingObj.Spec.ConflictResolution
	}

	targetClusters = mergeTargetClusters(targetClusters, requiredByBindingSnapshot)

	var jobCompletions []workv1alpha2.TargetCluster
	var err error
	if workload.GetKind() == util.JobKind {
		jobCompletions, err = divideReplicasByJobCompletions(workload, targetClusters)
		if err != nil {
			return err
		}
	}

	// Create a wait group to track goroutines
	var wg sync.WaitGroup
	// Create error channel to collect errors from goroutines
	errChan := make(chan error, len(targetClusters))
	for i := range targetClusters {
		targetCluster := targetClusters[i]
		needDelayedScaling := needDelayedScaling(targetClusters, targetCluster)

		if isEnableDelayedScalingNs(workload.GetNamespace()) && isAtmsNodeCmName(workload.GetName()) &&
			needDelayedScaling && cache != nil {
			klog.Infof("%s/%s is scaling down in cluster %s and other clusters is scaling up, delay to process ensureWork",
				workload.GetNamespace(), workload.GetName(), targetCluster.Name)
			wg.Add(1)

			go func(targetCluster workv1alpha2.TargetCluster, i int) {
				defer wg.Done()
				if err := processEnsureWorkWithRetry(cache, client, resourceInterpreter, workload,
					overrideManager, binding, scope, targetCluster, placement, replicas,
					jobCompletions, i, conflictResolutionInBinding); err != nil {

					klog.Errorf("Error processing target cluster %s: %v", targetCluster.Name, err)
					errChan <- err
				}
			}(targetCluster, i)
		} else {
			klog.Infof("%s/%s is scaling up in cluster %s, going to process ensureWork", workload.GetNamespace(), workload.GetName(), targetCluster.Name)
			if err := processEnsureWork(client, resourceInterpreter, workload, overrideManager, binding, scope, targetCluster, placement, replicas,
				jobCompletions, i, conflictResolutionInBinding); err != nil {
				return err
			}
		}
	}
	// Create a channel to signal WaitGroup completion
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for either completion or timeout
	select {
	case <-done:
		// Check if there were any errors
		close(errChan)
		for err := range errChan {
			if err != nil {
				return fmt.Errorf("error during async processing: %v", err)
			}
		}
		return nil
	case <-time.After(time.Duration(EnvDelayedScalingTimeoutSecond) * time.Second):
		return fmt.Errorf("timeout waiting for delayed scaling operations to complete")
	}
}

func processEnsureWorkWithRetry(cache *gocache.Cache, client client.Client, resourceInterpreter resourceinterpreter.ResourceInterpreter,
	workload *unstructured.Unstructured, overrideManager overridemanager.OverrideManager, binding metav1.Object, scope apiextensionsv1.ResourceScope,
	targetCluster workv1alpha2.TargetCluster, placement *policyv1alpha1.Placement, replicas int32,
	jobCompletions []workv1alpha2.TargetCluster, idx int, conflictResolutionInBinding policyv1alpha1.ConflictResolution) error {

	sleepDuration := time.Duration(EnvDelayedScalingSleepDurationSecond) * time.Second
	maxAttempts := int(EnvDelayedScalingTimeoutSecond / int(sleepDuration.Seconds()))

	workloadKey := fmt.Sprintf("%s/%s", workload.GetNamespace(), workload.GetName())

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check if other clusters have reached scale up threshold
		hasReachedThreshold, err := IsOtherReachScaleUpThreshold(cache, client, targetCluster.Name, workload.GetNamespace(), workload.GetName())
		if err != nil {
			klog.Warningf("Failed to check scale up threshold for %s in cluster %s (attempt %d/%d): %v",
				workloadKey, targetCluster.Name, attempt, maxAttempts, err)
			// Continue with retry even if check failed
		}

		klog.Infof("Scale up threshold check for %s in cluster %s (attempt %d/%d): reached=%v",
			workloadKey, targetCluster.Name, attempt, maxAttempts, hasReachedThreshold)

		if hasReachedThreshold {
			klog.Infof("Scale up threshold reached for %s in cluster %s, process ensureWork",
				workloadKey, targetCluster.Name)
			return processEnsureWork(client, resourceInterpreter, workload, overrideManager, binding, scope,
				targetCluster, placement, replicas, jobCompletions, idx, conflictResolutionInBinding)
		}

		if attempt < maxAttempts {
			time.Sleep(sleepDuration)
		}
	}

	klog.Warningf("Timeout waiting for scale up threshold, process ensureWork for %s in cluster %s",
		workloadKey, targetCluster.Name)

	// Fallback: process work creation even if threshold was never reached
	klog.Infof("reached the scale up threshold, going to process ensureWork for %s/%s in cluster %s", workload.GetNamespace(), workload.GetName(), targetCluster.Name)
	return processEnsureWork(client, resourceInterpreter, workload, overrideManager, binding, scope,
		targetCluster, placement, replicas, jobCompletions, idx, conflictResolutionInBinding)
}

func processEnsureWork(
	c client.Client, resourceInterpreter resourceinterpreter.ResourceInterpreter, workload *unstructured.Unstructured,
	overrideManager overridemanager.OverrideManager, binding metav1.Object, scope apiextensionsv1.ResourceScope,
	targetCluster workv1alpha2.TargetCluster, placement *policyv1alpha1.Placement, replicas int32,
	jobCompletions []workv1alpha2.TargetCluster, idx int, conflictResolutionInBinding policyv1alpha1.ConflictResolution,
) error {
	var err error
	clonedWorkload := workload.DeepCopy()

	workNamespace := names.GenerateExecutionSpaceName(targetCluster.Name)
	// add cluster name to the workload's annotation
	util.MergeAnnotation(clonedWorkload, util.ClusterNameAnnotation, targetCluster.Name)

	// If and only if the resource template has replicas, and the replica scheduling policy is divided,
	// we need to revise replicas.
	if needReviseReplicas(replicas, placement) {
		if resourceInterpreter.HookEnabled(clonedWorkload.GroupVersionKind(), configv1alpha1.InterpreterOperationReviseReplica) {
			clonedWorkload, err = resourceInterpreter.ReviseReplica(clonedWorkload, int64(targetCluster.Replicas))
			if err != nil {
				klog.Errorf("Failed to revise replica for %s/%s/%s in cluster %s, err is: %v",
					workload.GetKind(), workload.GetNamespace(), workload.GetName(), targetCluster.Name, err)
				return err
			}
		}

		// Set allocated completions for Job only when the '.spec.completions' field not omitted from resource template.
		// For jobs running with a 'work queue' usually leaves '.spec.completions' unset, in that case we skip
		// setting this field as well.
		// Refer to: https://kubernetes.io/docs/concepts/workloads/controllers/job/#parallel-jobs.
		if len(jobCompletions) > 0 {
			if err = helper.ApplyReplica(clonedWorkload, int64(jobCompletions[idx].Replicas), util.CompletionsField); err != nil {
				klog.Errorf("Failed to apply Completions for %s/%s/%s in cluster %s, err is: %v",
					clonedWorkload.GetKind(), clonedWorkload.GetNamespace(), clonedWorkload.GetName(), targetCluster.Name, err)
				return err
			}
		}
	}

	// We should call ApplyOverridePolicies last, as override rules have the highest priority
	cops, ops, err := overrideManager.ApplyOverridePolicies(clonedWorkload, targetCluster.Name)
	if err != nil {
		klog.Errorf("Failed to apply overrides for %s/%s/%s, err is: %v", clonedWorkload.GetKind(), clonedWorkload.GetNamespace(), clonedWorkload.GetName(), err)
		return err
	}
	workLabel := mergeLabel(clonedWorkload, binding, scope)

	annotations := mergeAnnotations(clonedWorkload, binding, scope)
	annotations = mergeConflictResolution(clonedWorkload, conflictResolutionInBinding, annotations)
	annotations, err = RecordAppliedOverrides(cops, ops, annotations)
	if err != nil {
		klog.Errorf("Failed to record appliedOverrides, Error: %v", err)
		return err
	}

	workMeta := metav1.ObjectMeta{
		Name:        names.GenerateWorkName(clonedWorkload.GetKind(), clonedWorkload.GetName(), clonedWorkload.GetNamespace()),
		Namespace:   workNamespace,
		Finalizers:  []string{util.ExecutionControllerFinalizer},
		Labels:      workLabel,
		Annotations: annotations,
	}

	if err = helper.CreateOrUpdateWork(c, workMeta, clonedWorkload); err != nil {
		return err
	}
	return nil
}

func mergeTargetClusters(targetClusters []workv1alpha2.TargetCluster, requiredByBindingSnapshot []workv1alpha2.BindingSnapshot) []workv1alpha2.TargetCluster {
	if len(requiredByBindingSnapshot) == 0 {
		return targetClusters
	}

	scheduledClusterNames := util.ConvertToClusterNames(targetClusters)

	for _, requiredByBinding := range requiredByBindingSnapshot {
		for _, targetCluster := range requiredByBinding.Clusters {
			if !scheduledClusterNames.Has(targetCluster.Name) {
				scheduledClusterNames.Insert(targetCluster.Name)
				targetClusters = append(targetClusters, targetCluster)
			}
		}
	}

	return targetClusters
}

func mergeLabel(workload *unstructured.Unstructured, binding metav1.Object, scope apiextensionsv1.ResourceScope) map[string]string {
	var workLabel = make(map[string]string)
	if scope == apiextensionsv1.NamespaceScoped {
		bindingID := util.GetLabelValue(binding.GetLabels(), workv1alpha2.ResourceBindingPermanentIDLabel)
		util.MergeLabel(workload, workv1alpha2.ResourceBindingPermanentIDLabel, bindingID)
		workLabel[workv1alpha2.ResourceBindingPermanentIDLabel] = bindingID
	} else {
		bindingID := util.GetLabelValue(binding.GetLabels(), workv1alpha2.ClusterResourceBindingPermanentIDLabel)
		util.MergeLabel(workload, workv1alpha2.ClusterResourceBindingPermanentIDLabel, bindingID)
		workLabel[workv1alpha2.ClusterResourceBindingPermanentIDLabel] = bindingID
	}
	return workLabel
}

func mergeAnnotations(workload *unstructured.Unstructured, binding metav1.Object, scope apiextensionsv1.ResourceScope) map[string]string {
	annotations := make(map[string]string)

	if scope == apiextensionsv1.NamespaceScoped {
		util.MergeAnnotation(workload, workv1alpha2.ResourceBindingNamespaceAnnotationKey, binding.GetNamespace())
		util.MergeAnnotation(workload, workv1alpha2.ResourceBindingNameAnnotationKey, binding.GetName())
		annotations[workv1alpha2.ResourceBindingNamespaceAnnotationKey] = binding.GetNamespace()
		annotations[workv1alpha2.ResourceBindingNameAnnotationKey] = binding.GetName()
	} else {
		util.MergeAnnotation(workload, workv1alpha2.ClusterResourceBindingAnnotationKey, binding.GetName())
		annotations[workv1alpha2.ClusterResourceBindingAnnotationKey] = binding.GetName()
	}

	return annotations
}

// RecordAppliedOverrides record applied (cluster) overrides to annotations
func RecordAppliedOverrides(cops *overridemanager.AppliedOverrides, ops *overridemanager.AppliedOverrides,
	annotations map[string]string) (map[string]string, error) {
	if annotations == nil {
		annotations = make(map[string]string)
	}

	if cops != nil {
		appliedBytes, err := cops.MarshalJSON()
		if err != nil {
			return nil, err
		}
		if appliedBytes != nil {
			annotations[util.AppliedClusterOverrides] = string(appliedBytes)
		}
	}

	if ops != nil {
		appliedBytes, err := ops.MarshalJSON()
		if err != nil {
			return nil, err
		}
		if appliedBytes != nil {
			annotations[util.AppliedOverrides] = string(appliedBytes)
		}
	}

	return annotations, nil
}

// mergeConflictResolution determine the conflictResolution annotation of Work: preferentially inherit from RT, then RB
func mergeConflictResolution(workload *unstructured.Unstructured, conflictResolutionInBinding policyv1alpha1.ConflictResolution,
	annotations map[string]string) map[string]string {
	// conflictResolutionInRT refer to the annotation in ResourceTemplate
	conflictResolutionInRT := util.GetAnnotationValue(workload.GetAnnotations(), workv1alpha2.ResourceConflictResolutionAnnotation)

	// the final conflictResolution annotation value of Work inherit from RT preferentially
	// so if conflictResolution annotation is defined in RT already, just copy the value and return
	if conflictResolutionInRT == workv1alpha2.ResourceConflictResolutionOverwrite || conflictResolutionInRT == workv1alpha2.ResourceConflictResolutionAbort {
		annotations[workv1alpha2.ResourceConflictResolutionAnnotation] = conflictResolutionInRT
		return annotations
	} else if conflictResolutionInRT != "" {
		// ignore its value and add logs if conflictResolutionInRT is neither abort nor overwrite.
		klog.Warningf("Ignore the invalid conflict-resolution annotation in ResourceTemplate %s/%s/%s: %s",
			workload.GetKind(), workload.GetNamespace(), workload.GetName(), conflictResolutionInRT)
	}

	if conflictResolutionInBinding == policyv1alpha1.ConflictOverwrite {
		annotations[workv1alpha2.ResourceConflictResolutionAnnotation] = workv1alpha2.ResourceConflictResolutionOverwrite
		return annotations
	}

	annotations[workv1alpha2.ResourceConflictResolutionAnnotation] = workv1alpha2.ResourceConflictResolutionAbort
	return annotations
}

func divideReplicasByJobCompletions(workload *unstructured.Unstructured, clusters []workv1alpha2.TargetCluster) ([]workv1alpha2.TargetCluster, error) {
	var targetClusters []workv1alpha2.TargetCluster
	completions, found, err := unstructured.NestedInt64(workload.Object, util.SpecField, util.CompletionsField)
	if err != nil {
		return nil, err
	}

	if found {
		targetClusters = helper.SpreadReplicasByTargetClusters(int32(completions), clusters, nil)
	}

	return targetClusters, nil
}

func needReviseReplicas(replicas int32, placement *policyv1alpha1.Placement) bool {
	return replicas > 0 && placement != nil && placement.ReplicaSchedulingType() == policyv1alpha1.ReplicaSchedulingTypeDivided
}

func isEnableDelayedScalingNs(ns string) bool {
	nss := strings.Split(EnvEnableDelayedScalingNamespace, ",")
	for _, n := range nss {
		if n == ns {
			return true
		}
	}
	return false
}

func isAtmsNodeCmName(name string) bool {
	return strings.HasPrefix(name, ATMSNodeCmPrefix)
}

func needDelayedScaling(targetClusters []workv1alpha2.TargetCluster, currentCluster workv1alpha2.TargetCluster) bool {
	containScaleUpFlag := false
	for _, cluster := range targetClusters {
		if cluster.ReplicaChangeStatus == workv1alpha2.ReplicaChangeStatusScalingUp {
			containScaleUpFlag = true
			break
		}
	}
	if containScaleUpFlag {
		return currentCluster.ReplicaChangeStatus == workv1alpha2.ReplicaChangeStatusScalingDown
	}
	return false
}

// KNodeScale app node scaleobject
type KNodeScale struct {
	CooldownPeriod   *int32 `json:"cooldownPeriod,omitempty" bson:"cooldownPeriod,omitempty"`
	IsUseFixReplicas bool   `json:"isUseFixReplicas" yaml:"isUseFixReplicas"`
	FixedReplicas    int    `json:"fixedReplicas" yaml:"fixedReplicas"`
	IdleReplicas     *int32 `json:"idleReplicas,omitempty" yaml:"idleReplicas,omitempty"`
	MinReplicas      int    `json:"minReplicas" yaml:"minReplicas"`
	MaxReplicas      int    `json:"maxReplicas" yaml:"maxReplicas"`
}
type KAppNodeCmData struct {
	Scale *KNodeScale `json:"scale" yaml:"scale"`
}

func getMinReplicasFromResourceInterpreter(resourceInterpreter resourceinterpreter.ResourceInterpreter, workload *unstructured.Unstructured) (int32, error) {
	minReplicas, _, err := resourceInterpreter.GetMinReplicas(workload)
	if err != nil {
		klog.Errorf("Failed to get minReplicas for workload %s/%s, error: %v", workload.GetNamespace(), workload.GetName(), err)
		return 0, err
	}
	return minReplicas, nil
}

func getMinReplicasFromResourceTemplate(workload *unstructured.Unstructured) (int32, error) {
	klog.Infof("Processing workload for %s/%s: %+v", workload.GetNamespace(), workload.GetName(), workload)

	workloadObj := workload.Object
	if workloadObj == nil {
		klog.Infof("Workload object is nil for %s/%s", workload.GetNamespace(), workload.GetName())
		return 0, nil
	}

	nodeCmData, exists := workloadObj["data"]
	if !exists || nodeCmData == nil {
		klog.Infof("No data field found in workload object for %s/%s", workload.GetNamespace(), workload.GetName())
		return 0, nil
	}

	nodeCmDataMap, ok := nodeCmData.(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("node-cm data is not map[string]interface{} type, actual type: %T, value: %v", nodeCmData, nodeCmData)
	}

	appYaml, exists := nodeCmDataMap["app-yaml"]
	if !exists || appYaml == nil {
		klog.Infof("No app-yaml field found in node-cm data for %s/%s", workload.GetNamespace(), workload.GetName())
		return 0, nil
	}

	appYamlStr, ok := appYaml.(string)
	if !ok {
		return 0, fmt.Errorf("app-yaml is not string type, actual type: %T, value: %v", appYaml, appYaml)
	}

	appNodeCmData := &KAppNodeCmData{}
	if err := k8syaml.Unmarshal([]byte(appYamlStr), appNodeCmData); err != nil {
		return 0, fmt.Errorf("failed to unmarshal node-cm configmap: %v", err)
	}

	if appNodeCmData.Scale != nil {
		klog.Infof("Scale configuration found for %s/%s: %+v", workload.GetNamespace(), workload.GetName(), appNodeCmData.Scale)
		return int32(appNodeCmData.Scale.MinReplicas), nil
	}

	return 0, nil
}

func getMinReplicas(resourceInterpreter resourceinterpreter.ResourceInterpreter, workload *unstructured.Unstructured) (int, error) {
	// 从resource interpreter中获取minReplicas
	minReplicas, err := getMinReplicasFromResourceInterpreter(resourceInterpreter, workload)
	if err == nil && minReplicas != 0 {
		klog.Infof("Get minReplicas from resource interpreter for workload %s/%s: %d", workload.GetNamespace(), workload.GetName(), minReplicas)
		return int(minReplicas), nil
	}
	// 从resource template中获取minReplicas
	minReplicas, err = getMinReplicasFromResourceTemplate(workload)
	if err == nil {
		klog.Infof("Get minReplicas from resource template for workload %s/%s: %d", workload.GetNamespace(), workload.GetName(), minReplicas)
		return int(minReplicas), nil
	}

	klog.Errorf("Failed to get minReplicas for workload %s/%s, error: %v", workload.GetNamespace(), workload.GetName(), err)
	return 0, err
}

func recordBeginEndpoint(gocache *gocache.Cache, client client.Client, resourceInterpreter resourceinterpreter.ResourceInterpreter, workload *unstructured.Unstructured,
	binding metav1.Object, scope apiextensionsv1.ResourceScope) error {

	var targetClusters []workv1alpha2.TargetCluster
	var requiredByBindingSnapshot []workv1alpha2.BindingSnapshot
	switch scope {
	case apiextensionsv1.NamespaceScoped:
		bindingObj := binding.(*workv1alpha2.ResourceBinding)
		targetClusters = bindingObj.Spec.Clusters
		requiredByBindingSnapshot = bindingObj.Spec.RequiredBy
	case apiextensionsv1.ClusterScoped:
		bindingObj := binding.(*workv1alpha2.ClusterResourceBinding)
		targetClusters = bindingObj.Spec.Clusters
		requiredByBindingSnapshot = bindingObj.Spec.RequiredBy
	}

	targetClusters = mergeTargetClusters(targetClusters, requiredByBindingSnapshot)

	SyncTargetClusterToCache(gocache, workload.GetNamespace(), workload.GetName(), targetClusters)

	replicasSum := 0
	for i := range targetClusters {
		targetCluster := targetClusters[i]
		replicasSum += int(targetCluster.Replicas)
	}
	minReplicas, err := getMinReplicas(resourceInterpreter, workload)
	if err != nil {
		klog.Errorf("Failed to get minReplicas for workload %s/%s, error: %v", workload.GetNamespace(), workload.GetName(), err)
		return err
	}
	for i := range targetClusters {
		targetCluster := targetClusters[i]
		clusterName := targetCluster.Name
		// Skip recording endpoint for the cluster that is scaling down
		if targetCluster.ReplicaChangeStatus == workv1alpha2.ReplicaChangeStatusScalingDown {
			continue
		}
		endpointsSubsetAddressLength, err := getEndpointsFromClient(client, clusterName, workload.GetNamespace(), workload.GetName())
		if err != nil {
			klog.Errorf("Failed to get endpoint for cluster %s, workload: %s/%s, error: %v", clusterName, workload.GetNamespace(), workload.GetName(), err)
			continue
		}
		endpointName := GetEndpointName(workload.GetName())
		progress := &ExpansionProgress{
			Namespace:        workload.GetNamespace(),
			Name:             endpointName,
			CurrentEndpoints: endpointsSubsetAddressLength,
			BeginEndpoints:   endpointsSubsetAddressLength,
			LastUpdate:       time.Now(),
		}

		if replicasSum > 0 {
			progress.FinMinReplicas = minReplicas * int(targetCluster.Replicas) / replicasSum
		}
		SyncEndpointProgressToCache(gocache, clusterName, workload.GetNamespace(), workload.GetName(), progress)
		klog.Infof("Recorded endpoint for cluster %s, workload: %s/%s, progress: %v", clusterName, workload.GetNamespace(), workload.GetName(), progress)
	}
	return nil
}

func getEndpointsFromClient(client client.Client, clusterName, ns, name string) (int, error) {
	name = GetEndpointName(name)
	clusterClient, err := util.NewClusterClientSet(clusterName, client, &util.ClientOption{})
	if err != nil {
		klog.Errorf("Failed to create a ClusterClient for the given member cluster: %v, err is : %v", clusterName, err)
		// return c.setStatusCollectionFailedCondition(cluster, currentClusterStatus, fmt.Sprintf("failed to create a ClusterClient: %v", err))
		return 0, err
	}

	var endpointsSubsetAddressLength int
	endpoint, err := getEndpointFromDiscoveryClient(clusterClient, clusterName, ns, name)
	if err != nil {
		klog.Errorf("Failed to get endpoint from DiscoveryClient for cluster %s, error: %v", clusterName, err)
		return 0, err
	} else {
		endpointsSubsetAddressLength = 0
		if len(endpoint.Subsets) > 0 {
			for _, subset := range endpoint.Subsets {
				endpointsSubsetAddressLength += len(subset.Addresses)
			}
		}
		klog.Infof("recordEndpoint for cluster %s, workload: %s/%s endpoint Subsets len: %d, endpoint Subsets: %v",
			clusterName, ns, name, endpointsSubsetAddressLength, endpoint.Subsets)
	}
	return endpointsSubsetAddressLength, nil
}

func getEndpointFromDiscoveryClient(clusterClient *util.ClusterClient, cluster, ns, name string) (*k8scorev1.Endpoints, error) {
	if clusterClient == nil || clusterClient.KubeClient == nil || clusterClient.KubeClient.DiscoveryClient == nil {
		return nil, fmt.Errorf("invalid cluster client configuration")
	}

	endpointName := GetEndpointName(name)
	endpoint := &k8scorev1.Endpoints{}

	err := clusterClient.KubeClient.DiscoveryClient.RESTClient().
		Get().
		AbsPath("api/v1/namespaces/" + ns + "/endpoints/" + endpointName).
		Do(context.Background()).
		Into(endpoint)

	if err != nil {
		return nil, fmt.Errorf("failed to get endpoints from cluster %s for %s/%s: %v", cluster, ns, name, err)
	}

	return endpoint, nil
}
