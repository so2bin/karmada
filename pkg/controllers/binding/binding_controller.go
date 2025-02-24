/*
Copyright 2020 The Karmada Authors.

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
	"encoding/json"
	"fmt"

	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/events"
	"github.com/karmada-io/karmada/pkg/metrics"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter"
	"github.com/karmada-io/karmada/pkg/sharedcli/ratelimiterflag"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/overridemanager"
	gocache "github.com/patrickmn/go-cache"
)

// ControllerName is the controller name that will be used when reporting events.
const ControllerName = "binding-controller"

// ResourceBindingController is to sync ResourceBinding.
type ResourceBindingController struct {
	client.Client                                               // used to operate ClusterResourceBinding resources.
	DynamicClient   dynamic.Interface                           // used to fetch arbitrary resources from api server.
	InformerManager genericmanager.SingleClusterInformerManager // used to fetch arbitrary resources from cache.

	MulticlusterInformerManager genericmanager.MultiClusterInformerManager
	EventRecorder               record.EventRecorder
	RESTMapper                  meta.RESTMapper
	OverrideManager             overridemanager.OverrideManager
	ResourceInterpreter         resourceinterpreter.ResourceInterpreter
	RateLimiterOptions          ratelimiterflag.Options

	KarmadaSearchCli *SKarmadaSearch // used to get endpoints from karmada search
	GoCache          *gocache.Cache
}

// Reconcile performs a full reconciliation for the object referred to by the Request.
// The Controller will requeue the Request to be processed again if an error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (c *ResourceBindingController) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	klog.V(4).Infof("Reconciling ResourceBinding %s.", req.NamespacedName.String())

	binding := &workv1alpha2.ResourceBinding{}
	if err := c.Client.Get(ctx, req.NamespacedName, binding); err != nil {
		// The resource no longer exist, in which case we stop processing.
		if apierrors.IsNotFound(err) {
			return controllerruntime.Result{}, nil
		}

		return controllerruntime.Result{}, err
	}

	if !binding.DeletionTimestamp.IsZero() {
		klog.V(4).Infof("Begin to delete works owned by binding(%s).", req.NamespacedName.String())
		if err := helper.DeleteWorks(c.Client, req.Namespace, req.Name, binding.Labels[workv1alpha2.ResourceBindingPermanentIDLabel]); err != nil {
			klog.Errorf("Failed to delete works related to %s/%s: %v", binding.GetNamespace(), binding.GetName(), err)
			return controllerruntime.Result{}, err
		}
		return c.removeFinalizer(binding)
	}

	return c.syncBinding(binding)
}

// removeFinalizer removes finalizer from the given ResourceBinding
func (c *ResourceBindingController) removeFinalizer(rb *workv1alpha2.ResourceBinding) (controllerruntime.Result, error) {
	if !controllerutil.ContainsFinalizer(rb, util.BindingControllerFinalizer) {
		return controllerruntime.Result{}, nil
	}

	controllerutil.RemoveFinalizer(rb, util.BindingControllerFinalizer)
	err := c.Client.Update(context.TODO(), rb)
	if err != nil {
		return controllerruntime.Result{}, err
	}
	return controllerruntime.Result{}, nil
}

// syncBinding will sync resourceBinding to Works.
func (c *ResourceBindingController) syncBinding(binding *workv1alpha2.ResourceBinding) (controllerruntime.Result, error) {
	if err := c.removeOrphanWorks(binding); err != nil {
		return controllerruntime.Result{}, err
	}

	workload, err := helper.FetchResourceTemplate(c.DynamicClient, c.InformerManager, c.RESTMapper, binding.Spec.Resource)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// It might happen when the resource template has been removed but the garbage collector hasn't removed
			// the ResourceBinding which dependent on resource template.
			// So, just return without retry(requeue) would save unnecessary loop.
			return controllerruntime.Result{}, nil
		}
		klog.Errorf("Failed to fetch workload for resourceBinding(%s/%s). Error: %v.",
			binding.GetNamespace(), binding.GetName(), err)
		return controllerruntime.Result{}, err
	}
	start := time.Now()

	if isEnableDelayedScalingNs(workload.GetNamespace()) && isAtmsNodeCmName(workload.GetName()) {
		err = recordBeginEndpoint(c.GoCache, c.KarmadaSearchCli, c.ResourceInterpreter, workload, binding, apiextensionsv1.NamespaceScoped)
		if err != nil {
			klog.Errorf("recordBeginEndpoint error: %v", err)
		}
	}
	err = ensureWork(c.GoCache, c.Client, c.KarmadaSearchCli, c.ResourceInterpreter, workload, c.OverrideManager, binding, apiextensionsv1.NamespaceScoped)
	metrics.ObserveSyncWorkLatency(err, start)
	if err != nil {
		klog.Errorf("Failed to transform resourceBinding(%s/%s) to works. Error: %v.",
			binding.GetNamespace(), binding.GetName(), err)
		c.EventRecorder.Event(binding, corev1.EventTypeWarning, events.EventReasonSyncWorkFailed, err.Error())
		c.EventRecorder.Event(workload, corev1.EventTypeWarning, events.EventReasonSyncWorkFailed, err.Error())
		return controllerruntime.Result{}, err
	}

	msg := fmt.Sprintf("Sync work of resourceBinding(%s/%s) successful.", binding.Namespace, binding.Name)
	klog.V(4).Infof(msg)
	c.EventRecorder.Event(binding, corev1.EventTypeNormal, events.EventReasonSyncWorkSucceed, msg)
	c.EventRecorder.Event(workload, corev1.EventTypeNormal, events.EventReasonSyncWorkSucceed, msg)
	return controllerruntime.Result{}, nil
}

func (c *ResourceBindingController) removeOrphanWorks(binding *workv1alpha2.ResourceBinding) error {
	works, err := helper.FindOrphanWorks(c.Client, binding.Namespace, binding.Name,
		binding.Labels[workv1alpha2.ResourceBindingPermanentIDLabel], helper.ObtainBindingSpecExistingClusters(binding.Spec))
	if err != nil {
		klog.Errorf("Failed to find orphan works by resourceBinding(%s/%s). Error: %v.",
			binding.GetNamespace(), binding.GetName(), err)
		c.EventRecorder.Event(binding, corev1.EventTypeWarning, events.EventReasonCleanupWorkFailed, err.Error())
		return err
	}

	err = helper.RemoveOrphanWorks(c.Client, works)
	if err != nil {
		klog.Errorf("Failed to remove orphan works by resourceBinding(%s/%s). Error: %v.",
			binding.GetNamespace(), binding.GetName(), err)
		c.EventRecorder.Event(binding, corev1.EventTypeWarning, events.EventReasonCleanupWorkFailed, err.Error())
		return err
	}

	return nil
}

// SetupWithManager creates a controller and register to controller manager.
func (c *ResourceBindingController) SetupWithManager(mgr controllerruntime.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).For(&workv1alpha2.ResourceBinding{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Watches(&policyv1alpha1.OverridePolicy{}, handler.EnqueueRequestsFromMapFunc(c.newOverridePolicyFunc())).
		Watches(&policyv1alpha1.ClusterOverridePolicy{}, handler.EnqueueRequestsFromMapFunc(c.newOverridePolicyFunc())).
		WithOptions(controller.Options{RateLimiter: ratelimiterflag.DefaultControllerRateLimiter(c.RateLimiterOptions)}).
		Complete(c)
}

func (c *ResourceBindingController) newOverridePolicyFunc() handler.MapFunc {
	return func(_ context.Context, a client.Object) []reconcile.Request {
		var overrideRS []policyv1alpha1.ResourceSelector
		var namespace string
		switch t := a.(type) {
		case *policyv1alpha1.ClusterOverridePolicy:
			overrideRS = t.Spec.ResourceSelectors
		case *policyv1alpha1.OverridePolicy:
			overrideRS = t.Spec.ResourceSelectors
			namespace = t.Namespace
		default:
			return nil
		}

		bindingList := &workv1alpha2.ResourceBindingList{}
		if err := c.Client.List(context.TODO(), bindingList); err != nil {
			klog.Errorf("Failed to list resourceBindings, error: %v", err)
			return nil
		}

		var requests []reconcile.Request
		for _, binding := range bindingList.Items {
			// Skip resourceBinding with different namespace of current overridePolicy.
			if len(namespace) != 0 && namespace != binding.Namespace {
				continue
			}

			// Nil resourceSelectors means matching all resources.
			if len(overrideRS) == 0 {
				klog.V(2).Infof("Enqueue ResourceBinding(%s/%s) as override policy(%s/%s) changes.", binding.Namespace, binding.Name, a.GetNamespace(), a.GetName())
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}})
				continue
			}

			workload, err := helper.FetchResourceTemplate(c.DynamicClient, c.InformerManager, c.RESTMapper, binding.Spec.Resource)
			if err != nil {
				// If we cannot fetch resource template from binding, this may be due to the fact that the resource template has been deleted.
				// Just skip it so that it will not affect other bindings.
				klog.Errorf("Failed to fetch workload for resourceBinding(%s/%s). Error: %v.", binding.Namespace, binding.Name, err)
				continue
			}

			for _, rs := range overrideRS {
				if util.ResourceMatches(workload, rs) {
					klog.V(2).Infof("Enqueue ResourceBinding(%s/%s) as override policy(%s/%s) changes.", binding.Namespace, binding.Name, a.GetNamespace(), a.GetName())
					requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: binding.Namespace, Name: binding.Name}})
					break
				}
			}
		}
		return requests
	}
}

type SKarmadaSearch struct {
	SearchCli rest.Interface
	BaseApi   string
	Scheme    *runtime.Scheme
	Mapper    meta.RESTMapper
}

type SearchOptions struct {
	LabelSelector string
	Unique        bool //过滤多集群同名资源, 只保留一份
}

func (ss *SKarmadaSearch) search(ctx context.Context, gvk schema.GroupVersionKind, nn types.NamespacedName,
	options *SearchOptions) (unstructured.UnstructuredList, error) {
	var rawList unstructured.UnstructuredList

	apiPrefix := "/apis"
	if gvk.Group == "" {
		apiPrefix = "/api"
	}
	basePath := fmt.Sprintf("%s/%s", apiPrefix, gvk.GroupVersion())

	// 如果有命名空间，加上命名空间的路径
	if nn.Namespace != "" {
		basePath = fmt.Sprintf("%s/namespaces/%s", basePath, nn.Namespace)
	}

	// 加上资源类型
	rMap, err := ss.Mapper.RESTMapping(schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind}, gvk.Version)
	if err != nil {
		klog.Errorf("get rest mapping err: %v", err)
		return rawList, err
	}
	resourcePath := fmt.Sprintf("%s/%s", basePath, rMap.Resource.Resource)

	// 如果有具体的资源名称，加上资源名称
	if nn.Name != "" {
		resourcePath = fmt.Sprintf("%s/%s", resourcePath, nn.Name)
	}

	req := ss.SearchCli.Get().AbsPath(fmt.Sprintf("%s/%s", ss.BaseApi, resourcePath))
	if options != nil {
		if options.LabelSelector != "" {
			req = req.Param("labelSelector", options.LabelSelector)
		}
	}

	res := req.Do(ctx)

	b, err := res.Raw()
	if err != nil {
		return rawList, err
	}

	err = json.Unmarshal(b, &rawList)
	if err != nil {
		return rawList, err
	}

	if !rawList.IsList() || len(rawList.Items) == 0 {
		return rawList, fmt.Errorf("not found")
	}
	return rawList, nil
}

func (ss *SKarmadaSearch) GetEndpointsFromKarmadaSearch(nn types.NamespacedName) ([]corev1.Endpoints, error) {
	raws, err := ss.search(context.TODO(), schema.GroupVersionKind{
		Group:   "", // Endpoints is core API group
		Version: "v1",
		Kind:    "Endpoints",
	}, nn, &SearchOptions{Unique: true})

	if err != nil {
		return nil, fmt.Errorf("failed to get endpoints from karmadaSearch for %s/%s, error: %v", nn.Namespace, nn.Name, err)
	}

	endpoints := make([]corev1.Endpoints, 0, len(raws.Items))
	for _, raw := range raws.Items {
		var endpoint corev1.Endpoints
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, &endpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to convert unstructured to Endpoints: %v", err)
		}
		endpoints = append(endpoints, endpoint)
	}

	return endpoints, nil
}

type ClusterReplicas struct {
	ClusterName string
	Replicas    int
}

func GetClusterEndpointMap(endpoints []corev1.Endpoints) (map[string]int, error) {
	var clusterEndpoints map[string]int = make(map[string]int)
	for _, endpoint := range endpoints {
		if clusterName, ok := endpoint.Annotations["resource.karmada.io/cached-from-cluster"]; ok {
			endpointLength := 0
			for _, subset := range endpoint.Subsets {
				endpointLength += len(subset.Addresses)
			}
			clusterEndpoints[clusterName] = endpointLength
		}
	}
	return clusterEndpoints, nil
}
