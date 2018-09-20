/*
Copyright 2018 The Kubernetes Authors.

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

package serviceaccount

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/util/metrics"
)

const CAConfigMapName = "serviceaccount-ca-cert"

func DefaultCaConfigMap(rootCA []byte) v1.ConfigMap {
	return v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: CAConfigMapName,
		},
		Data: map[string]string{
			"ca.crt": string(rootCA),
		},
	}

}

func NewCaConfigMapController(cmInformer coreinformers.ConfigMapInformer, nsInformer coreinformers.NamespaceInformer, cl clientset.Interface, cm v1.ConfigMap) (*CaConfigMapController, error) {
	e := &CaConfigMapController{
		client:    cl,
		configMap: cm,
		queue:     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "configmap"),
	}
	if cl != nil && cl.CoreV1().RESTClient().GetRateLimiter() != nil {
		if err := metrics.RegisterMetricAndTrackRateLimiterUsage("ca_map_controller", cl.CoreV1().RESTClient().GetRateLimiter()); err != nil {
			return nil, err
		}
	}

	cmInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: e.configMapDeleted,
	})
	e.cmLister = cmInformer.Lister()
	e.cmListerSynced = cmInformer.Informer().HasSynced

	nsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    e.namespaceAdded,
		UpdateFunc: e.namespaceUpdated,
	})
	e.nsLister = nsInformer.Lister()
	e.nsListerSynced = nsInformer.Informer().HasSynced

	e.syncHandler = e.syncNamespace

	return e, nil

}

type CaConfigMapController struct {
	client    clientset.Interface
	configMap v1.ConfigMap

	// To allow injection for testing.
	syncHandler func(key string) error

	cmLister       corelisters.ConfigMapLister
	cmListerSynced cache.InformerSynced

	nsLister       corelisters.NamespaceLister
	nsListerSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface
}

func (c *CaConfigMapController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	glog.Infof("Starting ca configmap controller")
	defer glog.Infof("Shutting down ca configmap controller")

	if !controller.WaitForCacheSync("ca configmap", stopCh, c.cmListerSynced, c.nsListerSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *CaConfigMapController) configMapDeleted(obj interface{}) {
	cm, ok := obj.(*v1.ConfigMap)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("Couldn't get object from tombstone %#v", obj))
			return
		}
		cm, ok = tombstone.Obj.(*v1.ConfigMap)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("Tombstone contained object that is not a ConfigMap %#v", obj))
			return
		}
	}
	c.queue.Add(cm.Namespace)
}

func (c *CaConfigMapController) namespaceAdded(obj interface{}) {
	namespace := obj.(*v1.Namespace)
	c.queue.Add(namespace.Name)
}

func (c *CaConfigMapController) namespaceUpdated(oldObj interface{}, newObj interface{}) {
	newNamespace := newObj.(*v1.Namespace)
	c.queue.Add(newNamespace.Name)
}

func (c *CaConfigMapController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
func (c *CaConfigMapController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncHandler(key.(string))
	if err == nil {
		c.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", key, err))
	c.queue.AddRateLimited(key)

	return true
}

func (c *CaConfigMapController) syncNamespace(key string) error {
	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing namespace %q (%v)", key, time.Since(startTime))
	}()

	ns, err := c.nsLister.Get(key)
	if apierrs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if ns.Status.Phase != v1.NamespaceActive {
		// If namespace is not active, we shouldn't try to create anything
		return nil
	}

	createFailures := []error{}

	switch _, err := c.cmLister.ConfigMaps(ns.Name).Get(c.configMap.Name); {
	case err == nil:
		return nil
	case apierrs.IsNotFound(err):
	case err != nil:
		return err
	}

	if _, err := c.client.CoreV1().ConfigMaps(ns.Name).Create(&c.configMap); err != nil && !apierrs.IsAlreadyExists(err) {
		createFailures = append(createFailures, err)
	}

	return utilerrors.Flatten(utilerrors.NewAggregate(createFailures))
}
