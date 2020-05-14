package csidrivercontroller

import (
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	appsinformersv1 "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"

	operatorv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/status"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
)

var log = logf.Log.WithName("gcp_pd_csi_driver_operator")

const (
	operandName       = "gcp-pd-csi-driver"
	operandNamespace  = "openshift-gcp-pd-csi-driver"
	operatorNamespace = "openshift-gcp-pd-csi-driver-operator"

	operatorVersionEnvName          = "OPERATOR_IMAGE_VERSION"
	operandVersionEnvName           = "OPERAND_IMAGE_VERSION"
	driverImageEnvName              = "DRIVER_IMAGE"
	provisionerImageEnvName         = "PROVISIONER_IMAGE"
	attacherImageEnvName            = "ATTACHER_IMAGE"
	resizerImageEnvName             = "RESIZER_IMAGE"
	snapshotterImageEnvName         = "SNAPSHOTTER_IMAGE"
	nodeDriverRegistrarImageEnvName = "NODE_DRIVER_REGISTRAR_IMAGE"
	livenessProbeImageEnvName       = "LIVENESS_PROBE_IMAGE"

	// Index of a container in assets/controller.yaml and assets/node.yaml
	csiDriverContainerIndex           = 0 // Both Deployment and DaemonSet
	provisionerContainerIndex         = 1
	attacherContainerIndex            = 2
	resizerContainerIndex             = 3
	snapshottterContainerIndex        = 4
	nodeDriverRegistrarContainerIndex = 1
	livenessProbeContainerIndex       = 2 // Only in DaemonSet

	globalConfigName = "cluster"

	maxRetries = 15
)

type csiDriverController struct {
	client             v1helpers.OperatorClient
	kubeClient         kubernetes.Interface
	dynamicClient      dynamic.Interface
	deploymentInformer appsinformersv1.DeploymentInformer
	dsSetInformer      appsinformersv1.DaemonSetInformer
	versionGetter      status.VersionGetter
	eventRecorder      events.Recorder
	informersSynced    []cache.InformerSynced

	syncHandler func() error

	queue workqueue.RateLimitingInterface

	stopCh <-chan struct{}

	operatorVersion string
	operandVersion  string
	images          images

	manifests resourceapply.AssetFunc
	files     []string
}

type images struct {
	csiDriver           string
	attacher            string
	provisioner         string
	resizer             string
	snapshotter         string
	nodeDriverRegistrar string
	livenessProbe       string
}

func NewCSIDriverController(
	client v1helpers.OperatorClient,
	dynamicClient dynamic.Interface,
	kubeClient kubernetes.Interface,
	deployInformer appsinformersv1.DeploymentInformer,
	dsInformer appsinformersv1.DaemonSetInformer,
	versionGetter status.VersionGetter,
	eventRecorder events.Recorder,
	operatorVersion string,
	operandVersion string,
	manifests resourceapply.AssetFunc,
	files []string,
) *csiDriverController {
	controller := &csiDriverController{
		client:             client,
		kubeClient:         kubeClient,
		dynamicClient:      dynamicClient,
		deploymentInformer: deployInformer,
		dsSetInformer:      dsInformer,
		versionGetter:      versionGetter,
		eventRecorder:      eventRecorder,
		queue:              workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "gcp-pd-csi-driver"),
		operatorVersion:    operatorVersion,
		operandVersion:     operandVersion,
		images:             imagesFromEnv(),
		manifests:          manifests,
		files:              files,
	}

	deployInformer.Informer().AddEventHandler(controller.eventHandler("deployment"))
	dsInformer.Informer().AddEventHandler(controller.eventHandler("daemonset"))
	client.Informer().AddEventHandler(controller.eventHandler("csidriver")) //TODO: more generic name?

	controller.informersSynced = append(
		controller.informersSynced,
		deployInformer.Informer().HasSynced,
		dsInformer.Informer().HasSynced,
		client.Informer().HasSynced,
	)

	controller.syncHandler = controller.sync

	return controller
}

func (c *csiDriverController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.stopCh = stopCh

	if !cache.WaitForCacheSync(stopCh, c.informersSynced...) {
		return
	}
	klog.Infof("Caches synced, running the controller")

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (c *csiDriverController) sync() error {
	meta, _, err := c.client.GetObjectMeta()
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Warningf("Object metadata not found: %v", err)
			return nil
		}
		return err
	}

	opSpec, opStatus, opResourceVersion, err := c.client.GetOperatorState()
	if err != nil {
		return err
	}

	// We only support Managed for now
	if opSpec.ManagementState != operatorv1.Managed {
		return nil
	}

	startTime := time.Now()
	klog.Info("Starting syncing operator at ", startTime)
	defer func() {
		klog.Info("Finished syncing operator at ", time.Since(startTime))
	}()

	syncErr := c.handleSync(opResourceVersion, meta, opSpec, opStatus)
	if syncErr != nil {
		c.eventRecorder.Eventf("SyncError", "Error syncing CSI driver: %s", syncErr)
	}

	c.updateSyncError(opStatus, syncErr)

	// Update the status using our copy
	_, _, err = v1helpers.UpdateStatus(c.client, func(status *operatorv1.OperatorStatus) error {
		// Store a copy of our starting conditions, we need to preserve last transition time
		originalConditions := status.DeepCopy().Conditions

		// Copy over everything else
		opStatus.DeepCopyInto(status)

		// Restore the starting conditions
		status.Conditions = originalConditions

		// Manually update the conditions while preserving last transition time
		for _, condition := range opStatus.Conditions {
			v1helpers.SetOperatorCondition(&status.Conditions, condition)
		}

		return nil
	})

	if err != nil {
		klog.Errorf("failed to update status: %v", err)
		if syncErr == nil {
			syncErr = err
		}
	}

	return syncErr
}

func (c *csiDriverController) updateSyncError(status *operatorv1.OperatorStatus, err error) {
	if err != nil {
		// Operator is Degraded: could not finish what it was doing
		v1helpers.SetOperatorCondition(&status.Conditions,
			operatorv1.OperatorCondition{
				Type:    operatorv1.OperatorStatusTypeDegraded,
				Status:  operatorv1.ConditionTrue,
				Reason:  "OperatorSync",
				Message: err.Error(),
			})
		// Operator is Progressing: some action failed, will try to progress more after exp. backoff.
		// Do not overwrite existing "Progressing=true" condition to keep its message.
		cnd := v1helpers.FindOperatorCondition(status.Conditions, operatorv1.OperatorStatusTypeProgressing)
		if cnd == nil || cnd.Status == operatorv1.ConditionFalse {
			v1helpers.SetOperatorCondition(&status.Conditions,
				operatorv1.OperatorCondition{
					Type:    operatorv1.OperatorStatusTypeProgressing,
					Status:  operatorv1.ConditionTrue,
					Reason:  "OperatorSync",
					Message: err.Error(),
				})
		}
	} else {
		v1helpers.SetOperatorCondition(&status.Conditions,
			operatorv1.OperatorCondition{
				Type:   operatorv1.OperatorStatusTypeDegraded,
				Status: operatorv1.ConditionFalse,
			})
		// Progressing condition was set in c.handleSync().
	}
}

func (c *csiDriverController) handleSync(resourceVersion string, meta *metav1.ObjectMeta, spec *operatorv1.OperatorSpec, status *operatorv1.OperatorStatus) error {
	credentialsRequest, err := c.syncCredentialsRequest(status)
	if err != nil {
		return fmt.Errorf("failed to sync CredentialsRequest: %v", err)
	}

	// TODO: wait for secret

	deployment, err := c.syncDeployment(spec, status)
	if err != nil {
		return fmt.Errorf("failed to sync Deployment: %v", err)
	}

	daemonSet, err := c.syncDaemonSet(spec, status)
	if err != nil {
		return fmt.Errorf("failed to sync DaemonSet: %v", err)
	}

	if err := c.syncStatus(meta, status, deployment, daemonSet, credentialsRequest); err != nil {
		return fmt.Errorf("failed to sync status: %v", err)
	}

	return nil
}

func (c *csiDriverController) setVersion(operandName, version string) {
	if c.versionGetter.GetVersions()[operandName] != version {
		c.versionGetter.SetVersion(operandName, version)
	}
}

func (c *csiDriverController) versionChanged(operandName, version string) bool {
	return c.versionGetter.GetVersions()[operandName] != version
}

func (c *csiDriverController) enqueue(obj interface{}) {
	// we're filtering out config maps that are "leader" based and we don't have logic around them
	// resyncing on these causes the operator to sync every 14s for no good reason
	if cm, ok := obj.(*corev1.ConfigMap); ok && cm.GetAnnotations() != nil && cm.GetAnnotations()[resourcelock.LeaderElectionRecordAnnotationKey] != "" {
		return
	}
	// Sync corresponding Driver instance. Since there is only one, sync that one.
	// It will check all other objects (Deployment, DaemonSet) and update/overwrite them as needed.
	c.queue.Add(globalConfigName)
}

func (c *csiDriverController) eventHandler(kind string) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			logInformerEvent(kind, obj, "added")
			c.enqueue(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			logInformerEvent(kind, new, "updated")
			c.enqueue(new)
		},
		DeleteFunc: func(obj interface{}) {
			logInformerEvent(kind, obj, "deleted")
			c.enqueue(obj)
		},
	}
}

func (c *csiDriverController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *csiDriverController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncHandler()
	c.handleErr(err, key)

	return true
}

func (c *csiDriverController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		klog.V(2).Infof("Error syncing operator %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	klog.V(2).Infof("Dropping operator %q out of the queue: %v", key, err)
	c.queue.Forget(key)
	c.queue.AddAfter(key, 1*time.Minute)
}

func logInformerEvent(kind, obj interface{}, message string) {
	if klog.V(6) {
		objMeta, err := meta.Accessor(obj)
		if err != nil {
			return
		}
		klog.V(6).Infof("Received event: %s %s %s", kind, objMeta.GetName(), message)
	}
}

func imagesFromEnv() images {
	return images{
		csiDriver:           os.Getenv(driverImageEnvName),
		provisioner:         os.Getenv(provisionerImageEnvName),
		attacher:            os.Getenv(attacherImageEnvName),
		resizer:             os.Getenv(resizerImageEnvName),
		snapshotter:         os.Getenv(snapshotterImageEnvName),
		nodeDriverRegistrar: os.Getenv(nodeDriverRegistrarImageEnvName),
		livenessProbe:       os.Getenv(livenessProbeImageEnvName),
	}
}
