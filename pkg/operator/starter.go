package operator

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/klog"

	apiclientset "github.com/openshift/client-go/config/clientset/versioned"
	apiinformers "github.com/openshift/client-go/config/informers/externalversions"
	"github.com/openshift/library-go/pkg/controller/controllercmd"
	goc "github.com/openshift/library-go/pkg/operator/genericoperatorclient"
	"github.com/openshift/library-go/pkg/operator/loglevel"
	"github.com/openshift/library-go/pkg/operator/management"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/staticresourcecontroller"
	"github.com/openshift/library-go/pkg/operator/v1helpers"

	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/apis/operator/v1alpha1"
	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/common"
	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/generated"

	"github.com/bertinatto/csi-driver-controller/pkg/csidrivercontroller"
)

const (
	operandName       = "gcp-pd-csi-driver"
	operandNamespace  = "openshift-gcp-pd-csi-driver"
	operatorNamespace = "openshift-gcp-pd-csi-driver-operator"

	resync = 20 * time.Minute
)

func RunOperator(ctx context.Context, controllerConfig *controllercmd.ControllerContext) error {
	apiClientset, err := apiclientset.NewForConfig(controllerConfig.KubeConfig)
	if err != nil {
		return err
	}

	dynamicConfig := dynamic.ConfigFor(controllerConfig.KubeConfig)
	dynamicClientset, err := dynamic.NewForConfig(dynamicConfig)
	if err != nil {
		return err
	}

	apiInformers := apiinformers.NewSharedInformerFactoryWithOptions(apiClientset, resync)

	// Create GenericOperatorclient. This is used by controllers created down below
	gvr := v1alpha1.SchemeGroupVersion.WithResource("pddrivers")
	operatorClient, dynamicInformers, err := goc.NewClusterScopedOperatorClient(controllerConfig.KubeConfig, gvr)
	if err != nil {
		return err
	}

	cb, err := common.NewBuilder("")
	if err != nil {
		klog.Fatalf("error creating clients: %v", err)
	}

	ctrlCtx := common.CreateControllerContext(cb, ctx.Done(), operandNamespace)
	kubeClient := ctrlCtx.ClientBuilder.KubeClientOrDie(operandName)

	csiDriverController := csidrivercontroller.NewCSIDriverController(
		operatorClient,
		dynamicClientset,
		kubeClient,
		ctrlCtx.KubeNamespacedInformerFactory.Apps().V1().Deployments(),
		ctrlCtx.KubeNamespacedInformerFactory.Apps().V1().DaemonSets(),
		controllerConfig.EventRecorder,
		generated.Asset,
		[]string{
			// "controller.yaml",
			// "node.yaml",
			// "credentials.yaml",
		},
	)

	// This controller syncs CR.Status.Conditions with the value in the field CR.Spec.ManagementStatus. It only supports Managed state
	managementStateController := management.NewOperatorManagementStateController(operandName, operatorClient, controllerConfig.EventRecorder)
	management.SetOperatorNotRemovable()

	// This controller syncs the operator log level with the value set in the CR.Spec.OperatorLogLevel
	logLevelController := loglevel.NewClusterOperatorLoggingController(operatorClient, controllerConfig.EventRecorder)

	// Static files
	kubeInformersForNamespaces := v1helpers.NewKubeInformersForNamespaces(
		kubeClient,
		"",
		operandNamespace,
		operatorNamespace)

	staticResourceController := staticresourcecontroller.NewStaticResourceController(
		"GCPPDDriverStaticResources",
		generated.Asset,
		[]string{
			"namespace.yaml",
			"storageclass.yaml",
			"controller_sa.yaml",
			"node_sa.yaml",
			"rbac/provisioner_binding.yaml",
			"rbac/provisioner_role.yaml",
			"rbac/attacher_binding.yaml",
			"rbac/attacher_role.yaml",
			"rbac/privileged_role.yaml",
			"rbac/controller_privileged_binding.yaml",
			"rbac/node_privileged_binding.yaml",
		},
		(&resourceapply.ClientHolder{}).WithKubernetes(kubeClient),
		operatorClient,
		controllerConfig.EventRecorder,
	).AddKubeInformers(kubeInformersForNamespaces)

	klog.Info("Starting the Informers.")
	for _, informer := range []interface {
		Start(stopCh <-chan struct{})
	}{
		apiInformers,
		ctrlCtx.KubeNamespacedInformerFactory,
		kubeInformersForNamespaces,
		dynamicInformers,
	} {
		informer.Start(ctx.Done())
	}

	klog.Info("Starting the controllers")
	for _, controller := range []interface {
		Run(ctx context.Context, workers int)
	}{
		staticResourceController,
		logLevelController,
		managementStateController,
		csiDriverController,
	} {
		go controller.Run(ctx, 1)
	}

	<-ctx.Done()

	return fmt.Errorf("stopped")
}
