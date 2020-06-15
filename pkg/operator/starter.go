package operator

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/klog"

	"github.com/openshift/library-go/pkg/controller/controllercmd"
	goc "github.com/openshift/library-go/pkg/operator/genericoperatorclient"
	"github.com/openshift/library-go/pkg/operator/loglevel"
	"github.com/openshift/library-go/pkg/operator/management"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/staticresourcecontroller"
	"github.com/openshift/library-go/pkg/operator/v1helpers"

	"github.com/bertinatto/csi-driver-controller/pkg/csidrivercontroller"

	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/apis/operator/v1alpha1"
	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/common"
	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/generated"
)

const (
	operandName       = "gcp-pd-csi-driver"
	operandNamespace  = "openshift-gcp-pd-csi-driver"
	operatorNamespace = "openshift-gcp-pd-csi-driver-operator"

	resync = 20 * time.Minute
)

func RunOperator(ctx context.Context, controllerConfig *controllercmd.ControllerContext) error {

	dynamicConfig := dynamic.ConfigFor(controllerConfig.KubeConfig)
	dynamicClientset, err := dynamic.NewForConfig(dynamicConfig)
	if err != nil {
		return err
	}

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

	// This controller syncs CR.Status.Conditions with the value in the field CR.Spec.ManagementStatus. It only supports Managed state
	managementStateController := management.NewOperatorManagementStateController(operandName, operatorClient, controllerConfig.EventRecorder)
	management.SetOperatorNotRemovable()

	// This controller syncs the operator log level with the value set in the CR.Spec.OperatorLogLevel
	logLevelController := loglevel.NewClusterOperatorLoggingController(operatorClient, controllerConfig.EventRecorder)

	kubeInformersForNamespaces := v1helpers.NewKubeInformersForNamespaces(
		kubeClient,
		"",
		operandNamespace,
		operatorNamespace)

	// This controller makes sure some static files are syncs
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

	// This controller syncs a CSI driver (deployment, daemonSet and credentialsRequest)
	config := &csidrivercontroller.Config{
		OperandName:         operandName,
		OperandNamespace:    operandNamespace,
		ControllerManifest:  generated.MustAsset("controller.yaml"),
		NodeManifest:        generated.MustAsset("node.yaml"),
		CredentialsManifest: generated.MustAsset("credentials.yaml"),
	}
	csiDriverController, err := csidrivercontroller.NewCSIDriverController(
		config,
		operatorClient,
		dynamicClientset,
		kubeClient,
		ctrlCtx.KubeNamespacedInformerFactory.Apps().V1().Deployments(),
		ctrlCtx.KubeNamespacedInformerFactory.Apps().V1().DaemonSets(),
		controllerConfig.EventRecorder,
	)
	if err != nil {
		return err
	}

	klog.Info("Starting the Informers.")
	for _, informer := range []interface {
		Start(stopCh <-chan struct{})
	}{
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
