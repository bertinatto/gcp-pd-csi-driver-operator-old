package operator

import (
	"context"
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog"

	apiclientset "github.com/openshift/client-go/config/clientset/versioned"
	apiinformers "github.com/openshift/client-go/config/informers/externalversions"
	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"github.com/openshift/library-go/pkg/operator/loglevel"
	"github.com/openshift/library-go/pkg/operator/management"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/staticresourcecontroller"
	"github.com/openshift/library-go/pkg/operator/status"
	"github.com/openshift/library-go/pkg/operator/v1helpers"

	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/common"
	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/generated"
	clientset "github.com/openshift/gcp-pd-csi-driver-operator/pkg/generated/clientset/versioned"
	informers "github.com/openshift/gcp-pd-csi-driver-operator/pkg/generated/informers/externalversions"
)

const (
	resync = 20 * time.Minute
)

func RunOperator(ctx context.Context, controllerConfig *controllercmd.ControllerContext) error {
	ctrlClientset, err := clientset.NewForConfig(controllerConfig.KubeConfig)
	if err != nil {
		return err
	}

	ctrlInformers := informers.NewSharedInformerFactoryWithOptions(
		ctrlClientset,
		resync,
		informers.WithTweakListOptions(singleNameListOptions(globalConfigName)),
	)

	apiClientset, err := apiclientset.NewForConfig(controllerConfig.KubeConfig)
	if err != nil {
		return err
	}

	dynamicConfig := dynamic.ConfigFor(controllerConfig.KubeConfig)
	dynamicClient, err := dynamic.NewForConfig(dynamicConfig)
	if err != nil {
		return err
	}

	apiInformers := apiinformers.NewSharedInformerFactoryWithOptions(apiClientset, resync)

	operatorClient := OperatorClient{
		ctrlInformers,
		ctrlClientset.GcpV1alpha1(),
	}

	// // operatorClient, _, err := goc.NewClusterScopedOperatorClient(
	// // 	controllerConfig.KubeConfig,
	// // 	v1alpha1.SchemeGroupVersion.WithResource("pddrivers"),
	// // )
	// if err != nil {
	// 	return err
	// }

	cb, err := common.NewBuilder("")
	if err != nil {
		klog.Fatalf("error creating clients: %v", err)
	}

	ctrlCtx := common.CreateControllerContext(cb, ctx.Done(), operandNamespace)
	versionGetter := status.NewVersionGetter()
	kubeClient := ctrlCtx.ClientBuilder.KubeClientOrDie(operandName)

	operator := NewCSIDriverOperator(
		operatorClient,
		ctrlCtx.KubeNamespacedInformerFactory.Core().V1().PersistentVolumes(),
		ctrlCtx.KubeNamespacedInformerFactory.Core().V1().Namespaces(),
		ctrlCtx.KubeNamespacedInformerFactory.Storage().V1beta1().CSIDrivers(),
		ctrlCtx.KubeNamespacedInformerFactory.Core().V1().ServiceAccounts(),
		ctrlCtx.KubeNamespacedInformerFactory.Rbac().V1().ClusterRoles(),
		ctrlCtx.KubeNamespacedInformerFactory.Rbac().V1().ClusterRoleBindings(),
		ctrlCtx.KubeNamespacedInformerFactory.Apps().V1().Deployments(),
		ctrlCtx.KubeNamespacedInformerFactory.Apps().V1().DaemonSets(),
		ctrlCtx.KubeNamespacedInformerFactory.Storage().V1().StorageClasses(),
		ctrlCtx.KubeNamespacedInformerFactory.Core().V1().Secrets(),
		kubeClient,
		dynamicClient,
		versionGetter,
		controllerConfig.EventRecorder,
		os.Getenv(operatorVersionEnvName),
		os.Getenv(operandVersionEnvName),
		imagesFromEnv(),
	)

	// This controller syncs CR.Status.Conditions with the value in the field CR.Spec.ManagementStatus. It only supports Managed state
	managementStateController := management.NewOperatorManagementStateController(operandName, operatorClient, controllerConfig.EventRecorder)
	management.SetOperatorNotRemovable()

	// This controller syncs the operator log level with the value set in the CR.Spec.OperatorLogLevel
	logLevelController := loglevel.NewClusterOperatorLoggingController(operatorClient, controllerConfig.EventRecorder)

	// Static files
	kubeInformersForNamespaces := v1helpers.NewKubeInformersForNamespaces(kubeClient,
		"",
		operandNamespace,
		operatorNamespace)

	staticResourceController := staticresourcecontroller.NewStaticResourceController(
		"GCPPDDriverStaticResources",
		generated.Asset,
		[]string{
			"namespace.yaml",
			"controller_sa.yaml",
			"node_sa.yaml",
			"storageclass.yaml",
			"rbac/provisioner_binding.yaml",
			"rbac/provisioner_role.yaml",
			"rbac/attacher_binding.yaml",
			"rbac/attacher_role.yaml",
		},
		(&resourceapply.ClientHolder{}).WithKubernetes(kubeClient),
		operatorClient,
		operator.eventRecorder,
	).AddKubeInformers(kubeInformersForNamespaces)

	klog.Info("Starting the Informers.")
	for _, informer := range []interface {
		Start(stopCh <-chan struct{})
	}{
		ctrlInformers,
		apiInformers,
		ctrlCtx.KubeNamespacedInformerFactory,
		kubeInformersForNamespaces,
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
	} {
		go controller.Run(ctx, 1)
	}

	klog.Info("Starting the operator.")
	// go operator.Run(1, ctx.Done())

	<-ctx.Done()

	return fmt.Errorf("stopped")
}

func singleNameListOptions(name string) func(opts *metav1.ListOptions) {
	return func(opts *metav1.ListOptions) {
		opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
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
