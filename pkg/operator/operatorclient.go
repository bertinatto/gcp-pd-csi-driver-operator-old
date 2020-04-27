package operator

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	operatorv1 "github.com/openshift/api/operator/v1"

	"github.com/openshift/gcp-pd-csi-driver-operator/pkg/apis/operator/v1alpha1"
	configset "github.com/openshift/gcp-pd-csi-driver-operator/pkg/generated/clientset/versioned/typed/operator/v1alpha1"
	informers "github.com/openshift/gcp-pd-csi-driver-operator/pkg/generated/informers/externalversions"
)

type OperatorClient struct {
	Informers informers.SharedInformerFactory
	Client    configset.PDDriversGetter
}

func (c OperatorClient) Informer() cache.SharedIndexInformer {
	return c.Informers.Gcp().V1alpha1().PDDrivers().Informer()
}

func (c OperatorClient) GetOperatorState() (*operatorv1.OperatorSpec, *operatorv1.OperatorStatus, string, error) {
	instance, err := c.Informers.Gcp().V1alpha1().PDDrivers().Lister().Get(globalConfigName)
	if err != nil {
		return nil, nil, "", err
	}

	return &instance.Spec.OperatorSpec, &instance.Status.OperatorStatus, instance.ResourceVersion, nil
}

func (c OperatorClient) UpdateOperatorSpec(resourceVersion string, spec *operatorv1.OperatorSpec) (*operatorv1.OperatorSpec, string, error) {
	original, err := c.Informers.Gcp().V1alpha1().PDDrivers().Lister().Get(globalConfigName)
	if err != nil {
		return nil, "", err
	}
	copy := original.DeepCopy()
	copy.ResourceVersion = resourceVersion
	copy.Spec.OperatorSpec = *spec

	ret, err := c.Client.PDDrivers().Update(context.TODO(), copy, metav1.UpdateOptions{})
	if err != nil {
		return nil, "", err
	}

	return &ret.Spec.OperatorSpec, ret.ResourceVersion, nil
}

func (c OperatorClient) UpdateOperatorStatus(resourceVersion string, status *operatorv1.OperatorStatus) (*operatorv1.OperatorStatus, error) {
	original, err := c.Informers.Gcp().V1alpha1().PDDrivers().Lister().Get(globalConfigName)
	if err != nil {
		return nil, err
	}
	copy := original.DeepCopy()
	copy.ResourceVersion = resourceVersion
	copy.Status.OperatorStatus = *status

	ret, err := c.Client.PDDrivers().UpdateStatus(context.TODO(), copy, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}

	return &ret.Status.OperatorStatus, nil
}

func (c OperatorClient) GetOperatorInstance() (*v1alpha1.PDDriver, error) {
	instance, err := c.Informers.Gcp().V1alpha1().PDDrivers().Lister().Get(globalConfigName)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func (c OperatorClient) UpdateFinalizers(instance *v1alpha1.PDDriver) (*v1alpha1.PDDriver, string, error) {
	original, err := c.Informers.Gcp().V1alpha1().PDDrivers().Lister().Get(globalConfigName)
	if err != nil {
		return nil, "", err
	}
	copy := original.DeepCopy()
	copy.ResourceVersion = instance.ResourceVersion
	copy.ObjectMeta.Finalizers = instance.ObjectMeta.Finalizers

	ret, err := c.Client.PDDrivers().Update(context.TODO(), copy, metav1.UpdateOptions{})
	if err != nil {
		return nil, "", err
	}

	return ret, ret.ResourceVersion, nil
}
