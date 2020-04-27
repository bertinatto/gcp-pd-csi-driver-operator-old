package v1alpha1

import (
	operatorv1 "github.com/openshift/api/operator/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PDDriver is a specification for a PDDriver resource
type PDDriver struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PDDriverSpec   `json:"spec"`
	Status PDDriverStatus `json:"status"`
}

// PDDriverSpec is the spec for a PDPDDriver resource
type PDDriverSpec struct {
	operatorv1.OperatorSpec `json:",inline"`
}

// PDDriverStatus is the status for a PDDriver resource
type PDDriverStatus struct {
	operatorv1.OperatorStatus `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PDDriverList is a list of PDDriver resources
type PDDriverList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []PDDriver `json:"items"`
}
