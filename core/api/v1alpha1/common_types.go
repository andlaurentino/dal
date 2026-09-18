// Package v1alpha1 defines the DAL custom resource types: Connection, Sync, and Projection.
package v1alpha1

// TypeMeta mirrors Kubernetes' apiVersion/kind envelope so resources are
// self-describing when serialized standalone (e.g. in a YAML file).
type TypeMeta struct {
	APIVersion string `json:"apiVersion" yaml:"apiVersion"`
	Kind       string `json:"kind" yaml:"kind"`
}

// ObjectMeta carries the identity of a resource. Name is the unique key
// within its kind and is what other resources reference (connectionRef,
// syncRef).
type ObjectMeta struct {
	Name string `json:"name" yaml:"name"`
}

const APIVersion = "dal.io/v1alpha1"

const (
	KindConnection = "Connection"
	KindSync       = "Sync"
	KindProjection = "Projection"
)
