// Package workermgr ensures exactly one Kubernetes ReplicaSet exists per
// Sync, running the same image controlplaned itself runs in (which bundles
// workerd) with command ["workerd", "--sync-id=<name>", ...]. Kubernetes'
// own ReplicaSet controller then owns restart-on-crash; this package only
// owns keeping the set of ReplicaSets in sync with the set of desired Syncs.
package workermgr

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/andersonlaurentino/dal-controlplane/internal/store"
)

const (
	appLabel        = "dal-worker"
	reconcileTick   = 30 * time.Second
	notifyQueueSize = 32
)

// Config carries what workermgr needs to render a worker's ReplicaSet.
type Config struct {
	Namespace            string
	Image                string
	ControlplaneAddr     string // gRPC address, e.g. "controlplane:9090"
	ControlplaneHTTPAddr string // REST address, e.g. "http://controlplane:8080"
}

type Manager struct {
	clientset *kubernetes.Clientset
	store     *store.Store
	cfg       Config
	log       *slog.Logger
	notify    chan string
}

func New(clientset *kubernetes.Clientset, st *store.Store, cfg Config, log *slog.Logger) *Manager {
	return &Manager{
		clientset: clientset,
		store:     st,
		cfg:       cfg,
		log:       log,
		notify:    make(chan string, notifyQueueSize),
	}
}

// Notify tells the manager a Sync named `name` was applied or deleted; it
// reconciles just that one Sync against the store's current state.
func (m *Manager) Notify(name string) {
	select {
	case m.notify <- name:
	default:
		m.log.Warn("workermgr notify queue full, dropping (periodic reconcile will catch up)", "sync", name)
	}
}

// Start reconciles once immediately (catching ReplicaSets left over from a
// previous run, or Syncs that appeared while controlplaned was down), then
// keeps reconciling on every Notify and on a periodic safety-net tick until
// ctx is cancelled.
func (m *Manager) Start(ctx context.Context) {
	m.reconcileAll(ctx)

	ticker := time.NewTicker(reconcileTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case name := <-m.notify:
			m.reconcileOne(ctx, name)
		case <-ticker.C:
			m.reconcileAll(ctx)
		}
	}
}

func (m *Manager) reconcileOne(ctx context.Context, name string) {
	exists, err := m.store.Exists(ctx, store.KindSync, name)
	if err != nil {
		m.log.Error("workermgr: checking sync existence", "sync", name, "err", err)
		return
	}
	if !exists {
		if err := m.deleteReplicaSet(ctx, name); err != nil {
			m.log.Error("workermgr: deleting replicaset", "sync", name, "err", err)
		}
		return
	}
	// Always delete-then-recreate: the Sync's spec may have changed, and
	// workerd only reads it once at startup, so a fresh Pod is required to
	// pick up new topic/table/mapping/schedule values. Wait for the delete
	// to fully land before recreating so the API server doesn't reject the
	// create as a still-terminating duplicate.
	if err := m.deleteReplicaSet(ctx, name); err != nil {
		m.log.Error("workermgr: deleting replicaset before recreate", "sync", name, "err", err)
		return
	}
	if err := m.waitDeleted(ctx, name); err != nil {
		m.log.Error("workermgr: waiting for replicaset deletion", "sync", name, "err", err)
		return
	}
	if err := m.createReplicaSet(ctx, name); err != nil {
		m.log.Error("workermgr: creating replicaset", "sync", name, "err", err)
	}
}

func (m *Manager) reconcileAll(ctx context.Context) {
	desired, err := m.store.List(ctx, store.KindSync)
	if err != nil {
		m.log.Error("workermgr: listing syncs", "err", err)
		return
	}
	desiredSet := make(map[string]bool, len(desired))
	for _, name := range desired {
		desiredSet[name] = true
	}

	existing, err := m.listReplicaSets(ctx)
	if err != nil {
		m.log.Error("workermgr: listing replicasets", "err", err)
		return
	}

	for _, name := range desired {
		if !existing[name] {
			if err := m.createReplicaSet(ctx, name); err != nil {
				m.log.Error("workermgr: creating replicaset", "sync", name, "err", err)
			}
		}
	}
	for name := range existing {
		if !desiredSet[name] {
			if err := m.deleteReplicaSet(ctx, name); err != nil {
				m.log.Error("workermgr: deleting orphaned replicaset", "sync", name, "err", err)
			}
		}
	}
}

func (m *Manager) listReplicaSets(ctx context.Context) (map[string]bool, error) {
	list, err := m.clientset.AppsV1().ReplicaSets(m.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + appLabel,
	})
	if err != nil {
		return nil, err
	}
	names := make(map[string]bool, len(list.Items))
	for _, rs := range list.Items {
		if sync := rs.Labels["sync"]; sync != "" {
			names[sync] = true
		}
	}
	return names, nil
}

func replicaSetName(syncName string) string {
	return "dal-worker-" + syncName
}

func (m *Manager) createReplicaSet(ctx context.Context, syncName string) error {
	replicas := int32(1)
	labels := map[string]string{"app": appLabel, "sync": syncName}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      replicaSetName(syncName),
			Namespace: m.cfg.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "workerd",
							Image:           m.cfg.Image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"workerd"},
							Args: []string{
								"--sync-id=" + syncName,
								"--controlplane-addr=" + m.cfg.ControlplaneAddr,
								"--controlplane-http-addr=" + m.cfg.ControlplaneHTTPAddr,
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
		},
	}

	_, err := m.clientset.AppsV1().ReplicaSets(m.cfg.Namespace).Create(ctx, rs, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("creating replicaset for sync %q: %w", syncName, err)
	}
	m.log.Info("workermgr: created replicaset", "sync", syncName)
	return nil
}

// waitDeleted polls until the ReplicaSet is gone or the timeout elapses,
// since Delete returns before Kubernetes has finished tearing down the
// object (and its Pod), and creating a same-named replacement too early
// would be rejected as a duplicate.
func (m *Manager) waitDeleted(ctx context.Context, syncName string) error {
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := m.clientset.AppsV1().ReplicaSets(m.cfg.Namespace).Get(ctx, replicaSetName(syncName), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for replicaset %q to be deleted", replicaSetName(syncName))
		case <-ticker.C:
		}
	}
}

func (m *Manager) deleteReplicaSet(ctx context.Context, syncName string) error {
	err := m.clientset.AppsV1().ReplicaSets(m.cfg.Namespace).Delete(ctx, replicaSetName(syncName), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deleting replicaset for sync %q: %w", syncName, err)
	}
	m.log.Info("workermgr: deleted replicaset", "sync", syncName)
	return nil
}
