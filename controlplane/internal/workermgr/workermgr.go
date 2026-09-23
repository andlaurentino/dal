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
	"sort"
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
	// S3CredentialsSecret names the k8s Secret (keys "access-key"/
	// "secret-key") workers read S3-compatible datalake credentials from.
	// Named generically, not after any specific backend (RustFS, MinIO,
	// AWS S3), since it's the same two-key contract regardless of which
	// one the datalake Connection's endpoint actually points at.
	S3CredentialsSecret string
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

// WorkerStatus is one Sync's combined live (Kubernetes Pod) and observed
// (workerd's own heartbeat/error reports, read back from internal/store)
// status, for the /workers API.
type WorkerStatus struct {
	SyncName string

	// PodPhase/Ready/Restarts come from the Kubernetes Pod itself —
	// "Missing" PodPhase means no Pod exists for this Sync at all (e.g.
	// still being scheduled, or workermgr hasn't reconciled it yet).
	PodPhase string
	Ready    bool
	Restarts int32

	// Alive reports whether workerd's own heartbeat is currently within
	// its TTL — this can be true even if the Pod's own readiness probe
	// says otherwise (or vice versa: a Pod can be Ready while workerd is
	// wedged and no longer heartbeating), which is exactly why both are
	// surfaced rather than collapsed into one status.
	Alive           bool
	ObservedPhase   string
	LastHeartbeatAt *time.Time
	ConsumerLag     *int64
	Watermark       string
	LastError       string
}

// ListWorkerStatus returns one WorkerStatus per desired Sync (not per
// ReplicaSet/Pod — a Sync with no Pod yet still gets an entry with
// PodPhase "Missing", so a caller can tell "not scheduled" apart from
// "no such Sync"), combining live Kubernetes Pod state with workerd's own
// observed state from internal/store.
func (m *Manager) ListWorkerStatus(ctx context.Context) ([]WorkerStatus, error) {
	desired, err := m.store.List(ctx, store.KindSync)
	if err != nil {
		return nil, fmt.Errorf("listing syncs: %w", err)
	}
	sort.Strings(desired)

	pods, err := m.clientset.CoreV1().Pods(m.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + appLabel,
	})
	if err != nil {
		return nil, fmt.Errorf("listing worker pods: %w", err)
	}
	podBySync := make(map[string]corev1.Pod, len(pods.Items))
	for _, p := range pods.Items {
		if sync := p.Labels["sync"]; sync != "" {
			podBySync[sync] = p
		}
	}

	statuses := make([]WorkerStatus, 0, len(desired))
	for _, name := range desired {
		ws := WorkerStatus{SyncName: name, PodPhase: "Missing"}

		if pod, ok := podBySync[name]; ok {
			ws.PodPhase = string(pod.Status.Phase)
			if len(pod.Status.ContainerStatuses) > 0 {
				cs := pod.Status.ContainerStatuses[0]
				ws.Ready = cs.Ready
				ws.Restarts = cs.RestartCount
			}
		}

		observed, alive, err := m.store.GetSyncObserved(ctx, name)
		if err != nil {
			m.log.Error("workermgr: getting observed sync state", "sync", name, "err", err)
		} else {
			ws.Alive = alive
			if observed != nil {
				ws.ObservedPhase = observed.Phase
				ws.LastError = observed.LastError
				ws.Watermark = observed.Watermark
				lag := observed.ConsumerLag
				ws.ConsumerLag = &lag
				if !observed.LastHeartbeatAt.IsZero() {
					at := observed.LastHeartbeatAt
					ws.LastHeartbeatAt = &at
				}
			}
		}

		statuses = append(statuses, ws)
	}
	return statuses, nil
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
							Env: []corev1.EnvVar{
								{
									Name: "S3_ACCESS_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: m.cfg.S3CredentialsSecret},
											Key:                  "access-key",
										},
									},
								},
								{
									Name: "S3_SECRET_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: m.cfg.S3CredentialsSecret},
											Key:                  "secret-key",
										},
									},
								},
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
