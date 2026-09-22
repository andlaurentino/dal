// Command controlplaned is the DAL control plane: it receives Connection/
// Sync/Projection resource definitions over HTTP, tracks their desired
// state in Redis, exposes a gRPC server for workers (health/heartbeat) and
// the broker (query plan resolution), and runs internal/workermgr to keep
// one Kubernetes ReplicaSet running per Sync.
package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"

	"github.com/andersonlaurentino/dal-controlplane/internal/grpcserver"
	"github.com/andersonlaurentino/dal-controlplane/internal/httpapi"
	"github.com/andersonlaurentino/dal-controlplane/internal/store"
	"github.com/andersonlaurentino/dal-controlplane/internal/validate"
	"github.com/andersonlaurentino/dal-controlplane/internal/workermgr"
	"github.com/andersonlaurentino/dal-core/logging"
	controlplanev1 "github.com/andersonlaurentino/dal-core/proto/gen/controlplane/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	httpAddr := flag.String("http-addr", ":8080", "address for the external HTTP CRUD API")
	grpcAddr := flag.String("grpc-addr", ":9090", "address for the internal gRPC server (worker/broker facing)")
	workerImage := flag.String("worker-image", os.Getenv("WORKER_IMAGE"), "image to run for each per-Sync worker ReplicaSet (must bundle workerd)")
	workerControlplaneAddr := flag.String("worker-controlplane-addr", "controlplane:9090", "gRPC address workers should use to reach this control plane")
	workerControlplaneHTTPAddr := flag.String("worker-controlplane-http-addr", "http://controlplane:8080", "REST address workers should use to reach this control plane")
	flag.Parse()

	log := logging.New("controlplane")

	if *workerImage == "" {
		log.Error("--worker-image (or WORKER_IMAGE) is required")
		os.Exit(1)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	st := store.New(rdb)
	validator := validate.New(st)

	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Error("failed to load in-cluster Kubernetes config (is this running in a pod with a ServiceAccount?)", "err", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.Error("failed to build Kubernetes clientset", "err", err)
		os.Exit(1)
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	s3CredentialsSecret := os.Getenv("S3_CREDENTIALS_SECRET")
	if s3CredentialsSecret == "" {
		s3CredentialsSecret = "s3-credentials"
	}

	workers := workermgr.New(clientset, st, workermgr.Config{
		Namespace:            namespace,
		Image:                *workerImage,
		ControlplaneAddr:     *workerControlplaneAddr,
		ControlplaneHTTPAddr: *workerControlplaneHTTPAddr,
		S3CredentialsSecret:  s3CredentialsSecret,
	}, log)
	go workers.Start(context.Background())

	go func() {
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			log.Error("failed to listen (grpc)", "addr", *grpcAddr, "err", err)
			os.Exit(1)
		}
		grpcServer := grpc.NewServer()
		srv := grpcserver.New(st, log)
		controlplanev1.RegisterWorkerManagerServer(grpcServer, srv)
		controlplanev1.RegisterQueryPlannerServer(grpcServer, srv)

		log.Info("controlplane gRPC server listening", "addr", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("gRPC server stopped", "err", err)
		}
	}()

	mux := http.NewServeMux()
	httpapi.New(st, validator, workers, log).Routes(mux)

	log.Info("controlplane HTTP server listening", "addr", *httpAddr, "redis", redisAddr)
	if err := http.ListenAndServe(*httpAddr, mux); err != nil {
		log.Error("HTTP server stopped", "err", err)
	}
}
