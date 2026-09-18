// Command workerd is spawned by the control plane, one process per Sync. It
// fetches its Sync's spec (and the source/target Connections it references)
// from the control plane's REST API, then runs the matching sync-type
// implementation (only kafka->postgres exists so far), reporting health back
// via the heartbeat client throughout.
package main

import (
	"context"
	"flag"
	"os"

	v1alpha1 "github.com/andersonlaurentino/dal-core/api/v1alpha1"
	"github.com/andersonlaurentino/dal-core/logging"
	"github.com/andersonlaurentino/dal-worker/internal/cpclient"
	"github.com/andersonlaurentino/dal-worker/internal/heartbeat"
	"github.com/andersonlaurentino/dal-worker/internal/kafkapg"
)

func main() {
	syncID := flag.String("sync-id", "", "name of the Sync resource this worker is running")
	controlplaneAddr := flag.String("controlplane-addr", "localhost:9090", "control plane gRPC address")
	controlplaneHTTPAddr := flag.String("controlplane-http-addr", "http://localhost:8080", "control plane REST API address")
	flag.Parse()

	log := logging.New("worker")
	if *syncID == "" {
		log.Error("--sync-id is required")
		os.Exit(1)
	}

	client, err := heartbeat.Dial(*controlplaneAddr)
	if err != nil {
		log.Error("failed to dial control plane", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.Register(ctx, *syncID, int64(os.Getpid())); err != nil {
		log.Error("failed to register with control plane", "err", err)
		os.Exit(1)
	}
	log.Info("worker registered", "sync", *syncID)

	sync, source, target, err := resolveSync(ctx, *controlplaneHTTPAddr, *syncID)
	if err != nil {
		log.Error("failed to resolve sync", "sync", *syncID, "err", err)
		_ = client.ReportError(ctx, *syncID, err.Error(), true)
		os.Exit(1)
	}

	err = kafkapg.Run(ctx, kafkapg.Config{
		SyncName:  *syncID,
		Sync:      *sync,
		Source:    *source,
		Target:    *target,
		Heartbeat: client,
		Log:       log,
	})
	if err != nil {
		log.Error("worker stopped", "sync", *syncID, "err", err)
		_ = client.ReportError(ctx, *syncID, err.Error(), true)
		os.Exit(1)
	}
}

func resolveSync(ctx context.Context, controlplaneHTTPAddr, syncID string) (*v1alpha1.SyncSpec, *v1alpha1.ConnectionSpec, *v1alpha1.ConnectionSpec, error) {
	sync, err := cpclient.GetSync(ctx, controlplaneHTTPAddr, syncID)
	if err != nil {
		return nil, nil, nil, err
	}
	source, err := cpclient.GetConnection(ctx, controlplaneHTTPAddr, sync.Source.ConnectionRef)
	if err != nil {
		return nil, nil, nil, err
	}
	target, err := cpclient.GetConnection(ctx, controlplaneHTTPAddr, sync.Target.ConnectionRef)
	if err != nil {
		return nil, nil, nil, err
	}
	return sync, source, target, nil
}
