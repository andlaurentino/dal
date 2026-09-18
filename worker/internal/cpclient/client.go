// Package cpclient is a thin HTTP client workerd uses to fetch its own
// Sync's spec and the source/target Connections it references from the
// control plane's REST API (controlplane/internal/httpapi), since the gRPC
// WorkerManager service only carries heartbeat/registration traffic.
package cpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	v1alpha1 "github.com/andersonlaurentino/dal-core/api/v1alpha1"
)

type envelope struct {
	Spec json.RawMessage `json:"spec"`
}

func get(ctx context.Context, baseURL, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: unexpected status %d", path, res.StatusCode)
	}

	var env envelope
	if err := json.NewDecoder(res.Body).Decode(&env); err != nil {
		return fmt.Errorf("GET %s: decoding response: %w", path, err)
	}
	if err := json.Unmarshal(env.Spec, out); err != nil {
		return fmt.Errorf("GET %s: decoding spec: %w", path, err)
	}
	return nil
}

// GetSync fetches a Sync's spec by name.
func GetSync(ctx context.Context, baseURL, name string) (*v1alpha1.SyncSpec, error) {
	var spec v1alpha1.SyncSpec
	if err := get(ctx, baseURL, "/api/v1alpha1/syncs/"+name, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

// GetConnection fetches a Connection's spec by name.
func GetConnection(ctx context.Context, baseURL, name string) (*v1alpha1.ConnectionSpec, error) {
	var spec v1alpha1.ConnectionSpec
	if err := get(ctx, baseURL, "/api/v1alpha1/connections/"+name, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}
