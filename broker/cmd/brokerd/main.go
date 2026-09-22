// Command brokerd resolves and executes queries against the store selected
// by the control plane's query planner. It is explicitly not a query
// engine: no cross-store joins or aggregation, just resolve-and-fetch.
package main

import (
	"flag"
	"net/http"

	"github.com/andersonlaurentino/dal-broker/internal/grpcclient"
	"github.com/andersonlaurentino/dal-broker/internal/httpapi"
	"github.com/andersonlaurentino/dal-core/logging"
)

func main() {
	httpAddr := flag.String("http-addr", ":8081", "address for the public HTTP query API")
	controlplaneAddr := flag.String("controlplane-addr", "localhost:9090", "control plane gRPC address")
	flag.Parse()

	log := logging.New("broker")

	planner, err := grpcclient.Dial(*controlplaneAddr)
	if err != nil {
		log.Error("failed to dial control plane", "err", err)
		return
	}
	defer planner.Close()

	handler, err := httpapi.New(planner, log)
	if err != nil {
		log.Error("failed to initialize http api", "err", err)
		return
	}
	mux := http.NewServeMux()
	handler.Routes(mux)

	log.Info("broker HTTP server listening", "addr", *httpAddr)
	if err := http.ListenAndServe(*httpAddr, mux); err != nil {
		log.Error("HTTP server stopped", "err", err)
	}
}
