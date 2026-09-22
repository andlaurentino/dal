GOBIN ?= $(shell go env GOPATH)/bin

# On macOS, `docker build` shells out to a credential helper (per
# ~/.docker/config.json's credsStore, typically "osxkeychain") to pull base
# images. Docker Desktop and Rancher Desktop both ship the helper binary,
# but their install locations aren't always on PATH for non-interactive
# shells (Makefiles included). Find it once here and prepend it for every
# docker invocation below.
DOCKER_EXTRA_PATH := $(shell for d in \
		"/Applications/Docker.app/Contents/Resources/bin" \
		"/Applications/Rancher Desktop.app/Contents/Resources/resources/darwin/bin" \
		"$$HOME/.docker/bin" \
		"/usr/local/bin"; do \
		[ -x "$$d/docker-credential-osxkeychain" ] && echo "$$d" && break; \
	done)
DOCKER := PATH="$(DOCKER_EXTRA_PATH):$$PATH" docker
KUBECTL := kubectl --context rancher-desktop

.PHONY: setup
setup: ## One-time local dev setup: toolchain checks, proto codegen, deps for every module
	@command -v protoc >/dev/null 2>&1 || { \
		echo "protoc not found. Install it first, e.g.: brew install protobuf"; exit 1; \
	}
	@test -x "$(GOBIN)/protoc-gen-go" || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@test -x "$(GOBIN)/protoc-gen-go-grpc" || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@command -v cargo >/dev/null 2>&1 || { \
		echo "cargo not found. Install Rust first, e.g.: https://rustup.rs (workerd needs rustc >= 1.94)"; exit 1; \
	}
	@command -v cmake >/dev/null 2>&1 || { \
		echo "cmake not found (needed to build workerd's vendored librdkafka), e.g.: brew install cmake"; exit 1; \
	}
	$(MAKE) proto
	$(MAKE) tidy
	cd workerd && cargo fetch
	cd web && npm install

.PHONY: build
build: ## Build the latest images (controlplane, broker, workerd, web)
	$(DOCKER) build -f controlplane/Dockerfile -t dal-controlplane:latest .
	$(DOCKER) build -f broker/Dockerfile -t dal-broker:latest .
	$(DOCKER) build -f workerd/Dockerfile -t dal-workerd:latest .
	$(DOCKER) build -f web/Dockerfile -t dal-web:latest web

.PHONY: up
up: ## Deploy manifests to Rancher Desktop's k8s (redis, kafka, minio, postgres, controlplane, broker, web; workers are spawned per-Sync by controlplaned); images must already exist — run `make build` first
	$(KUBECTL) create configmap postgres-init --from-file=infra/postgres/init --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) apply -f infra/k8s/
	$(KUBECTL) rollout status deployment/redis deployment/kafka deployment/minio deployment/postgres deployment/controlplane deployment/broker deployment/web --timeout=180s
	$(KUBECTL) apply -f infra/datalake/bucket-init-job.yaml
	$(KUBECTL) wait --for=condition=complete job/datalake-bucket-init --timeout=60s

.PHONY: down
down: ## Delete the stack from Kubernetes, including per-Sync worker ReplicaSets (add ARGS=--all to also drop PVC-stored data)
	$(KUBECTL) delete rs -l app=dal-worker --ignore-not-found
	$(KUBECTL) delete -f infra/datalake/bucket-init-job.yaml --ignore-not-found
	$(KUBECTL) delete -f infra/k8s/ --ignore-not-found
	$(KUBECTL) delete configmap postgres-init --ignore-not-found
	@if [ "$(ARGS)" = "--all" ]; then $(KUBECTL) delete pvc redis-data kafka-data minio-data postgres-data --ignore-not-found; fi

.PHONY: logs
logs: ## Tail logs from every DAL-managed pod (base services + per-Sync workers); use ARGS=sync=<name> to follow one worker
	$(KUBECTL) logs -f -l 'app in (redis,kafka,minio,postgres,controlplane,broker,web,dal-worker)' --all-containers --prefix --max-log-requests=20

.PHONY: run-producer
run-producer: ## Run the standalone event producer against Kafka's NodePort (localhost:30920); pass extra flags via ARGS, e.g. ARGS="--topic=user-events --interval=500ms"
	cd tools/producer && GOWORK=off go run ./cmd/producer --brokers=localhost:30920 $(ARGS)

CONTROLPLANE_URL ?= http://localhost:30800

.PHONY: apply-examples
apply-examples: ## POST every examples/<dir> resource to controlplane's REST API (connections, then syncs, then projections); pass ARGS=<dir> to apply just one, e.g. ARGS=tiered-events
	@dirs="$(ARGS)"; \
	if [ -z "$$dirs" ]; then dirs=$$(ls examples); fi; \
	for d in $$dirs; do \
		echo "==> examples/$$d"; \
		for f in examples/$$d/connection-*.yaml examples/$$d/sync.yaml examples/$$d/sync-*.yaml examples/$$d/projection.yaml examples/$$d/projection-*.yaml; do \
			[ -f "$$f" ] || continue; \
			printf '  applying %s ... ' "$$f"; \
			body=$$(curl -sS -o /tmp/dal-apply-response -w '%{http_code}' -X POST --data-binary @"$$f" "$(CONTROLPLANE_URL)/api/v1alpha1/apply"); \
			if [ "$$body" = "200" ]; then echo "ok"; else echo "FAILED ($$body)"; cat /tmp/dal-apply-response; echo; exit 1; fi; \
		done; \
	done

.PHONY: apply-tiered-seed
apply-tiered-seed: ## Apply the tiered-events example and seed historical data into the lake (postgres ends up recent-only after a forced prune, lake keeps full history)
	$(MAKE) apply-examples ARGS=tiered-events
	$(MAKE) seed-tiered-events

.PHONY: seed-tiered-events
seed-tiered-events: ## Publish backdated + recent user-events so events-to-postgres/events-to-lake visibly diverge (postgres recent-only, lake full history); run via apply-tiered-seed
	@echo "==> seeding historical + recent data for tiered-events"
	@$(KUBECTL) exec deploy/kafka -- rpk topic create user-events --brokers localhost:9092 >/dev/null 2>&1 || true
	cd tools/producer && GOWORK=off go run ./cmd/producer --brokers=localhost:30920 --topic=user-events --count=200 --interval=0 --backdate=720h --backdate-jitter=1440h
	cd tools/producer && GOWORK=off go run ./cmd/producer --brokers=localhost:30920 --topic=user-events --count=30 --interval=0
	@echo "  waiting for the events-to-postgres worker pod so its retention prune can be forced to run immediately..."
	@for i in $$(seq 1 15); do \
		pod=$$($(KUBECTL) get pods -l sync=events-to-postgres -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); \
		[ -n "$$pod" ] && break; \
		sleep 1; \
	done; \
	if [ -n "$$pod" ]; then \
		echo "  restarting $$pod to force an immediate retention prune"; \
		$(KUBECTL) delete pod -l sync=events-to-postgres --wait=false; \
	else \
		echo "  warning: no events-to-postgres worker pod found yet — skipping forced prune (it'll prune on its normal hourly schedule)"; \
	fi

.PHONY: proto
proto:
	PATH="$(GOBIN):$$PATH" protoc \
		--proto_path=core/proto \
		--go_out=core/proto/gen --go_opt=paths=source_relative \
		--go-grpc_out=core/proto/gen --go-grpc_opt=paths=source_relative \
		core/proto/controlplane/v1/worker_manager.proto \
		core/proto/controlplane/v1/query_plan.proto

.PHONY: test
test:
	cd core && go test ./...
	cd controlplane && go test ./...
	cd broker && go test ./...
	cd workerd && cargo test

.PHONY: tidy
tidy:
	cd core && go mod tidy
	cd controlplane && go mod tidy
	cd broker && go mod tidy
