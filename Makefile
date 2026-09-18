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
	$(MAKE) proto
	$(MAKE) tidy
	cd web && npm install

.PHONY: up
up: ## Build images and apply manifests to Rancher Desktop's k8s (redis, kafka, postgres, controlplane, broker, web; workers are spawned per-Sync by controlplaned)
	$(DOCKER) build -f controlplane/Dockerfile -t dal-controlplane:latest .
	$(DOCKER) build -f broker/Dockerfile -t dal-broker:latest .
	$(DOCKER) build -f web/Dockerfile -t dal-web:latest web
	$(KUBECTL) create configmap postgres-init --from-file=infra/postgres/init --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) apply -f infra/k8s/
	$(KUBECTL) rollout status deployment/redis deployment/kafka deployment/postgres deployment/controlplane deployment/broker deployment/web --timeout=180s

.PHONY: down
down: ## Delete the stack from Kubernetes (add ARGS=--all to also drop PVC-stored data)
	$(KUBECTL) delete -f infra/k8s/ --ignore-not-found
	$(KUBECTL) delete configmap postgres-init --ignore-not-found
	@if [ "$(ARGS)" = "--all" ]; then $(KUBECTL) delete pvc redis-data kafka-data postgres-data --ignore-not-found; fi

.PHONY: logs
logs: ## Tail logs from every DAL-managed pod (base services + per-Sync workers); use ARGS=sync=<name> to follow one worker
	$(KUBECTL) logs -f -l 'app in (redis,kafka,postgres,controlplane,broker,web,dal-worker)' --all-containers --prefix --max-log-requests=20

.PHONY: run-producer
run-producer: ## Run the standalone event producer against Kafka's NodePort (localhost:30920); pass extra flags via ARGS, e.g. ARGS="--topic=user-events --interval=500ms"
	cd tools/producer && GOWORK=off go run ./cmd/producer --brokers=localhost:30920 $(ARGS)

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
	cd worker && go test ./...
	cd broker && go test ./...

.PHONY: tidy
tidy:
	cd core && go mod tidy
	cd controlplane && go mod tidy
	cd worker && go mod tidy
	cd broker && go mod tidy
