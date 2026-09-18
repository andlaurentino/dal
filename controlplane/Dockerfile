# Build context is the repo root (see docker-compose.yml) because this
# image bundles both controlplaned and workerd: the control plane execs
# workerd as a local subprocess per Sync (internal/workermgr's execSpawner),
# so the two binaries must live in the same image/filesystem.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Note: go.work is deliberately not copied — each module builds in plain
# module mode using its own go.mod replace directive to ../core, so the
# build doesn't need Go's workspace mode to resolve local siblings.
COPY core/ core/
COPY controlplane/ controlplane/
COPY worker/ worker/

RUN cd controlplane && CGO_ENABLED=0 go build -o /out/controlplaned ./cmd/controlplaned
RUN cd worker && CGO_ENABLED=0 go build -o /out/workerd ./cmd/workerd

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/controlplaned /usr/local/bin/controlplaned
COPY --from=build /out/workerd /usr/local/bin/workerd

EXPOSE 8080 9090
ENTRYPOINT ["controlplaned"]
