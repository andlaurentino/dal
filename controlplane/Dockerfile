# Build context is the repo root (see Makefile) so this module's go.work
# replace directive to ../core resolves during the build. workerd is a
# separate Rust binary/image (workerd/Dockerfile) — workermgr schedules it
# by image name only, so it no longer needs to be bundled here.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Note: go.work is deliberately not copied — the module builds in plain
# module mode using its own go.mod replace directive to ../core.
COPY core/ core/
COPY controlplane/ controlplane/

RUN cd controlplane && CGO_ENABLED=0 go build -o /out/controlplaned ./cmd/controlplaned

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/controlplaned /usr/local/bin/controlplaned

EXPOSE 8080 9090
ENTRYPOINT ["controlplaned"]
