# Build context is the repo root (see docker-compose.yml) so this module's
# go.work-based replace directive to ../core resolves during the build.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Note: go.work is deliberately not copied — the module builds in plain
# module mode using its own go.mod replace directive to ../core.
COPY core/ core/
COPY broker/ broker/

RUN cd broker && CGO_ENABLED=0 go build -o /out/brokerd ./cmd/brokerd

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/brokerd /usr/local/bin/brokerd

EXPOSE 8081
ENTRYPOINT ["brokerd"]
