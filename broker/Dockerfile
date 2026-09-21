# Build context is the repo root (see Makefile) so this module's go.work
# replace directive to ../core resolves during the build.
#
# CGO_ENABLED=1 (not the usual Go static-binary default): the lake executor
# (broker/internal/executors/lake) uses go-duckdb, a CGO binding around
# DuckDB's C++ core — there's no pure-Go Delta Lake reader mature enough to
# avoid this, same reasoning that pushed workerd's write path to Rust. A
# glibc base is needed for both stages accordingly, same tradeoff workerd
# already made for rdkafka.
FROM golang:1.27-bookworm AS build
WORKDIR /src

# Note: go.work is deliberately not copied — the module builds in plain
# module mode using its own go.mod replace directive to ../core.
COPY core/ core/
COPY broker/ broker/

RUN cd broker && CGO_ENABLED=1 go build -o /out/brokerd ./cmd/brokerd

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/brokerd /usr/local/bin/brokerd

EXPOSE 8081
ENTRYPOINT ["brokerd"]
