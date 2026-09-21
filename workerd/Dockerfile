# Build context is the repo root (see Makefile), matching controlplane/broker's
# Dockerfiles, so build.rs's "../core/proto/..." path resolves identically
# whether building locally (cargo build from workerd/) or in this image
# (core/ and workerd/ copied in as siblings under /src). A glibc base is
# used rather than Alpine/musl for both stages: rdkafka's librdkafka C
# dependency is a known pain point to get working against musl.
# deltalake's transitive aws-sdk/aws-smithy deps push the effective MSRV to
# ~1.94; "1-bookworm" tracks the latest stable 1.x release rather than
# pinning a version that will go stale below that floor.
FROM rust:1-slim-bookworm AS build
WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config libssl-dev protobuf-compiler libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY core/ core/
COPY workerd/ workerd/

RUN cd workerd && cargo build --release

FROM debian:bookworm-slim
# libcurl4: librdkafka links against it even with WITH_CURL=0 (used
# unconditionally for OAUTHBEARER/OIDC token fetching support).
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libcurl4 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/workerd/target/release/workerd /usr/local/bin/workerd

ENTRYPOINT ["workerd"]
