# Standalone module — no dependency on the rest of the repo, so the build
# context is just this directory.
FROM golang:1.27-alpine AS build
WORKDIR /src

COPY . .
RUN CGO_ENABLED=0 go build -o /out/producer ./cmd/producer

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/producer /usr/local/bin/producer

ENTRYPOINT ["producer"]
