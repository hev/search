# Multi-stage build for the hevsearch-api binary.
#
# Stage 1 compiles the binary against `rust:1.94-bookworm` with the
# same protobuf-compiler + libprotobuf-dev layer `Dockerfile.dev`
# installs (lance-encoding / lance-file need both at build time).
#
# Stage 2 is a minimal `debian:bookworm-slim` with just `ca-certificates`
# installed so the binary can talk to S3 over TLS. The release binary
# is self-contained otherwise (statically links everything except
# glibc).
#
# Build with:  docker build -t hevsearch-api .
# Or via compose: `docker compose up --build hevsearch`

FROM rust:1.94-bookworm AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the whole workspace so cargo can see every member crate.
# A docker bind mount / .dockerignore keeps target/ and
# .cargo-cache/ out of the build context.
COPY . .

RUN cargo build --release -p hevsearch-api

# --- runtime ---
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/hevsearch-api /usr/local/bin/hevsearch-api

# foyer NVMe tier needs a writable directory; default to
# /var/lib/hevsearch inside the container and surface it via the
# default `HEVSEARCH_CACHE_NVME_PATH`.
RUN mkdir -p /var/lib/hevsearch/cache
ENV HEVSEARCH_CACHE_NVME_PATH=/var/lib/hevsearch/cache
ENV HEVSEARCH_BIND=0.0.0.0:3000
ENV RUST_LOG=info

EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/hevsearch-api"]
