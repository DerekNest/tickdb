# tickdb-server container image.
#
# Multi-stage build: a full rust toolchain compiles the release binary,
# then only the binary (not the toolchain, not the source tree) ships in
# the final image.
#
# NOTE: this Dockerfile has not been built/run in the environment that
# produced it (no docker available there) — it's written to match the CLI
# surface verified live against the real `tickdb-server` binary (see
# docs/DESIGN.md's Phase 3 section), but should be treated as a first
# draft until you've run `docker compose build` yourself.

FROM rust:1.96-bookworm AS builder

WORKDIR /build

# Copy the whole workspace — crates reference each other via path deps, so
# a partial copy would break dependency resolution.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build only the server binary and its dependency graph (not benches, not
# dev-dependencies like proptest/criterion — those never get built here).
RUN cargo build --release -p server --bin tickdb-server

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/tickdb-server /usr/local/bin/tickdb-server

# Data and Raft state live here; docker-compose.yml mounts a named volume
# at this path so data survives container restarts (needed for the chaos
# test's "kill the leader, restart it, confirm it catches up" step).
VOLUME /data

ENTRYPOINT ["/usr/local/bin/tickdb-server"]
