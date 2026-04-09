# Stage 1: Build
FROM rust:1.88-slim AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies: copy manifests first, create dummy src, build deps
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release || true
RUN rm -rf src

# Copy actual source and migrations, then build
COPY src/ src/
COPY migrations/ migrations/
RUN touch src/main.rs && cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/indexer /usr/local/bin/indexer
COPY assets/ /assets/

EXPOSE 4000

ENTRYPOINT ["indexer"]
