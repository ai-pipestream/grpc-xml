# Build stage: compile and run the whole suite. An image never ships from a
# tree whose tests did not pass, so `cargo test` is a build step and not a
# separate CI job that an image could be built around.
FROM rust:1.98-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      protobuf-compiler libprotobuf-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo test --release --locked \
    && cargo build --release --locked --bin grpc-xml \
    && strip target/release/grpc-xml

# Runtime: the binary and nothing else. No shell is needed to run it, but
# debian-slim keeps one available for an operator, matching the fleet's other
# Rust images; the process itself never execs anything.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin grpcxml
COPY --from=build /src/target/release/grpc-xml /usr/local/bin/grpc-xml

# The server is diskless by doctrine: document bytes live in memory between
# the request stream and the parser, nothing is written, and no library
# spills. `docker run --read-only` therefore works with no tmpfs mount at all
# — the compose stack runs it that way and the parse tests cover the paths.
USER grpcxml
EXPOSE 50051
ENV GRPC_XML_ADDR=0.0.0.0:50051

# grpc.health.v1.Health is registered, so an orchestrator probes the service
# rather than the port. There is no HTTP endpoint and no curl in the image;
# use grpc_health_probe or the runtime's native gRPC probe.
ENTRYPOINT ["/usr/local/bin/grpc-xml"]
