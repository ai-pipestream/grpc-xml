# Build stage: compile and run the whole suite. An image never ships from a
# tree whose tests did not pass, so `cargo test` is a build step and not a
# separate CI job that an image could be built around.
FROM dhi.io/rust:1-dev AS build
# The dev variant of the hardened toolchain image: it carries apt (needed for
# protoc below) and runs as root, where the plain dhi.io/rust:1 runtime-style
# image has no package manager at all.
RUN apt-get update && apt-get install -y --no-install-recommends \
      protobuf-compiler libprotobuf-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo test --release --locked \
    && cargo build --release --locked --bin grpc-xml \
    && strip target/release/grpc-xml

# Runtime: the binary and nothing else. Docker Hardened Images debian-base:
# glibc and libgcc, no package manager, pulls from the docker.io ecosystem
# (dhi.io) with signed provenance, and runs as uid 65532 out of the box. The
# process itself never execs anything, so no shell tooling is needed.
FROM dhi.io/debian-base:trixie-debian13
COPY --from=build /src/target/release/grpc-xml /usr/local/bin/grpc-xml

# The server is diskless by doctrine: document bytes live in memory between
# the request stream and the parser, nothing is written, and no library
# spills. `docker run --read-only` therefore works with no tmpfs mount at all
# — the compose stack runs it that way and the parse tests cover the paths.
USER nonroot
EXPOSE 50066
ENV GRPC_XML_ADDR=0.0.0.0:50066

# grpc.health.v1.Health is registered, so an orchestrator probes the service
# rather than the port. There is no HTTP endpoint and no curl in the image;
# use grpc_health_probe or the runtime's native gRPC probe.
ENTRYPOINT ["/usr/local/bin/grpc-xml"]
