FROM docker.io/library/rust:1-bookworm AS build
ARG CARGO_FEATURES=""
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo test --release --locked --features="$CARGO_FEATURES" \
 && cargo build --release --locked --features="$CARGO_FEATURES"

FROM docker.io/library/python:3.12-slim
COPY --from=build /src/target/release/kvreap /usr/local/bin/kvreap
CMD ["kvreap"]
