FROM docker.io/library/rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo test --release --locked && cargo build --release --locked

FROM docker.io/library/python:3.12-slim
COPY --from=build /src/target/release/kvreap /usr/local/bin/kvreap
CMD ["kvreap"]
