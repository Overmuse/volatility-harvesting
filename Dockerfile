FROM rust as planner
WORKDIR volatility-harvesting
# We only pay the installation cost once, 
# it will be cached from the second build onwards
# To ensure a reproducible build consider pinning 
# the cargo-chef version with `--version X.X.X`
RUN cargo install cargo-chef 

COPY . .

RUN cargo chef prepare  --recipe-path recipe.json

FROM rust as cacher
WORKDIR volatility-harvesting
RUN cargo install cargo-chef
COPY --from=planner /volatility-harvesting/recipe.json recipe.json
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

FROM rust as builder
WORKDIR volatility-harvesting
COPY . .
# Copy over the cached dependencies
COPY --from=cacher /volatility-harvesting/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN --mount=type=ssh cargo build --release --bin volatility-harvesting

FROM debian:buster-slim as runtime
WORKDIR volatility-harvesting
COPY --from=builder /volatility-harvesting/target/release/volatility-harvesting /usr/local/bin
ENV RUST_LOG=volatility_harvesting=debug
RUN apt-get update && apt-get -y install ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/local/bin/volatility-harvesting"]
