# select build image
FROM rust:1.83.0 AS build

# create a new empty shell project
RUN USER=root cargo new --bin my_project
WORKDIR /my_project

# copy your source tree
COPY ./src ./src
COPY ./Cargo.toml .

# build for release
RUN cargo build --release --all-features

# our final base
FROM debian:bookworm-slim

# copy the build artifact from the build stage
COPY --from=build /my_project/target/release/bgpkit-broker /usr/local/bin/bgpkit-broker

RUN apt update && apt install -y curl tini sqlite3
WORKDIR /bgpkit-broker

EXPOSE 40064
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/bgpkit-broker"]
CMD ["serve", "bgpkit-broker.sqlite3", "--bootstrap", "--silent"]
