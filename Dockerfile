# select build image
FROM rust:1.70 as build

# create a new empty shell project
RUN USER=root cargo new --bin my_project
WORKDIR /my_project

# copy your source tree
COPY ./src ./src
COPY ./Cargo.toml .
COPY ./Cargo.lock .

# build for release
RUN cargo build --release

# our final base
FROM debian:bullseye

# copy the build artifact from the build stage
COPY --from=build /my_project/target/release/bgpkit-broker /usr/local/bin/bgpkit-broker
RUN DEBIAN=NONINTERACTIVE apt update; apt install -y curl libssl-dev ca-certificates tzdata cron; rm -rf /var/lib/apt/lists/*

ENTRYPOINT bash -c '/usr/local/bin/bgpkit-broker serve'