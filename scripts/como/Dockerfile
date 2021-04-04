FROM debian:latest
LABEL maintainer="ruslanti@gmail.com"
COPY *.deb .
RUN apt-get update && apt-get install -y ./como_0.1.0_amd64.deb && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/como
ENTRYPOINT ["/usr/bin/como"]
