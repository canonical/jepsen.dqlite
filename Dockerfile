FROM ubuntu:jammy

ARG GO_VERSION=1.21.0

RUN apt-get update
RUN apt-get install --yes --no-install-recommends \
  automake \
  build-essential \
  curl \
  git \
  gnuplot-nox \
  graphviz \
  iproute2 \
  iptables \
  leiningen \
  libjna-java \
  liblz4-dev \
  libsqlite3-dev \
  libtool \
  libuv1-dev \
  pkg-config \
  sudo

WORKDIR /root
RUN git clone --depth 1 https://github.com/ianlancetaylor/libbacktrace

WORKDIR /root/libbacktrace
RUN autoreconf -i
RUN ./configure
RUN make install

WORKDIR /root
RUN rm -rf libbacktrace
RUN curl -L -o go.tar.gz https://go.dev/dl/go$GO_VERSION.linux-amd64.tar.gz
RUN tar xzf go.tar.gz
RUN mv go /usr/local
RUN rm -f go.tar.gz
