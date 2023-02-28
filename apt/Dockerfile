FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y && \
    apt-get install -y bash build-essential ca-certificates cmake git lsb-release wget && \
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt update && \
    apt install -y -V libarrow-dev && \
    apt install -y -V libarrow-glib-dev && \
    apt install -y -V libarrow-dataset-dev && \
    apt install -y -V libarrow-dataset-glib-dev && \
    apt install -y -V libarrow-flight-dev && \
    apt install -y -V libarrow-flight-glib-dev && \
    apt install -y -V libparquet-dev && \
    rm -rf /var/lib/apt/lists/*


# Build and install parquet2csv
ENV PARQUET2CSV_BUILD_DIR=$SRC_DIR/parquet2csv/build
ENV VERSION=main
RUN git clone --depth 1 -b "$VERSION" https://github.com/lsst-dm/parquet2csv.git $SRC_DIR/parquet2csv

RUN mkdir $PARQUET2CSV_BUILD_DIR
WORKDIR $PARQUET2CSV_BUILD_DIR

RUN cmake .. && \
  make -j8 && \
  make install
