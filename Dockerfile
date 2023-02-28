FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y && \
    apt-get install -y bash build-essential cmake git && \
    rm -rf /var/lib/apt/lists/*

ENV SRC_DIR=/root

RUN git config --global advice.detachedHead false

# Build and install RapidJson
ENV RAPIDJSON_VERSION=v1.1.0
RUN git clone --depth 1 -b "$RAPIDJSON_VERSION" https://github.com/Tencent/rapidjson

# Build and install Arrow
ENV ARROW_VERSION=apache-arrow-11.0.0
ENV ARROW_HOME=/usr/local
ENV ARROW_BUILD_DIR=$SRC_DIR/arrow/cpp/build
ENV CMAKE_MODULE_PATH=$SRC_DIR/arrow/cpp/cmake_modules
RUN git clone --depth 1 -b "$ARROW_VERSION" https://github.com/apache/arrow.git $SRC_DIR/arrow

RUN mkdir $ARROW_BUILD_DIR
WORKDIR $ARROW_BUILD_DIR

RUN cmake .. \
  -DSPEEX_PATH=$SRC_DIR/rapidjson/include \
  -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
  -DARROW_CSV=ON \
  -DARROW_PARQUET=ON \
  -DARROW_BUILD_EXAMPLES=ON \
  -DPARQUET_BUILD_EXAMPLES=ON \
  -DARROW_WITH_SNAPPY=ON && \
  make -j8 && \
  make install


# Build and install parquet2csv
ENV PARQUET2CSV_BUILD_DIR=$SRC_DIR/parquet2csv/build
ENV VERSION=main
RUN git clone --depth 1 -b "$VERSION" https://github.com/lsst-dm/parquet2csv.git $SRC_DIR/parquet2csv

RUN mkdir $PARQUET2CSV_BUILD_DIR
WORKDIR $PARQUET2CSV_BUILD_DIR

RUN cmake .. \
  make -j8 && \
  make install
