FROM gitlab-registry.in2p3.fr/qserv/parquet2csv/arrow:11.0.0-1

ENV DEBIAN_FRONTEND noninteractive

# Build and install parquet2csv
ENV PARQUET2CSV_BUILD_DIR=/opt/parquet2csv/build
ADD . /opt/parquet2csv


RUN mkdir $PARQUET2CSV_BUILD_DIR
WORKDIR $PARQUET2CSV_BUILD_DIR

RUN cmake .. && \
    make -j8
