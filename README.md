# parquet2csv

Parquet to CSV converter

## Arrow installation (Ubuntu 20.04)

In order to compile the arrow examples, one has to install the rapidjson package

```shell
git clone https://github.com/Tencent/rapidjson
```

Modify the `arrow/cpp/examples/arrow/CMakeLists.txt` to define where the rapdjson include files are available
`include_directories(~/Workspace/LSST/partition/install_rapidjson/include)`

## Arrow & parquet libraries installation

```shell
git clone https://github.com/apache/arrow.git

# define ARROW_HOME as the directory in which arrow is going to be installed<br>
export CMAKE_MODULE_PATH=arrow_source_code_directory/arrow/arrow-dir/cpp/cmake_modules

mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$ARROW_HOME -DARROW_CSV=ON -DARROW_PARQUET=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_WITH_SNAPPY=ON
make -j8
make install
```

# Arrow/Parquet interface - read a parquet file as blocks of rows

## Build

```shell
# define ARROW_HOME in order for cmake to find the arrow and parquet libraries<br>
mkdir build
cd build
# Use -DCMAKE_INSTALL_PREFIX:PATH=<PATH> to change install directory
# Default to /usr/local/bin
cmake ..
make
sudo make install
```

## Download data and configurations files related to the dp02 dataset

This files are available here:
https://github.com/lsst-dm/parquet2csv/releases/download/2022.03.03/data-2023.03.03.zip

```shell
curl -LO https://github.com/lsst-dm/parquet2csv/releases/download/2022.03.03/data-2023.03.03.zip
unzip data-2023.03.03.zip
```

## Read parquet file and save it as CSV file

```shell
parquet_reader -infile data/calibratedSourceTable_visit.parquet \
  -config data/PREOPS-863/calSourceTable_visit/configs/schema.abh \
  -output csv -outfile calibSource
```

## Socket

Launch the socket listener python script before reading the parquet file :

```shell
python3 /usr/local/bin/listener_socket.py
parquet_reader -infile data/calibratedSourceTable_visit.parquet \
  -config data/PREOPS-863/calSourceTable_visit/configs/schema.abh \
  -output stream_socket
```

# Fifo

```shell
mkfifo /tmp/test_fifo
cat < /tmp/test_fifo
parquet_reader -infile data/calibratedSourceTable_visit.parquet \
  -config data/PREOPS-863/calSourceTable_visit/configs/schema.abh \
  -output fifo
```

# Arrow example - how to stream a file between 2 processes

https://gist.github.com/westonpace/f5657117d7121b84c1356adec350fdb2
