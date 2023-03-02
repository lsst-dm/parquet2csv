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

```shell
# define ARROW_HOME in order for cmake to find the arrow and parquet libraries<br>
mkdir build
cd build
cmake ..
make
./test -f parquet_data_file_name
```

## Download data and configurations files related to the dp02 dataset

This files are available here:
https://github.com/lsst-dm/parquet2csv/releases/download/2022.03.03/data-2023.03.03.zip

```shell
wget https://github.com/lsst-dm/parquet2csv/releases/download/2022.03.03/data-2023.03.03.zip
unzip data-2023.03.03.zip
```

## Read parquet file and save it as CSV file

```shell
./reader/parquet_reader -in ../data/calibratedSourceTable_visit.parquet -config ../data/PREOPS-863/calSourceTable_visit/configs/schema.abh -format csv -out calibSource
```

## Socket

Launch the listener.py process defined in the listener directory

```shell
./reader/parquet_reader  -in ../data/calibratedSourceTable_visit.parquet -config ../data/PREOPS-863/calSourceTable_visit/configs/schema.abh -format stream_socket
```

# Fifo

```shell
cat</tmp/test_fifo
./reader/parquet_reader  -in ../data/calibratedSourceTable_visit.parquet -config ../data/PREOPS-863/calSourceTable_visit/configs/schema.abh -format fifo
```

# Arrow example - how to stream a file between 2 processes

https://gist.github.com/westonpace/f5657117d7121b84c1356adec350fdb2
