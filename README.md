# parquet2csv
Parquet to CSV converter

Arrow installation (Ubuntu 20.04)

In order to compile the arrow examples, one has to install the rapidjson package
git clone https://github.com/Tencent/rapidjson

Modify the arrow/cpp/examples/arrow/CMakeLists.txt to define where the rapdjson include files are available
include_directories(~/Workspace/LSST/partition/install_rapidjson/include)

Arrow & parquet libraries installation

git clone https://github.com/apache/arrow.git

define ARROW_HOME as the directory in which arrow is going to be installed
export CMAKE_MODULE_PATH=<blablabla>/arrow/arrow-dir/cpp/cmake_modules

mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$ARROW_HOME -DARROW_CSV=ON -DARROW_PARQUET=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_WITH_SNAPPY=ON
make -j8
make install


Arrow/Parquet interface - read a parquet file as blocks of rows 

define ARROW_HOME in order for cmake to find the arrow and parquet libraries 
mkdir build; cd build;
cmake ..
make
./test -f parquet_data_file_name

Data and configurations files related to the dp02 dataset are available here: 
https://mydrive.lapp.in2p3.fr/s/2qrb4XBGaaqqfsj

On going (Feb 2023) : dev of the process that checks that the values read from the parquet file are well defined ( no null, Nan, inf, true, false, ....)

Arrow example - how to stream a file between 2 processes
https://gist.github.com/westonpace/f5657117d7121b84c1356adec350fdb2
