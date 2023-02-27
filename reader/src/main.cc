  // Licensed to the Apache Software Foundation (ASF) under one
  // or more contributor license agreements. See the NOTICE file
  // distributed with this work for additional information
  // regarding copyright ownership. The ASF licenses this file
  // to you under the Apache License, Version 2.0 (the
  // "License"); you may not use this file except in compliance
  // with the License. You may obtain a copy of the License at
  //
  // http://www.apache.org/licenses/LICENSE-2.0

  // Unless required by applicable law or agreed to in writing,
  // software distributed under the License is distributed on an
  // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  // KIND, either express or implied. See the License for the
  // specific language governing permissions and limitations
  // under the License.

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

#include "partition_config.h"
#include "parquet_read_file.h"
#include "streaming_socket.h"
#include "csv_dump_file.h"
#include "ipc_dump_file.h"

class InputParser{
    public:
        InputParser (int &argc, char **argv){
            for (int i=1; i < argc; ++i)
                this->tokens.push_back(std::string(argv[i]));
        }
        /// @author iain
        const std::string& getCmdOption(const std::string &option) const{
            std::vector<std::string>::const_iterator itr;
            itr =  std::find(this->tokens.begin(), this->tokens.end(), option);
            if (itr != this->tokens.end() && ++itr != this->tokens.end()){
                return *itr;
            }
            static const std::string empty_string("");
            return empty_string;
        }
        /// @author iain
        bool cmdOptionExists(const std::string &option) const{
            return std::find(this->tokens.begin(), this->tokens.end(), option)
                   != this->tokens.end();
        }
    private:
        std::vector <std::string> tokens;
};

  arrow::Status RunScreenDisplay(std::string path_to_file, std::string config_file) {
  
    std::unique_ptr<ReadParquetBatch> batchReader(new ReadParquetBatch(path_to_file, config_file));

    int batchNumber=0;
    arrow::Status batchStatus;
    do
    {
      std::cout<<"\nRead next batch "<< batchNumber<<std::endl; 
    //  batchStatus = batchReader->ReadNextBatch();
      std::shared_ptr<arrow::Table> table_loc;
      if(!batchReader->FormattedConfigFile())
        batchStatus = batchReader->ReadNextBatchTable(table_loc);
      else
        batchStatus = batchReader->ReadNextBatchTable_Formatted(table_loc);
      batchNumber++;
    }
    while(batchStatus.ok()&&batchNumber<1);

    std::cout<<"EOF reading process"<<std::endl;
    return arrow::Status::OK();
  }

  arrow::Status RunSocketStreaming(std::string path_to_file, std::string config_file) {
  
    StreamFileToSocket streamer(path_to_file, config_file);
    arrow::Status st = streamer.StreamFile();

    std::cout<<"EOF streaming process"<<std::endl;
    return arrow::Status::OK();
  }

  arrow::Status RunCsvDumpFile(std::string path_to_file, std::string config_file, std::string output_file, bool bConcatenate) {
  
    //int res=mkfifo("/tmp/test_fifo",0666);
    //if(res<0)
    //  std::cout<<"Error while creating FIFO "<<strerror(errno)<<std::endl;

    CsvDumpFile csvInterface(path_to_file, config_file, output_file, bConcatenate);
    arrow::Status st = csvInterface.FormatFile_CSV();

    std::cout<<"EOF csv to text process"<<std::endl;
    return arrow::Status::OK();
  }

  arrow::Status RunFifoStreaming(std::string path_to_file, std::string config_file, std::string output_file, bool bConcatenate) {
  
  //  int res=mkfifo("/tmp/test_fifo",0666);
  //  if(res<0)
  //    std::cout<<"Error while creating FIFO "<<strerror(errno)<<std::endl;

    int fifo=open("/tmp/test_fifo",O_WRONLY);
    arrow::Status res = RunCsvDumpFile(path_to_file, config_file, "/tmp/test_fifo", false);
    close(fifo);

    return arrow::Status::OK();
  }

  arrow::Status RunIpcDumpFile(std::string path_to_file, std::string config_file, std::string output_file) {
  
    IPCDumpFile IPCInterface(path_to_file, config_file, output_file);
    arrow::Status st = IPCInterface.FormatFile_IPC();

    std::cout<<"EOF IPC to text process"<<std::endl;
    return arrow::Status::OK();
  }

  int main(int argc, char** argv) {

    InputParser input(argc, argv);

    if( input.cmdOptionExists("-help")){
        std::cout<<"test -in pq_file -config config_file -format (screen/stream_socket/csv/ipc/fifo) -out ouput_name -concat"<<std::endl;
        return EXIT_SUCCESS;
    }

    const std::string &path_to_file = input.getCmdOption("-in");
    const std::string &config_file = input.getCmdOption("-config");
    const std::string &display_data = input.getCmdOption("-format");
    const std::string &output_file = input.getCmdOption("-out");
    bool bConcatenate=input.cmdOptionExists("-concat");

    std::cout<<path_to_file<<std::endl;
    std::cout<<config_file<<std::endl;

    if(display_data=="screen"||display_data==""){
      arrow::Status status = RunScreenDisplay(path_to_file,config_file);

      if (!status.ok()) {
        std::cerr << "Error occurred: " << status.message() << std::endl;
        return EXIT_FAILURE;
      }
      return EXIT_SUCCESS;
    }

    if(display_data=="stream_socket"){
      arrow::Status status = RunSocketStreaming(path_to_file,config_file);

      if (!status.ok()) {
        std::cerr << "Error occurred: " << status.message() << std::endl;
        return EXIT_FAILURE;
      }
      return EXIT_SUCCESS;
    }

    if(display_data=="fifo"){
      arrow::Status status = RunFifoStreaming(path_to_file,config_file,output_file,bConcatenate);

      if (!status.ok()) {
        std::cerr << "Error occurred: " << status.message() << std::endl;
        return EXIT_FAILURE;
      }
      return EXIT_SUCCESS;
    }


    if(display_data=="csv"){
      arrow::Status status = RunCsvDumpFile(path_to_file,config_file,output_file,bConcatenate);

      if (!status.ok()) {
        std::cerr << "Error occurred: " << status.message() << std::endl;
        return EXIT_FAILURE;
      }
      return EXIT_SUCCESS;
    }

   if(display_data=="ipc"){
      arrow::Status status = RunIpcDumpFile(path_to_file,config_file,output_file);

      if (!status.ok()) {
        std::cerr << "Error occurred: " << status.message() << std::endl;
        return EXIT_FAILURE;
      }
      return EXIT_SUCCESS;
    }


    return EXIT_SUCCESS;
  }


