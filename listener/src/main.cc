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
#include <vector>
#include <fcntl.h>
#include <iostream>

#include "arrow/io/stdio.h"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"

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

arrow::Status ReadArrowTableFromFifo(std::string fifo_name) {

    arrow::io::StdinStream input;
    arrow::io::StdoutStream sink;

    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::RecordBatchStreamReader::Open(&input));

    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
     ARROW_ASSIGN_OR_RAISE(batch, reader->Next());
    }

    std::cout<<"EOF reading process"<<std::endl;
    return arrow::Status::OK();
}

 

int main(int argc, char** argv) {

    InputParser input(argc, argv);

    if( input.cmdOptionExists("-help")){
        std::cout<<"arrow_listener -in fifo"<<std::endl;
        return EXIT_SUCCESS;
    }

    const std::string &fifo_path = input.getCmdOption("-in");

    if(input.cmdOptionExists("-in")){
      arrow::Status status = ReadArrowTableFromFifo(fifo_path);
    }

    return EXIT_SUCCESS;
}



