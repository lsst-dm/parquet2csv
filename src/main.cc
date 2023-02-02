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

  #include "parquet_read_dp0.h"

  arrow::Status RunExamples(std::string path_to_file, std::string config_file) {
  
    std::unique_ptr<ReadParquetBatch> batchReader(new ReadParquetBatch(path_to_file, config_file));

    int batchNumber=0;
    arrow::Status batchStatus;
    do
    {
      std::cout<<"Read next batch "<< batchNumber<<std::endl; 
      batchStatus = batchReader->ReadNextBatch(batchNumber);
      batchNumber++;
    }
    while(batchStatus.ok());

    std::cout<<"EOF reading process"<<std::endl;
    return arrow::Status::OK();
  }

  int main(int argc, char** argv) {
    if (argc <2) {
      return EXIT_SUCCESS;
    }

    std::string path_to_file = argv[1];
    std::string config_file = "";
    if (argc>2) config_file = argv[2];

    arrow::Status status = RunExamples(path_to_file,config_file);

    if (!status.ok()) {
      std::cerr << "Error occurred: " << status.message() << std::endl;
      return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
  }


