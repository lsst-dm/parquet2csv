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

 // #include "arrow/util/key_value_metadata.h"

  #include "parquet_read_dp0.h"
  //#include "partition_config.h"

  ReadParquetBatch::ReadParquetBatch(std::string fileName, std::string partConfigFile) : 
      m_path_to_file(fileName), m_part_config_file(partConfigFile), m_vmRSS_init(0), m_batchNumber(0), m_batchSize(0)
  {
      // setup the reader that access te parquet file
      arrow::Status status1=SetupBatchReader();  
      // get parameter name and type from partition config file
      arrow::Status status2=SetupPartitionConfig(); 
  }

  // Memory used by the current process
  int ReadParquetBatch::DumpProcessMemory(std::string idValue, bool bVerbose) const {

    int tSize = 0, resident = 0, share = 0;
    std::ifstream buffer("/proc/self/statm");
    buffer >> tSize >> resident >> share;
    buffer.close();

    long page_size_kb =
        sysconf(_SC_PAGE_SIZE) / 1024;  // in case x86-64 is configured to use 2MB pages

    double vmSize = (tSize * page_size_kb) / 1024.0;
    double rss = (resident * page_size_kb) / 1024.0;
    double shared_mem = (share * page_size_kb) / 1024.0;

    if (bVerbose) {
      std::cout << "VmSize - " << vmSize << " MB  ";
      std::cout << "VmRSS - " << rss << " MB  ";
      std::cout << "Shared Memory - " << shared_mem << " MB  ";
      std::cout << "Private Memory - " << rss - shared_mem << "MB" << std::endl;
    }

    if (!idValue.empty()) {
      std::map<std::string, int> res{
          {"VmSize", vmSize}, {"VmRSS", rss}, {"SharedMem", shared_mem}};
      if (res.find(idValue) != res.end()) return res[idValue];
    }
    return 0;
  }

  // Compute the memory size of a row by adding its element size
  //   stringDefaultSize is the default size of a parameter identified as a string 
  int ReadParquetBatch::GetRecordSize(std::shared_ptr<arrow::Schema> schema, int stringDefaultSize) const {
    int recordSize = 0;
    int defaultSize = 32;

    const arrow::FieldVector& vFields = schema->fields();
    for (const auto& field : vFields) {
      int fieldSize = field->type()->byte_width();
      if (fieldSize < 0) fieldSize = stringDefaultSize;
      recordSize += fieldSize;
    }
    std::cout << "Record size " << recordSize << std::endl;
    return recordSize;
  }

  // get parameter name and type from partition config file
  arrow::Status ReadParquetBatch::SetupPartitionConfig(){

    m_partitionConfig = std::unique_ptr<PartitionConfig>(new PartitionConfig(m_part_config_file)); 
    return arrow::Status::OK();
  }

  // setup the reader that access te parquet file
  arrow::Status ReadParquetBatch::SetupBatchReader() {

    m_vmRSS_init = DumpProcessMemory("VmRSS", true);
    std::cout << "Init RSS value " << m_vmRSS_init << std::endl;

    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    m_batchSize = 128 * 16 * 4;    // batchSize is in fact the number of rows
    arrow_reader_props.set_batch_size(m_batchSize * 1024);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(
        reader_builder.OpenFile(m_path_to_file, /*memory_map=*/false, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    ARROW_ASSIGN_OR_RAISE(m_arrow_reader_gbl, reader_builder.Build());
    ARROW_RETURN_NOT_OK(m_arrow_reader_gbl->GetRecordBatchReader(&m_rb_reader_gbl)); 
    return arrow::Status::OK();
  }


  arrow::Status ReadParquetBatch::ReadNextBatch() { 

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch = m_rb_reader_gbl->Next(); 
    
    if(maybe_batch!=nullptr)
    {
      int vmRSS_batch = DumpProcessMemory("VmRSS", true);

      ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
      ARROW_ASSIGN_OR_RAISE(auto table,
                            arrow::Table::FromRecordBatches(batch->schema(), {batch}));

      std::cout << "Table size : "<<table->num_rows() << " x " << table->num_columns() << std::endl;
        
      int recordSize = GetRecordSize(table->schema());
      int tableSize_th = (recordSize * table->num_rows()) / (1024.0 * 1024);
      std::cout << "Theoretical table size : " << tableSize_th << " MB" << std::endl;
      std::cout << "Memory delta : " << vmRSS_batch - m_vmRSS_init << std::endl;

      int res = DumpProcessMemory("", true); 
      return arrow::Status::OK();
    }
 
    std::cout << "Last batch reached " << std::endl;
    return arrow::Status::ExecutionError("End of RecorBatchReader iterator");
  }

 