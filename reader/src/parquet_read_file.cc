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



  #include "parquet_read_file.h"
  #include "partition_config.h"

  ReadParquetBatch::ReadParquetBatch(std::string fileName, std::string partConfigFile, int maxMemAllocated ) : 
      m_path_to_file(fileName), m_part_config_file(partConfigFile), m_maxMemory(maxMemAllocated), m_vmRSS_init(0), m_batchNumber(0), m_batchSize(0)
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
    std::cout << "Record size (Bytes) " << recordSize << std::endl;
    return recordSize;
  }

  // get parameter name and type from partition config file
  arrow::Status ReadParquetBatch::SetupPartitionConfig(){

    m_partitionConfig=nullptr;
    if(m_part_config_file!="")
      m_partitionConfig = std::unique_ptr<PartitionConfig>(new PartitionConfig(m_part_config_file)); 
    return arrow::Status::OK();
  }
  
  bool ReadParquetBatch::FormattedConfigFile(){

    return(m_partitionConfig!=nullptr);
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
    m_batchSize = 5000;    // batchSize is in fact the number of rows
    arrow_reader_props.set_batch_size(m_batchSize);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(
        reader_builder.OpenFile(m_path_to_file, /*memory_map=*/false, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);
    
    ARROW_ASSIGN_OR_RAISE(m_arrow_reader_gbl, reader_builder.Build());
    ARROW_RETURN_NOT_OK(m_arrow_reader_gbl->GetRecordBatchReader(&m_rb_reader_gbl)); 

    // Compute the nimber of lines read by each batch in function of the maximum memory
    //     allocated to the process
    std::shared_ptr<::arrow::Schema> schema;
    m_arrow_reader_gbl->GetSchema(&schema);
   // std::cout<<schema->ToString()<<std::endl;
    m_recordSize = GetRecordSize(schema);
    m_batchSize = int((m_maxMemory*1024*1024*0.85)/m_recordSize);   // .85 is a "a la louche" factor
    std::cout<<"Max mem (MB): "<<m_maxMemory<<"  -> batch size : "<<m_batchSize<<std::endl;
    m_arrow_reader_gbl->set_batch_size(m_batchSize);

    return arrow::Status::OK();
  }

  // Read a batch from the file and shows the memory usage
  arrow::Status ReadParquetBatch::ReadNextBatch() { 

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch = m_rb_reader_gbl->Next(); 
    
    if(maybe_batch!=nullptr)
    {
      int vmRSS_batch = DumpProcessMemory("VmRSS", true);

      ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
      ARROW_ASSIGN_OR_RAISE(auto table,
                            arrow::Table::FromRecordBatches(batch->schema(), {batch}));

      std::cout << "Table size : "<<table->num_rows() << " x " << table->num_columns() << std::endl;
        

      int tableSize_th = (m_recordSize * table->num_rows()) / (1024.0 * 1024);
      std::cout << "Theoretical table size : " << tableSize_th << " MB" << std::endl;
      std::cout << "Memory delta : " << vmRSS_batch - m_vmRSS_init << std::endl;

      int res = DumpProcessMemory("", true); 
      return arrow::Status::OK();
    }
 
    std::cout << "Last batch reached " << std::endl;
    return arrow::Status::ExecutionError("End of RecorBatchReader iterator");
  }

// Read a batch from the file and return the table as it is
  arrow::Status ReadParquetBatch::ReadNextBatchTable(std::shared_ptr<arrow::Table>& out) { 

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch = m_rb_reader_gbl->Next(); 

    if(maybe_batch!=nullptr)
      {
        int vmRSS_batch = DumpProcessMemory("VmRSS", true);

        ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
        //ARROW_ASSIGN_OR_RAISE(auto table,
        ARROW_ASSIGN_OR_RAISE(out,
                            arrow::Table::FromRecordBatches(batch->schema(), {batch}));

        std::cout << "Table size : "<<out->num_rows() << " x " << out->num_columns() << std::endl;   

        return arrow::Status::OK();
      }
      std::cout << "Last batch reached " << std::endl;
    return arrow::Status::ExecutionError("End of RecorBatchReader iterator");
  }


// Read a batch from the file and format the table iaccording to the partition configuration file
//    -> column reordering, true/false -> 1/0, remove null values, ...
  arrow::Status ReadParquetBatch::ReadNextBatchTable_Formatted(std::shared_ptr<arrow::Table>& outputTable) { 

    if(m_partitionConfig==nullptr)
      return arrow::Status::ExecutionError("No partition configuration file defined");

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch = m_rb_reader_gbl->Next(); 

    std::vector<std::string> configParamNames=m_partitionConfig->GetConfigParamNames();
    std::vector<std::string> paramNotFound;
    std::map <std::string, std::shared_ptr<arrow::Field>> fieldConfig; 

    if(maybe_batch!=nullptr) {
      int vmRSS_batch = DumpProcessMemory("VmRSS", true);

      ARROW_ASSIGN_OR_RAISE(auto batch, maybe_batch);
      std::shared_ptr<arrow::Table> initTable;
      ARROW_ASSIGN_OR_RAISE(initTable,
                            arrow::Table::FromRecordBatches(batch->schema(), {batch}));

      std::cout << "Formatted table :  init size : "<<initTable->num_rows() << " x " << initTable->num_columns() << std::endl;   

      //std::cout<<initTable->schema()->ToString()<<std::endl;

      const arrow::FieldVector fields=initTable->schema()->fields();
      for(auto fd : fields){
        //std::cout<<"Field : "<< fd->name()<<" "<<fd->type()->ToString()<<std::endl;
        fieldConfig[fd->name()]=fd;
      }

      arrow::FieldVector formatedTable_fields;
      std::vector<std::shared_ptr<arrow::ChunkedArray>> formatedTable_columns;

      // Loop over the column names as defined in the partition config file
      for(std::string paramName : configParamNames){

        //std::cout<<"-> read column "<<paramName<<std::endl;
        std::shared_ptr<arrow::ChunkedArray> chunkedArray = initTable->GetColumnByName(paramName); 
  
        // Column not found in the arrow table...
        if(chunkedArray==NULLPTR){
          paramNotFound.push_back(paramName);
        }
        else{

          // Column type is boolean -> switch to 0/1 representation
          if(fieldConfig[paramName]->type()==arrow::boolean()){
            auto newChunkedArray = ChunkArrayReformatBoolean(chunkedArray,true);
            if(newChunkedArray==nullptr){
              return arrow::Status::ExecutionError("Error while formating boolean chunk array");
            }
            formatedTable_columns.push_back(newChunkedArray);

            std::shared_ptr<arrow::Field> newField=std::make_shared<arrow::Field>(fieldConfig[paramName]->name(),arrow::int8());
            formatedTable_fields.push_back(newField); 
          }
          // Simply keep the chunk as it is defined in teh arrow table
          else{
            formatedTable_columns.push_back(chunkedArray);
            formatedTable_fields.push_back(fieldConfig[paramName]); 
          }


        }
      } // end of loop over parameters 

      // If a column was not found, throw an error and stop
      if(paramNotFound.size()>0){
        for(auto name : paramNotFound)
          std::cout<<"ERROR : param name "<<name<<" not found in table columns"<<std::endl;
          return arrow::Status::ExecutionError("Configuration file : missing parameter in table");
      }

      // Create the arrow::schema of the new table
      std::shared_ptr<arrow::Schema> formatedSchema = std::make_shared<arrow::Schema>(arrow::Schema(formatedTable_fields, initTable->schema()->endianness()));
      //std::cout<<"New formated schema : "<<formatedSchema->ToString()<<std::endl;

      // and finally create the arrow::Table that matches the partition config file
      outputTable = arrow::Table::Make(formatedSchema, formatedTable_columns);
      arrow::Status resTable=outputTable->ValidateFull();
      if(!resTable.ok()){
        std::cout<<"ERROR : formated table full validation not OK"<<std::endl;
        return arrow::Status::ExecutionError("CSV output table not valid");
      }
      // Increment the batch number  
      m_batchNumber++;

      return arrow::Status::OK();
    }

    // The end of the parquet file has been reached
    std::cout << "Last batch reached " << std::endl;
    return arrow::Status::ExecutionError("End of RecorBatchReader iterator");
  
  }
  
 // Reformat a boolean chunk array : true/false boolean array => 1/0 int8 array
 std::shared_ptr<arrow::ChunkedArray> ReadParquetBatch::ChunkArrayReformatBoolean(std::shared_ptr<arrow::ChunkedArray>& inputArray, bool bCheck) {

  std::vector<std::shared_ptr<arrow::Array>> newChunks;
  std::shared_ptr<arrow::Array> array;  
  arrow::Int8Builder builder;

  const arrow::ArrayVector& chunks = inputArray->chunks();
  for(auto& elemChunk : chunks){
    std::shared_ptr<arrow::ArrayData>chunkData = elemChunk->data();
    builder.Reset();

    auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(elemChunk);                  
    for (int64_t i = 0; i < elemChunk->length(); ++i) {
      bool bIsNull=bool_array->IsNull(i);
      if(bIsNull)
        arrow::Status status=builder.AppendNull();
      else
        arrow::Status status=builder.Append(bool_array->Value(i));
    }

    if (!builder.Finish(&array).ok()) {
      std::cout<<"ERROR  while finalizong "<<inputArray->ToString()<<" new chunked array"<<std::endl;
    }

    if(bCheck){
      assert(array->length()==elemChunk->length());

      auto new_array = std::static_pointer_cast<arrow::Int8Array>(array);
      for (int64_t i = 0; i < elemChunk->length(); ++i) {
        assert(bool_array->IsNull(i)==array->IsNull(i));
        assert((bool_array->Value(i)==true&&new_array->Value(i)!=0)||(bool_array->Value(i)==false&&new_array->Value(i)==0));
      }
    }

    newChunks.push_back(std::move(array));
  }

  auto newChunkedArray = std::make_shared<arrow::ChunkedArray>(std::move(newChunks));

  auto status=newChunkedArray->ValidateFull();
  if(!status.ok()){
    std::cout<<"Invalid new chunkArraay : "<<status.ToString()<<std::endl;
    return nullptr;
  }

  return newChunkedArray;
 }








