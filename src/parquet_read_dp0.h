
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <unistd.h>
#include <iostream>
#include <fstream>
#include <map>

//class PartitionConfig;
#include "partition_config.h"

class ReadParquetBatch{

  public:
    ReadParquetBatch(std::string fileName, std::string partConfigFile);
    arrow::Status ReadNextBatch(int batchNumber);
    
  private:

    int DumpProcessMemory(std::string idValue="", bool bVerbose=false) const;
    int GetRecordSize(std::shared_ptr<arrow::Schema> schema, int defaultSize = 32) const;

    arrow::Status SetupBatchReader();
    arrow::Status SetupPartitionConfig();

    std::string m_path_to_file;
    std::string m_part_config_file;
    int m_vmRSS_init;
    int m_batchNumber, m_batchSize;

    std::unique_ptr<PartitionConfig> m_partitionConfig;
    std::unique_ptr<parquet::arrow::FileReader> m_arrow_reader_gbl;
  
};
