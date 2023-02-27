
#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/chunked_array.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <unistd.h>
#include <iostream>
#include <fstream>
#include <map>

class PartitionConfig;

class ReadParquetBatch{

  public:
    ReadParquetBatch(std::string fileName, std::string partConfigFile, int maxMemAllocated = 2000);
//    arrow::Status ReadNextBatch(int batchNumber);
    arrow::Status ReadNextBatch();
    //std::shared_ptr<arrow::Table> ReadNextBatchTable();
    arrow::Status ReadNextBatchTable(std::shared_ptr<arrow::Table>& table); 
    arrow::Status ReadNextBatchTable_Formatted(std::shared_ptr<arrow::Table>& table);
    bool FormattedConfigFile();

  private:

    int DumpProcessMemory(std::string idValue="", bool bVerbose=false) const;
    int GetRecordSize(std::shared_ptr<arrow::Schema> schema, int defaultSize = 32) const;
    std::shared_ptr<arrow::ChunkedArray> ChunkArrayReformatBoolean(std::shared_ptr<arrow::ChunkedArray>& inputArray,bool bCheck=false) ;

    arrow::Status SetupBatchReader();
    arrow::Status SetupPartitionConfig();

    std::string m_path_to_file;
    std::string m_part_config_file;
    int m_vmRSS_init;
    int m_batchNumber, m_batchSize;

    std::unique_ptr<PartitionConfig> m_partitionConfig;
    std::unique_ptr<parquet::arrow::FileReader> m_arrow_reader_gbl;
    std::unique_ptr<::arrow::RecordBatchReader> m_rb_reader_gbl;
    int m_maxMemory, m_recordSize;
};
