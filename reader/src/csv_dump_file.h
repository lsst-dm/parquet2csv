


class ReadParquetBatch;

class CsvDumpFile {

public:
    CsvDumpFile(std::string fileName, std::string partConfigFile, std::string outputFile, bool bConcatenate=false);
    arrow::Status FormatFile_CSV();

private:

    arrow::Status WriteFile(int batchNumber, std::shared_ptr<arrow::Table>& outputTable);
    arrow::Status ConcatenateFiles(int batchNumber);
    std::string GetFileName(int batchNumber);

    std::unique_ptr<ReadParquetBatch> m_batchReader;
    std::string m_path_to_file;
    std::string m_part_config_file;
    std::string m_output_file;
    bool m_bConcatenate;

};

