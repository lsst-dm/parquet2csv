


class ReadParquetBatch;

class IPCDumpFile {

public:
    IPCDumpFile(std::string fileName, std::string partConfigFile, std::string outputFile);
    arrow::Status FormatFile_IPC();

private:

    arrow::Status WriteFile(int batchNumber, std::shared_ptr<arrow::Table>& outputTable);
    std::string GetFileName(int batchNumber);

    std::unique_ptr<ReadParquetBatch> m_batchReader;
    std::string m_path_to_file;
    std::string m_part_config_file;
    std::string m_output_file;

};

