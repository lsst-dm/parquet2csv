
#include <arpa/inet.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <filesystem>

#include "partition_config.h"
#include "parquet_read_file.h"
#include "ipc_dump_file.h"

namespace fs = std::filesystem;


IPCDumpFile::IPCDumpFile(std::string fileName, std::string partConfigFile, std::string outputFile) :
    m_path_to_file(fileName), m_part_config_file(partConfigFile),m_output_file(outputFile)
{
    m_batchReader = nullptr;

    // Test if parquet and partition config files exist
    bool bInputError=false;
    std::cout<<"Data file : "<<m_path_to_file<<std::endl;
    fs::path f1{m_path_to_file};
    if (!fs::exists(f1)) {
        bInputError=true;
        std::cout << "Data file does not exist "<<m_path_to_file<<std::endl;
    }
    fs::path f2{m_part_config_file};
    if (!fs::exists(f2)) {
        bInputError=true;
        std::cout << "Partition config file does not exist "<<m_part_config_file<<std::endl;
    }
    if (bInputError) throw std::exception();

    // Parquet batch reader setup
    std::cout<<"SETUP ReadParquetBatch"<<std::endl;
    m_batchReader=std::make_unique<ReadParquetBatch>(ReadParquetBatch(m_path_to_file, m_part_config_file));

}

// Read an arrow batch, format the table acording to the config file and save it in IPC format
arrow::Status IPCDumpFile::FormatFile_IPC() {

    std::unique_ptr<ReadParquetBatch> batchReader(new ReadParquetBatch(m_path_to_file, m_part_config_file));

    int batchNumber=0;
    arrow::Status batchStatus;
    do {
        std::cout<<"\nRead next batch "<< batchNumber<<std::endl;

        std::shared_ptr<arrow::Table> table_loc;
        batchStatus = batchReader->ReadNextBatchTable_Formatted(table_loc);

        if(batchStatus.ok()) {
            arrow::Status st=WriteFile(batchNumber,table_loc);
        }

        batchNumber++;
    }
    //while(batchStatus.ok());
    while(batchStatus.ok()&&batchNumber<1);

    std::cout<<"EOF reading process"<<std::endl;
    return arrow::Status::OK();

}

// Create the IPC file name ( general name and name for each batch)
std::string IPCDumpFile::GetFileName(int batchNumber) {

    return std::string("/tmp/test_fifo");

    std::string filename=m_output_file+"_"+std::to_string(batchNumber)+".ipc";
    return filename;
}

// Write table in IPC format
arrow::Status IPCDumpFile::WriteFile(int batchNumber, std::shared_ptr<arrow::Table>& table) {

    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    auto IPC_filename = GetFileName(batchNumber);
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(IPC_filename));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                          arrow::ipc::MakeFileWriter(outfile, table->schema(), options));
    ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
    ARROW_RETURN_NOT_OK(ipc_writer->Close());

    return arrow::Status::OK();
}

