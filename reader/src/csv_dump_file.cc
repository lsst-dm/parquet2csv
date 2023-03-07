
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

#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>

#include "partition_config.h"
#include "parquet_read_file.h"
#include "csv_dump_file.h"

namespace fs = std::filesystem;

constexpr uint16_t port = 56565;
constexpr char host[] = "127.0.0.1";

CsvDumpFile::CsvDumpFile(std::string fileName, std::string partConfigFile, std::string outputFile, bool bConcatenate) :
    m_path_to_file(fileName), m_part_config_file(partConfigFile),m_output_file(outputFile),m_bConcatenate(bConcatenate)
{
    m_batchReader = nullptr;

    // Test if parquet and partition config files exist
    bool bInputError=false;
    fs::path f1{m_path_to_file};
    if (!fs::exists(f1))
    {
        bInputError=true;
        std::cout << "Data file does not exist "<<m_path_to_file<<std::endl;
    }
    fs::path f2{m_part_config_file};
    if (!fs::exists(f2))
    {
        bInputError=true;
        std::cout << "Partition config file does not exist "<<m_part_config_file<<std::endl;
    }
    if (bInputError) throw std::exception();

    // Parquet batch reader setup
    std::cout<<"SETUP ReadParquetBatch"<<std::endl;
    m_batchReader=std::make_unique<ReadParquetBatch>(ReadParquetBatch(m_path_to_file, m_part_config_file));

}

// Read an arrow batch, format the table acording to the config file and save it in csv format
arrow::Status CsvDumpFile::FormatFile_CSV()
{

    std::unique_ptr<ReadParquetBatch> batchReader(new ReadParquetBatch(m_path_to_file, m_part_config_file));

    int batchNumber=0;
    arrow::Status batchStatus;
    do
    {
        std::cout<<"\nRead next batch "<< batchNumber<<std::endl;

        std::shared_ptr<arrow::Table> table_loc;
        batchStatus = batchReader->ReadNextBatchTable_Formatted(table_loc);

        if(batchStatus.ok())
        {
            arrow::Status st=WriteFile(batchNumber,table_loc);
            if(m_bConcatenate) arrow::Status st=ConcatenateFiles(batchNumber);
        }
        else
        {
            std::cout<<"Error while reading and formating batch"<<std::endl;
        }
        batchNumber++;
    }
    //while(batchStatus.ok());
    while(batchStatus.ok()&&batchNumber<1);

    std::cout<<"EOF reading process"<<std::endl;
    return arrow::Status::OK();

}

// Create the csv file name ( general name and name for each batch)
std::string CsvDumpFile::GetFileName(int batchNumber=-1)
{

//  return std::string("/tmp/test_fifo");
    if (m_output_file.find("fifo") != std::string::npos)
        return m_output_file;

    if(batchNumber<0) return (m_output_file+".csv");

    std::string filename=m_output_file+"_"+std::to_string(batchNumber)+".csv";
    return filename;
}

// Write table in csv format
arrow::Status CsvDumpFile::WriteFile(int batchNumber, std::shared_ptr<arrow::Table>& table)
{

    // Csv file name
    auto csv_filename = GetFileName(batchNumber);
    ARROW_ASSIGN_OR_RAISE(auto outstream, arrow::io::FileOutputStream::Open(csv_filename));


    //ARROW_ASSIGN_OR_RAISE(auto outstream, arrow::io::BufferOutputStream::Create(1 << 10));


    // Options : null string, no header, no quotes around strings
    arrow::csv::WriteOptions writeOpt=arrow::csv::WriteOptions::Defaults();
    writeOpt.null_string="\\N";
    writeOpt.include_header=false;
    writeOpt.quoting_style=arrow::csv::QuotingStyle::None;

    std::cout << "Writing CSV file: " << csv_filename<<std::endl;
    ARROW_RETURN_NOT_OK(arrow::csv::WriteCSV(*table, writeOpt, outstream.get()));

    /*
     ARROW_ASSIGN_OR_RAISE(auto buffer, outstream->Finish());
     auto buffer_ptr = buffer.get()->data();
     int buffer_size=buffer->size();
     std::cout<<"Buffer length : "<<buffer_size<<std::endl;

     auto myfile = std::fstream("buffer.csv", std::ios::out);
     myfile.write((char*)&buffer_ptr[0], buffer_size);
     myfile.close();
    */
    return arrow::Status::OK();
}

// Concatenate the batch csv files to one big file
arrow::Status CsvDumpFile::ConcatenateFiles(int batchNumber)
{

    std::string gbl_filename=GetFileName();
    std::string current_filename=GetFileName(batchNumber);

    std::ostringstream cmd_concat, cmd_delete;

    cmd_concat << "cat "<<current_filename<<" >> "<<gbl_filename;
    std::cout<<"CONCATENATE "<<cmd_concat.str()<<std::endl;
    system(cmd_concat.str().c_str());

    cmd_delete << "rm  "<<current_filename;;
    std::cout<<"DELETE "<<cmd_delete.str()<<std::endl;
    system(cmd_delete.str().c_str());

    return arrow::Status::OK();

}