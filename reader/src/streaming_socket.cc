
#include <arpa/inet.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "partition_config.h"
#include "parquet_read_file.h"
#include "streaming_socket.h"

constexpr uint16_t port = 56565;
constexpr char host[] = "127.0.0.1";

StreamFileToSocket::StreamFileToSocket(std::string fileName, std::string partConfigFile) :
    m_path_to_file(fileName), m_part_config_file(partConfigFile)
{
    m_batchReader = nullptr;
    if(m_path_to_file!="") {
        std::cout<<"SETUP ReadParquetBatch"<<std::endl;
        m_batchReader=std::make_unique<ReadParquetBatch>(ReadParquetBatch(m_path_to_file, m_part_config_file));
    }
}

arrow::Status StreamFileToSocket::StreamFile() {

    if(m_path_to_file=="") return StreamFileExample();

    struct sockaddr_in addr;
    char hello[] = "Hello from client";
    char buffer[1024] = {0};
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (!CheckErr(sock, "socket")) {
        return arrow::Status::ExecutionError("Socket initialization failed");
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (!CheckErr(inet_pton(AF_INET, host, &addr.sin_addr), "inet_pton")) {
        return arrow::Status::ExecutionError("IP connection error");;
    }

    if (!CheckErr(connect(sock, (struct sockaddr *)&addr, sizeof(addr)),
                  "connect")) {
        return arrow::Status::ExecutionError("Socket connection error");
    }

    auto output_res = SocketOutputStream::Open(sock);
    if (!CheckErr(output_res.status(), "arrow::io::FileOutputStream")) {
        return arrow::Status::ExecutionError("arrow::io::FileOutputStream");
    }
    auto output = *output_res;
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>> writer_res;

    // Loop over RecordBatchReader
    int batchNumber=0;
    arrow::Status batchStatus;
    do
    {
        std::cout<<"\nRead next batch "<< batchNumber<<std::endl;
        std::shared_ptr<arrow::Table> table_loc;
        batchStatus = m_batchReader->ReadNextBatchTable(table_loc);
        if(batchStatus.ok()) {
            std::cout << "Streamed table size : "<<table_loc->num_rows() << " x " << table_loc->num_columns() << std::endl;

            // Send table schema
            if(batchNumber==0) {
                writer_res = arrow::ipc::MakeStreamWriter(output, table_loc->schema());
                if (!CheckErr(writer_res.status(), "arrow::ipc::MakeStreamWriter")) {
                    return arrow::Status::ExecutionError("ERROR : arrow::ipc::MakeStreamWriter");
                }
            }

            // Send table
            auto writer = *writer_res;

            if (!CheckErr(writer->WriteTable(*table_loc), "RecordBatchWriter::WriteTable")) {
                return arrow::Status::ExecutionError("ERROR : RecordBatchWriter::WriteTable");
            }

            //  std::cout<<table_loc->ToString()<<std::endl;
            batchNumber++;

            if(batchNumber==1) {
                auto writer = *writer_res;
                CheckErr(writer->Close(), "RecordBatchWriter::Close");
                std::cout<<"EOF reading process"<<std::endl;
                return arrow::Status::OK();
            }
        }
    }
    while(batchStatus.ok());

    auto writer = *writer_res;
    CheckErr(writer->Close(), "RecordBatchWriter::Close");

    std::cout<<"EOF reading process"<<std::endl;
    return arrow::Status::OK();

}



arrow::Status StreamFileToSocket::StreamFileExample() {

    struct sockaddr_in addr;
    char hello[] = "Hello from client";
    char buffer[1024] = {0};
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (!CheckErr(sock, "socket")) {
        return arrow::Status::ExecutionError("Socket initialization failed");
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (!CheckErr(inet_pton(AF_INET, host, &addr.sin_addr), "inet_pton")) {
        return arrow::Status::ExecutionError("IP connection error");;
    }

    if (!CheckErr(connect(sock, (struct sockaddr *)&addr, sizeof(addr)),
                  "connect")) {
        return arrow::Status::ExecutionError("Socket connection error");
    }

    SendTable(sock);
    std::cout<<"Table sent\n"<<std::endl;

    return arrow::Status::OK();
}


bool StreamFileToSocket::CheckErr(int err, std::string activity) {
    if (err < 0) {
        std::cerr << "Received error code " << err << " while calling " << activity
                  << std::endl;
        return false;
    }
    return true;
}

bool  StreamFileToSocket::CheckErr(arrow::Status status, std::string activity) {
    if (!status.ok()) {
        std::cerr << "Recevied err status " << status << " while calling "
                  << activity << std::endl;
        return false;
    }
    return true;
}

std::shared_ptr<arrow::Table> StreamFileToSocket::MakeTable() {
    arrow::MemoryPool *pool = arrow::default_memory_pool();
    arrow::Int64Builder values_builder(pool);
    arrow::Status st1 = values_builder.Append(1);
    arrow::Status st2 = values_builder.Append(2);
    arrow::Status st3 = values_builder.Append(3);
    std::shared_ptr<arrow::Int64Array> arr;
    if (!CheckErr(values_builder.Finish(&arr), "values_builder::Finish")) {
        return nullptr;
    }

    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("values", arrow::int64())
    };
    auto schema = std::make_shared<arrow::Schema>(fields);
    return arrow::Table::Make(schema, {arr});
}


void  StreamFileToSocket::SendTable(int socket_fd) {
    auto output_res = SocketOutputStream::Open(socket_fd);
    if (!CheckErr(output_res.status(), "arrow::io::FileOutputStream")) {
        return;
    }
    auto output = *output_res;

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    auto table = MakeTable();
    if (table == nullptr) {
        return;
    }

    auto writer_res = arrow::ipc::MakeStreamWriter(output, table->schema());
    if (!CheckErr(writer_res.status(), "arrow::ipc::MakeStreamWriter")) {
        return;
    }
    auto writer = *writer_res;
    if (!CheckErr(writer->WriteTable(*table), "RecordBatchWriter::WriteTable")) {
        return;
    }
    CheckErr(writer->Close(), "RecordBatchWriter::Close");
}
