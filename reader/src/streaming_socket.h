


class SocketOutputStream : public arrow::io::OutputStream {

public:
    SocketOutputStream(std::shared_ptr<arrow::io::FileOutputStream> target)
        : target_(target), position_(0) {}

    virtual ~SocketOutputStream() {}

    arrow::Status Close() override {
        return target_->Close();
    }
    arrow::Status Abort() override {
        return target_->Abort();
    }
    bool closed() const override {
        return target_->closed();
    }
    arrow::Status Flush() override {
        return target_->Flush();
    }

    static arrow::Result<std::shared_ptr<SocketOutputStream>> Open(int sock) {
        auto target_res = arrow::io::FileOutputStream::Open(sock);
        if (!target_res.ok()) {
            return target_res.status();
        }
        return std::make_shared<SocketOutputStream>(*target_res);
    }

    arrow::Status Write(const void *data, int64_t nbytes) override {
        position_ += nbytes;
        return target_->Write(data, nbytes);
    }

    arrow::Status Write(const std::shared_ptr<arrow::Buffer> &data) override {
        position_ += data->size();
        return target_->Write(data);
    }

    arrow::Result<int64_t> Tell() const override {
        return position_;
    }

private:
    std::shared_ptr<arrow::io::FileOutputStream> target_;
    uint64_t position_;
};

class ReadParquetBatch;

class StreamFileToSocket {

public:
    StreamFileToSocket(std::string fileName, std::string partConfigFile);
    arrow::Status StreamFile();
    arrow::Status StreamFileExample();

private:

    bool CheckErr(int err, std::string activity);
    bool CheckErr(arrow::Status status, std::string activity);
    std::shared_ptr<arrow::Table> MakeTable();
    void  SendTable(int socket_fd);

    std::unique_ptr<ReadParquetBatch> m_batchReader;

    std::string m_path_to_file;
    std::string m_part_config_file;

};

