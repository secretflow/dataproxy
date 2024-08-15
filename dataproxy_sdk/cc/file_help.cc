// Copyright 2024 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "dataproxy_sdk/cc/file_help.h"

#include <fstream>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/builder.h"
#include "arrow/csv/api.h"
#include "arrow/io/api.h"
#include "dataproxy_sdk/cc/exception.h"
#include "file_help.h"

namespace dataproxy_sdk {

class BinaryFileWrite : public FileHelpWrite {
 public:
  void DoWrite(std::shared_ptr<arrow::RecordBatch>& record_batch) {
    DATAPROXY_ENFORCE_EQ(record_batch->num_columns(), 1);

    auto binary_array =
        std::dynamic_pointer_cast<arrow::BinaryArray>(record_batch->column(0));
    CHECK_ARROW_OR_THROW(out_stream_->Write(
        binary_array->raw_data(), binary_array->total_values_length()));
  }
  void DoClose() { CHECK_ARROW_OR_THROW(out_stream_->Close()); }

 protected:
  void DoOpen(const std::string& file_name) {
    ASSIGN_ARROW_OR_THROW(out_stream_,
                          arrow::io::FileOutputStream::Open(file_name));
  }

 private:
  std::shared_ptr<arrow::io::FileOutputStream> out_stream_;
};

class CSVFileWrite : public FileHelpWrite {
 public:
  void DoWrite(std::shared_ptr<arrow::RecordBatch>& record_batch) {
    CHECK_ARROW_OR_THROW(arrow::csv::WriteCSV(
        *record_batch, arrow::csv::WriteOptions::Defaults(),
        out_stream_.get()));
  }
  void DoClose() { CHECK_ARROW_OR_THROW(out_stream_->Close()); }

 protected:
  void DoOpen(const std::string& file_name) {
    ASSIGN_ARROW_OR_THROW(out_stream_,
                          arrow::io::FileOutputStream::Open(file_name));
  }

 private:
  std::shared_ptr<arrow::io::FileOutputStream> out_stream_;
};

class ORCFileWrite : public FileHelpWrite {
 public:
  void DoWrite(std::shared_ptr<arrow::RecordBatch>& record_batch) {
    CHECK_ARROW_OR_THROW(orc_writer_->Write(*record_batch));
  }
  void DoClose() {
    CHECK_ARROW_OR_THROW(orc_writer_->Close());
    CHECK_ARROW_OR_THROW(out_stream_->Close());
  };

 protected:
  void DoOpen(const std::string& file_name) {
    ASSIGN_ARROW_OR_THROW(out_stream_,
                          arrow::io::FileOutputStream::Open(file_name));
    ASSIGN_ARROW_OR_THROW(
        orc_writer_,
        arrow::adapters::orc::ORCFileWriter::Open(out_stream_.get()));
  }

 private:
  std::unique_ptr<arrow::adapters::orc::ORCFileWriter> orc_writer_;
  std::shared_ptr<arrow::io::FileOutputStream> out_stream_;
};

std::unique_ptr<FileHelpWrite> FileHelpWrite::Make(
    proto::FileFormat file_format, const std::string& file_name) {
  std::unique_ptr<FileHelpWrite> ret;
  switch (file_format) {
    case proto::FileFormat::CSV:
      ret = std::make_unique<CSVFileWrite>();
      break;
    case proto::FileFormat::BINARY:
      ret = std::make_unique<BinaryFileWrite>();
      break;
    case proto::FileFormat::ORC:
      ret = std::make_unique<ORCFileWrite>();
      break;
    default:
      DATAPROXY_THROW("format[{}] not support.",
                      proto::FileFormat_Name<proto::FileFormat>(file_format));
      break;
  }
  ret->DoOpen(file_name);
  return ret;
}

class BinaryFileRead : public FileHelpRead {
 public:
  BinaryFileRead(FileHelpRead::Options options) : FileHelpRead(options) {}
  ~BinaryFileRead() = default;

 private:
  const int64_t kReadBytesLen = 128 * 1024;
  const int64_t kchunksNum = 8;

 public:
  static std::shared_ptr<arrow::Schema> kBinaryFileSchema;

 public:
  void DoRead(std::shared_ptr<arrow::RecordBatch>* record_batch) {
    arrow::BinaryBuilder binary_build;
    for (int i = 0; i < kchunksNum; ++i) {
      std::shared_ptr<arrow::Buffer> buffer;
      ASSIGN_ARROW_OR_THROW(buffer, read_stream_->Read(kReadBytesLen));
      CHECK_ARROW_OR_THROW(binary_build.Append(buffer->data(), buffer->size()));
      if (buffer->size() < kReadBytesLen) break;
    }

    if (binary_build.value_data_length() > 0) {
      std::vector<std::shared_ptr<arrow::Array>> arrays(1);
      CHECK_ARROW_OR_THROW(binary_build.Finish(&arrays[0]));
      *record_batch =
          arrow::RecordBatch::Make(this->Schema(), arrays.size(), arrays);
    }
  }
  void DoClose() { CHECK_ARROW_OR_THROW(read_stream_->Close()); }
  std::shared_ptr<arrow::Schema> Schema() { return kBinaryFileSchema; }

 protected:
  void DoOpen(const std::string& file_name) {
    std::shared_ptr<arrow::io::ReadableFile> file_stream;
    ASSIGN_ARROW_OR_THROW(file_stream,
                          arrow::io::ReadableFile::Open(file_name));
    int64_t file_total_size = 0;
    ASSIGN_ARROW_OR_THROW(file_total_size, file_stream->GetSize());
    ASSIGN_ARROW_OR_THROW(read_stream_, arrow::io::RandomAccessFile::GetStream(
                                            file_stream, 0, file_total_size));
  }

 private:
  std::shared_ptr<arrow::io::InputStream> read_stream_;
};

std::shared_ptr<arrow::Schema> BinaryFileRead::kBinaryFileSchema =
    arrow::schema({arrow::field("binary_data", arrow::binary())});

class CSVFileRead : public FileHelpRead {
 public:
  CSVFileRead(FileHelpRead::Options options)
      : FileHelpRead(options),
        convert_options_(arrow::csv::ConvertOptions::Defaults()) {
    for (auto& pair : options.column_types) {
      convert_options_.column_types.emplace(pair.first, pair.second);
      convert_options_.include_columns.push_back(pair.first);
    }
  }
  ~CSVFileRead() = default;

 public:
  void DoRead(std::shared_ptr<arrow::RecordBatch>* record_batch) {
    CHECK_ARROW_OR_THROW(file_reader_->ReadNext(record_batch));
  }
  void DoClose() { CHECK_ARROW_OR_THROW(file_reader_->Close()); }
  std::shared_ptr<arrow::Schema> Schema() { return file_reader_->schema(); }

 protected:
  void DoOpen(const std::string& file_name) {
    std::shared_ptr<arrow::io::ReadableFile> file_stream;
    ASSIGN_ARROW_OR_THROW(file_stream,
                          arrow::io::ReadableFile::Open(file_name));
    ASSIGN_ARROW_OR_THROW(
        file_reader_,
        arrow::csv::StreamingReader::Make(
            arrow::io::default_io_context(), file_stream,
            arrow::csv::ReadOptions::Defaults(),
            arrow::csv::ParseOptions::Defaults(), convert_options_));
  }

 private:
  std::shared_ptr<arrow::csv::StreamingReader> file_reader_;
  arrow::csv::ConvertOptions convert_options_;
};

class ORCFileRead : public FileHelpRead {
 public:
  ORCFileRead(FileHelpRead::Options options)
      : FileHelpRead(options), current_stripe_(0) {
    for (auto& pair : options.column_types) {
      include_names_.push_back(pair.first);
    }
  }
  ~ORCFileRead() = default;

 public:
  void DoRead(std::shared_ptr<arrow::RecordBatch>* record_batch) {
    if (current_stripe_ >= orc_reader_->NumberOfStripes()) return;
    if (include_names_.empty()) {
      ASSIGN_ARROW_OR_THROW(*record_batch,
                            orc_reader_->ReadStripe(current_stripe_));
    } else {
      ASSIGN_ARROW_OR_THROW(
          *record_batch,
          orc_reader_->ReadStripe(current_stripe_, include_names_));
    }
    ++current_stripe_;
  }
  void DoClose() { CHECK_ARROW_OR_THROW(file_stream_->Close()); }
  std::shared_ptr<arrow::Schema> Schema() {
    std::shared_ptr<arrow::Schema> ret;
    ASSIGN_ARROW_OR_THROW(ret, orc_reader_->ReadSchema());
    return ret;
  }

 protected:
  void DoOpen(const std::string& file_name) {
    ASSIGN_ARROW_OR_THROW(file_stream_,
                          arrow::io::ReadableFile::Open(file_name));
    ASSIGN_ARROW_OR_THROW(orc_reader_,
                          arrow::adapters::orc::ORCFileReader::Open(
                              file_stream_, arrow::default_memory_pool()));
  }

 private:
  int64_t current_stripe_;
  std::unique_ptr<arrow::adapters::orc::ORCFileReader> orc_reader_;
  std::shared_ptr<arrow::io::ReadableFile> file_stream_;
  std::vector<std::string> include_names_;
};

std::unique_ptr<FileHelpRead> FileHelpRead::Make(
    proto::FileFormat file_format, const std::string& file_name,
    const FileHelpRead::Options& options) {
  std::unique_ptr<FileHelpRead> ret;
  switch (file_format) {
    case proto::FileFormat::CSV:
      ret = std::make_unique<CSVFileRead>(options);
      break;
    case proto::FileFormat::BINARY:
      ret = std::make_unique<BinaryFileRead>(options);
      break;
    case proto::FileFormat::ORC:
      ret = std::make_unique<ORCFileRead>(options);
      break;
    default:
      DATAPROXY_THROW("format[{}] not support.",
                      proto::FileFormat_Name<proto::FileFormat>(file_format));
      break;
  }
  ret->DoOpen(file_name);
  return ret;
}

FileHelpRead::Options FileHelpRead::Options::Defaults() {
  return FileHelpRead::Options();
}

}  // namespace dataproxy_sdk