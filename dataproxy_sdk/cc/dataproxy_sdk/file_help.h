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

#pragma once

#include <memory>
#include <unordered_map>

#include "arrow/csv/options.h"
#include "arrow/type.h"
#include "arrow/util/type_fwd.h"

#include "dataproxy_sdk/data_proxy_pb.h"

namespace dataproxy_sdk {

class FileHelpWrite {
 public:
  struct Options {
    // only orc use by sf
    arrow::Compression::type compression = arrow::Compression::UNCOMPRESSED;
    // only orc use by sf
    int64_t compression_block_size = 64 * 1024;
    // only orc use by sf
    int64_t stripe_size = 64 * 1024 * 1024;
    // Both CSV and ORC use this parameter.
    // The default value is the same as WriteOptions in arrow.
    int64_t batch_size = 1024;
    bool csv_include_header = true;
    arrow::csv::QuotingStyle csv_quoting_style =
        arrow::csv::QuotingStyle::Needed;
    static Options Defaults();
  };

 public:
  static std::unique_ptr<FileHelpWrite> Make(proto::FileFormat file_format,
                                             const std::string& file_name,
                                             const Options& options);

 public:
  FileHelpWrite() = default;
  virtual ~FileHelpWrite() = default;

 public:
  virtual void DoOpen(const std::string& file_name, const Options& options) = 0;
  virtual void DoClose() = 0;
  virtual void DoWrite(std::shared_ptr<arrow::RecordBatch>& record_batch) = 0;
};

class FileHelpRead {
 public:
  struct Options {
    std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
        column_types;

    static Options Defaults();
  };

 public:
  static std::unique_ptr<FileHelpRead> Make(proto::FileFormat file_format,
                                            const std::string& file_name,
                                            const Options& options);

 public:
  FileHelpRead() = default;
  virtual ~FileHelpRead() = default;

 public:
  virtual void DoOpen(const std::string& file_name, const Options& options) = 0;
  virtual void DoClose() = 0;
  virtual void DoRead(std::shared_ptr<arrow::RecordBatch>* record_batch) = 0;
  virtual std::shared_ptr<arrow::Schema> Schema() = 0;
};

}  // namespace dataproxy_sdk