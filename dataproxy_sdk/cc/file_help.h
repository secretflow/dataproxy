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

#include "arrow/type.h"
#include "dataproxy_sdk/cc/data_proxy_pb.h"

namespace dataproxy_sdk {

class FileHelpBase {
 public:
  FileHelpBase() = default;
  virtual ~FileHelpBase() = default;

 public:
  virtual void DoClose() = 0;

 protected:
  virtual void DoOpen(const std::string& file_name) = 0;
};

class FileHelpWrite : public FileHelpBase {
 public:
  static std::unique_ptr<FileHelpWrite> Make(proto::FileFormat file_format,
                                             const std::string& file_name);

 public:
  FileHelpWrite() = default;
  virtual ~FileHelpWrite() = default;

 public:
  virtual void DoWrite(std::shared_ptr<arrow::RecordBatch>& record_batch) = 0;
};

class FileHelpRead : public FileHelpBase {
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
  static std::unique_ptr<FileHelpRead> Make(proto::FileFormat file_format,
                                            const std::string& file_name) {
    return Make(file_format, file_name, Options::Defaults());
  }

 public:
  explicit FileHelpRead(const Options& options){};
  virtual ~FileHelpRead() = default;

 public:
  virtual std::shared_ptr<arrow::Schema> Schema() = 0;
  virtual void DoRead(std::shared_ptr<arrow::RecordBatch>* record_batch) = 0;
};

}  // namespace dataproxy_sdk