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

#include "arrow/record_batch.h"

#include "dataproxy_sdk/data_proxy_pb.h"

namespace dataproxy_sdk {

class DataProxyStreamReader {
 public:
  virtual ~DataProxyStreamReader() = default;

 public:
  virtual void Get(std::shared_ptr<arrow::RecordBatch>* batch) = 0;
  virtual std::shared_ptr<arrow::Schema> Schema() = 0;
};

class DataProxyStreamWriter {
 public:
  virtual ~DataProxyStreamWriter() = default;

 public:
  virtual void Put(const std::shared_ptr<arrow::RecordBatch>& batch) = 0;
  virtual void Close() = 0;
};

class DataProxyStream : public std::enable_shared_from_this<DataProxyStream> {
 public:
  static std::shared_ptr<DataProxyStream> Make(
      const proto::DataProxyConfig& config);

  static std::shared_ptr<DataProxyStream> Make();

 public:
  DataProxyStream();
  ~DataProxyStream();

 public:
  std::unique_ptr<DataProxyStreamReader> GetReader(
      const proto::DownloadInfo& info);

  std::unique_ptr<DataProxyStreamReader> GetReader(const proto::SQLInfo& info);

  std::unique_ptr<DataProxyStreamWriter> GetWriter(proto::UploadInfo& info);

  void Close();

  void DeleteDomainData(const proto::UploadInfo& info);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dataproxy_sdk