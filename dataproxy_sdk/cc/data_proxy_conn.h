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

#include "arrow/flight/client.h"
#include "arrow/flight/types.h"
#include "arrow/type.h"

namespace dataproxy_sdk {

class DoPutResultWrapper {
 public:
  virtual void WriteRecordBatch(const arrow::RecordBatch& batch) = 0;
  virtual void Close() = 0;

 public:
  DoPutResultWrapper() = default;
  virtual ~DoPutResultWrapper() = default;
};

class FlightStreamReaderWrapper {
 public:
  virtual std::shared_ptr<arrow::RecordBatch> ReadRecordBatch() = 0;
  virtual std::shared_ptr<arrow::RecordBatch> ReadRecordBatch(size_t index) = 0;
  virtual std::shared_ptr<arrow::Schema> GetSchema() = 0;
  virtual size_t GetSize() = 0;

 public:
  FlightStreamReaderWrapper() = default;
  virtual ~FlightStreamReaderWrapper() = default;
};

class DataProxyConn {
 public:
  static std::unique_ptr<DataProxyConn> Connect(
      const std::string& host, bool use_tls,
      const arrow::flight::FlightClientOptions& options);

 public:
  DataProxyConn();
  ~DataProxyConn();

 public:
  std::unique_ptr<DoPutResultWrapper> DoPut(
      const arrow::flight::FlightDescriptor& descriptor,
      std::shared_ptr<arrow::Schema> schema);

  std::shared_ptr<FlightStreamReaderWrapper> DoGet(
      const arrow::flight::FlightDescriptor& descriptor);

  std::unique_ptr<arrow::flight::ResultStream> DoAction(
      const arrow::flight::Action& action);

  void Close();

 public:
 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dataproxy_sdk