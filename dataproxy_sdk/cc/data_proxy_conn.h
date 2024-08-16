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
  void WriteRecordBatch(const arrow::RecordBatch& batch);
  void Close();

 public:
  DoPutResultWrapper(arrow::flight::FlightClient::DoPutResult& result,
                     std::unique_ptr<arrow::flight::FlightClient> client)
      : stream_writer_(std::move(result.writer)),
        metadata_reader_(std::move(result.reader)),
        dp_client_(std::move(client)) {}
  ~DoPutResultWrapper() = default;

 private:
  //  a writer to write record batches to
  std::unique_ptr<arrow::flight::FlightStreamWriter> stream_writer_;
  //  a reader for application metadata from the server
  std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader_;
  //  If the dp is deployed alone, use client to manage lifecycle, not for
  //  public use
  std::unique_ptr<arrow::flight::FlightClient> dp_client_;
};

class FlightStreamReaderWrapper {
 public:
  std::shared_ptr<arrow::RecordBatch> ReadRecordBatch();

 public:
  FlightStreamReaderWrapper(
      std::unique_ptr<arrow::flight::FlightStreamReader> stream,
      std::unique_ptr<arrow::flight::FlightClient> client)
      : stream_reader_(std::move(stream)), dp_client_(std::move(client)) {}
  ~FlightStreamReaderWrapper() = default;

 private:
  std::unique_ptr<arrow::flight::FlightStreamReader> stream_reader_;
  //  If the dp is deployed alone, use client to manage lifecycle, not for
  //  public use
  std::unique_ptr<arrow::flight::FlightClient> dp_client_;
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

  std::unique_ptr<FlightStreamReaderWrapper> DoGet(
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