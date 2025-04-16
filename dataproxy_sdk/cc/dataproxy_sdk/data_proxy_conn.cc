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

#include "data_proxy_conn.h"

#include <sstream>

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

class SimpleDoPutResult : public DoPutResultWrapper {
 public:
  void WriteRecordBatch(const arrow::RecordBatch& batch) {
    CHECK_ARROW_OR_THROW(stream_writer_->WriteRecordBatch(batch));
  }

  void Close() { CHECK_ARROW_OR_THROW(stream_writer_->Close()); }

 public:
  SimpleDoPutResult(arrow::flight::FlightClient::DoPutResult& result)
      : stream_writer_(std::move(result.writer)),
        metadata_reader_(std::move(result.reader)) {}
  ~SimpleDoPutResult() = default;

 private:
  //  a writer to write record batches to
  std::unique_ptr<arrow::flight::FlightStreamWriter> stream_writer_;
  //  a reader for application metadata from the server
  std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader_;
};

class SimpleFlightStreamReader : public FlightStreamReaderWrapper {
 public:
  enum ReadType { kInvalid = 0, kSerial, kParallel };

 public:
  void CheckReadType(ReadType read_type) {
    if (read_type_ == ReadType::kInvalid) {
      read_type_ = read_type;
      return;
    }
    if (read_type != read_type_) {
      DATAPROXY_THROW("CheckReadType error. now:{}, use:{}", (int)read_type_,
                      (int)read_type);
    }
  }

  std::shared_ptr<arrow::RecordBatch> ReadRecordBatch() {
    CheckReadType(ReadType::kSerial);

    arrow::flight::FlightStreamChunk chunk;
    if (index_ < stream_readers_.size()) {
      ASSIGN_ARROW_OR_THROW(chunk, stream_readers_[index_]->Next());
      if (chunk.data == nullptr && (++index_) < stream_readers_.size()) {
        ASSIGN_ARROW_OR_THROW(chunk, stream_readers_[index_]->Next());
      }
    }

    return chunk.data;
  }
  std::shared_ptr<arrow::RecordBatch> ReadRecordBatch(size_t index) {
    CheckReadType(ReadType::kParallel);

    arrow::flight::FlightStreamChunk chunk;

    DATAPROXY_ENFORCE(index < stream_readers_.size());

    ASSIGN_ARROW_OR_THROW(chunk, stream_readers_[index]->Next());

    return chunk.data;
  }
  std::shared_ptr<arrow::Schema> GetSchema() {
    DATAPROXY_ENFORCE(stream_readers_.size());
    ASSIGN_DP_OR_THROW(auto ret, stream_readers_.front()->GetSchema());
    return ret;
  }
  size_t GetSize() { return stream_readers_.size(); };

 public:
  SimpleFlightStreamReader(
      std::vector<std::unique_ptr<arrow::flight::FlightStreamReader>> streams)
      : stream_readers_(std::move(streams)),
        index_(0),
        read_type_(ReadType::kInvalid) {}
  virtual ~SimpleFlightStreamReader() = default;

 private:
  std::vector<std::unique_ptr<arrow::flight::FlightStreamReader>>
      stream_readers_;
  size_t index_;
  ReadType read_type_;
};

class DataProxyConn::Impl {
 private:
  struct GetFlightInfoResult {
    struct Data {
      arrow::flight::Ticket dp_ticket;
      std::unique_ptr<arrow::flight::FlightClient> dp_client;
    };

    std::vector<Data> datas;
  };

 public:
  void Connect(const std::string& host, bool use_tls,
               const arrow::flight::FlightClientOptions& options) {
    std::stringstream uri_string;
    if (use_tls) {
      uri_string << "grpc+tls://" << host;
    } else {
      uri_string << "grpc+tcp://" << host;
    }
    arrow::flight::Location location;
    ASSIGN_ARROW_OR_THROW(location,
                          arrow::flight::Location::Parse(uri_string.str()));

    ASSIGN_ARROW_OR_THROW(
        dm_client_, arrow::flight::FlightClient::Connect(location, options));
  }

  GetFlightInfoResult GetFlightInfo(
      const arrow::flight::FlightDescriptor& descriptor) {
    GetFlightInfoResult result;
    ASSIGN_DP_OR_THROW(auto flight_info, dm_client_->GetFlightInfo(descriptor));
    DATAPROXY_ENFORCE(flight_info->endpoints().size() > 0);
    for (const auto& endpoint : flight_info->endpoints()) {
      DATAPROXY_ENFORCE(endpoint.locations.size() > 0);
      GetFlightInfoResult::Data data;
      data.dp_ticket = std::move(endpoint.ticket);
      const auto& location = endpoint.locations.front();
      const auto& endpoint_url = location.ToString();
      if (endpoint_url.find("kuscia://") == std::string::npos) {
        ASSIGN_DP_OR_THROW(auto dp_client,
                           arrow::flight::FlightClient::Connect(location));
        data.dp_client = std::move(dp_client);
      }
      result.datas.emplace_back(std::move(data));
    }

    return result;
  }

  std::shared_ptr<FlightStreamReaderWrapper> DoGet(
      const arrow::flight::FlightDescriptor& descriptor) {
    GetFlightInfoResult result = GetFlightInfo(descriptor);

    std::vector<std::unique_ptr<arrow::flight::FlightStreamReader>>
        stream_readers;
    for (auto& data : result.datas) {
      std::unique_ptr<arrow::flight::FlightStreamReader> stream_reader;
      if (data.dp_client) {
        ASSIGN_ARROW_OR_THROW(stream_reader,
                              data.dp_client->DoGet(data.dp_ticket));
      } else {
        ASSIGN_ARROW_OR_THROW(stream_reader, dm_client_->DoGet(data.dp_ticket));
      }
      stream_readers.emplace_back(std::move(stream_reader));
    }

    DATAPROXY_ENFORCE(stream_readers.size());
    // Check that all schemas are the same
    ASSIGN_DP_OR_THROW(auto first_schema, stream_readers[0]->GetSchema());
    for (size_t i = 1; i < stream_readers.size(); ++i) {
      ASSIGN_DP_OR_THROW(auto schema, stream_readers[i]->GetSchema());
      DATAPROXY_ENFORCE(first_schema->Equals(schema));
    }

    return std::make_shared<SimpleFlightStreamReader>(
        std::move(stream_readers));
  }

  std::unique_ptr<DoPutResultWrapper> DoPut(
      const arrow::flight::FlightDescriptor& descriptor,
      std::shared_ptr<arrow::Schema> schema) {
    GetFlightInfoResult result = GetFlightInfo(descriptor);

    auto& data = result.datas.front();
    auto dp_descriptor =
        arrow::flight::FlightDescriptor::Command(data.dp_ticket.ticket);
    std::unique_ptr<arrow::flight::FlightClient> dp_client =
        std::move(data.dp_client);
    arrow::flight::FlightClient::DoPutResult put_result;
    if (dp_client) {
      ASSIGN_ARROW_OR_THROW(put_result,
                            dp_client->DoPut(dp_descriptor, schema));
    } else {
      ASSIGN_ARROW_OR_THROW(put_result,
                            dm_client_->DoPut(dp_descriptor, schema));
    }

    return std::make_unique<SimpleDoPutResult>(put_result);
  }

  std::unique_ptr<arrow::flight::ResultStream> DoAction(
      const arrow::flight::Action& action) {
    std::unique_ptr<arrow::flight::ResultStream> ret;
    ASSIGN_ARROW_OR_THROW(ret, dm_client_->DoAction(action));
    return ret;
  }

  void Close() { CHECK_ARROW_OR_THROW(dm_client_->Close()); }

 private:
  std::unique_ptr<arrow::flight::FlightClient> dm_client_;
};

DataProxyConn::DataProxyConn() {
  impl_ = std::make_unique<DataProxyConn::Impl>();
}
DataProxyConn::~DataProxyConn() = default;

std::unique_ptr<DataProxyConn> DataProxyConn::Connect(
    const std::string& host, bool use_tls,
    const arrow::flight::FlightClientOptions& options) {
  std::unique_ptr<DataProxyConn> ret = std::make_unique<DataProxyConn>();
  ret->impl_->Connect(host, use_tls, options);
  return ret;
}

std::unique_ptr<DoPutResultWrapper> DataProxyConn::DoPut(
    const arrow::flight::FlightDescriptor& descriptor,
    std::shared_ptr<arrow::Schema> schema) {
  return impl_->DoPut(descriptor, schema);
}

std::shared_ptr<FlightStreamReaderWrapper> DataProxyConn::DoGet(
    const arrow::flight::FlightDescriptor& descriptor) {
  return impl_->DoGet(descriptor);
}

std::unique_ptr<arrow::flight::ResultStream> DataProxyConn::DoAction(
    const arrow::flight::Action& action) {
  return impl_->DoAction(action);
}

void DataProxyConn::Close() { impl_->Close(); }

}  // namespace dataproxy_sdk