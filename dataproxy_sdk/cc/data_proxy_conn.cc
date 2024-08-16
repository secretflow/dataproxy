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

#include "dataproxy_sdk/cc/exception.h"

namespace dataproxy_sdk {

class DataProxyConn::Impl {
 private:
  struct GetFlightInfoResult {
    std::unique_ptr<arrow::flight::FlightInfo> dp_info;
    //  单独部署的dp的连接
    std::unique_ptr<arrow::flight::FlightClient> dp_client;
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
    ASSIGN_ARROW_OR_THROW(result.dp_info,
                          dm_client_->GetFlightInfo(descriptor));

    // 2. 获取dp地址
    const arrow::flight::Location& location =
        result.dp_info->endpoints().front().locations.front();
    std::string dp_url = location.ToString();
    // 如果dp没有单独部署，后续通过dm进行相关操作
    if (dp_url.find("kuscia://") == std::string::npos) {
      ASSIGN_ARROW_OR_THROW(result.dp_client,
                            arrow::flight::FlightClient::Connect(location));
    }

    return result;
  }

  std::unique_ptr<FlightStreamReaderWrapper> DoGet(
      const arrow::flight::FlightDescriptor& descriptor) {
    GetFlightInfoResult result = GetFlightInfo(descriptor);

    std::unique_ptr<arrow::flight::FlightClient> dp_client =
        std::move(result.dp_client);
    std::unique_ptr<arrow::flight::FlightStreamReader> stream_reader;
    if (dp_client) {
      ASSIGN_ARROW_OR_THROW(
          stream_reader,
          dp_client->DoGet(result.dp_info->endpoints().front().ticket));
    } else {
      ASSIGN_ARROW_OR_THROW(
          stream_reader,
          dm_client_->DoGet(result.dp_info->endpoints().front().ticket));
    }

    return std::make_unique<FlightStreamReaderWrapper>(std::move(stream_reader),
                                                       std::move(dp_client));
  }

  std::unique_ptr<DoPutResultWrapper> DoPut(
      const arrow::flight::FlightDescriptor& descriptor,
      std::shared_ptr<arrow::Schema> schema) {
    GetFlightInfoResult result = GetFlightInfo(descriptor);

    auto dp_descriptor = arrow::flight::FlightDescriptor::Command(
        result.dp_info->endpoints().front().ticket.ticket);
    std::unique_ptr<arrow::flight::FlightClient> dp_client =
        std::move(result.dp_client);
    arrow::flight::FlightClient::DoPutResult put_result;
    if (dp_client) {
      ASSIGN_ARROW_OR_THROW(put_result,
                            dp_client->DoPut(dp_descriptor, schema));
    } else {
      ASSIGN_ARROW_OR_THROW(put_result,
                            dm_client_->DoPut(dp_descriptor, schema));
    }

    return std::make_unique<DoPutResultWrapper>(put_result,
                                                std::move(dp_client));
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

void DoPutResultWrapper::WriteRecordBatch(const arrow::RecordBatch& batch) {
  CHECK_ARROW_OR_THROW(stream_writer_->WriteRecordBatch(batch));
}

void DoPutResultWrapper::Close() {
  CHECK_ARROW_OR_THROW(stream_writer_->Close());
}

std::shared_ptr<arrow::RecordBatch>
FlightStreamReaderWrapper::ReadRecordBatch() {
  arrow::flight::FlightStreamChunk chunk;
  ASSIGN_ARROW_OR_THROW(chunk, stream_reader_->Next());
  return chunk.data;
}

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

std::unique_ptr<FlightStreamReaderWrapper> DataProxyConn::DoGet(
    const arrow::flight::FlightDescriptor& descriptor) {
  return impl_->DoGet(descriptor);
}

std::unique_ptr<arrow::flight::ResultStream> DataProxyConn::DoAction(
    const arrow::flight::Action& action) {
  return impl_->DoAction(action);
}

void DataProxyConn::Close() { impl_->Close(); }

}  // namespace dataproxy_sdk