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

#include "data_mesh_mock.h"

#include <iostream>
#include <thread>

#include "arrow/flight/api.h"
#include "arrow/table.h"
#include "spdlog/spdlog.h"

namespace dataproxy_sdk {

class DataMeshMockServer : public arrow::flight::FlightServerBase {
 public:
  DataMeshMockServer(bool open_dp) : open_dp_(open_dp) {}

 public:
  arrow::Status GetFlightInfo(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::FlightDescriptor &descriptor,
      std::unique_ptr<arrow::flight::FlightInfo> *info) override {
    SPDLOG_INFO("GetFlightInfo:{}", descriptor.ToString());
    ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo());
    *info = std::unique_ptr<arrow::flight::FlightInfo>(
        new arrow::flight::FlightInfo(std::move(flight_info)));

    return arrow::Status::OK();
  }

  arrow::Status DoPut(
      const arrow::flight::ServerCallContext &,
      std::unique_ptr<arrow::flight::FlightMessageReader> reader,
      std::unique_ptr<arrow::flight::FlightMetadataWriter>) override {
    ARROW_ASSIGN_OR_RAISE(table_, reader->ToTable());

    return arrow::Status::OK();
  }

  arrow::Status DoGet(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::Ticket &request,
      std::unique_ptr<arrow::flight::FlightDataStream> *stream) override {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::shared_ptr<arrow::RecordBatchReader> owning_reader;
    std::shared_ptr<arrow::Schema> schema;

    if (!table_) {
      return arrow::Status::Invalid("mock don't have data.");
    }

    arrow::TableBatchReader batch_reader(*table_);
    ARROW_ASSIGN_OR_RAISE(batches, batch_reader.ToRecordBatches());
    schema = table_->schema();

    ARROW_ASSIGN_OR_RAISE(owning_reader, arrow::RecordBatchReader::Make(
                                             std::move(batches), schema));
    *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
        new arrow::flight::RecordBatchStream(owning_reader));

    return arrow::Status::OK();
  }

  arrow::Status DoAction(
      const arrow::flight::ServerCallContext &,
      const arrow::flight::Action &action,
      std::unique_ptr<arrow::flight::ResultStream> *result) override {
    std::vector<arrow::flight::Result> results;
    ARROW_ASSIGN_OR_RAISE(auto flight_result,
                          arrow::flight::Result::Deserialize(""));
    results.push_back(flight_result);

    *result = std::unique_ptr<arrow::flight::ResultStream>(
        new arrow::flight::SimpleResultStream(std::move(results)));

    return arrow::Status::OK();
  }

 private:
  arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo() {
    auto descriptor = arrow::flight::FlightDescriptor::Command("");
    arrow::flight::FlightEndpoint endpoint;
    if (open_dp_) {
      endpoint.locations.push_back(location());
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto location, arrow::flight::Location::Parse("kuscia://datamesh"));
      endpoint.locations.push_back(location);
    }

    arrow::SchemaBuilder builder;
    ARROW_ASSIGN_OR_RAISE(auto schema, builder.Finish());

    return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, 0,
                                           0);
  }

  bool open_dp_;
  std::shared_ptr<arrow::Table> table_;
};

class DataMeshMock::Impl {
 public:
  arrow::Status StartServer(const std::string &dm_address, bool open_dp) {
    ARROW_ASSIGN_OR_RAISE(auto options, arrow::flight::Location::Parse(
                                            "grpc+tcp://" + dm_address));
    arrow::flight::FlightServerOptions server_location(options);
    server_ = std::make_shared<DataMeshMockServer>(open_dp);
    RETURN_NOT_OK(server_->Init(server_location));

    auto thread = std::thread(&DataMeshMockServer::Serve, server_);
    thread.detach();

    return arrow::Status::OK();
  }
  arrow::Status CloseServer() {
    if (server_) RETURN_NOT_OK(server_->Shutdown());

    return arrow::Status::OK();
  }

 public:
  Impl() = default;
  ~Impl() { auto status = CloseServer(); }

 private:
  std::shared_ptr<DataMeshMockServer> server_;
};

std::unique_ptr<DataMeshMock> DataMeshMock::Make() {
  return std::make_unique<DataMeshMock>();
}

DataMeshMock::DataMeshMock() { impl_ = std::make_unique<DataMeshMock::Impl>(); }

DataMeshMock::~DataMeshMock() = default;

arrow::Status DataMeshMock::StartServer(const std::string &dm_address,
                                        bool open_dp) {
  return impl_->StartServer(dm_address, open_dp);
}

arrow::Status DataMeshMock::CloseServer() { return impl_->CloseServer(); }

}  // namespace dataproxy_sdk
