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

#include "dataproxy_sdk/data_proxy_conn.h"

#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "test/tools/data_mesh_mock.h"
#include "test/tools/random.h"

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

class TestDataProxyConn : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21001"));

    data_ = RandomBatchGenerator::ExampleGenerate();
  }

 protected:
  std::shared_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
};

class TestDataProxyConnUseDP : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21002", 1));

    data_ = RandomBatchGenerator::ExampleGenerate();
  }

 protected:
  std::unique_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
};

std::shared_ptr<arrow::RecordBatch> DataProxyConnPutAndGet(
    const std::string& ip, const std::shared_ptr<arrow::RecordBatch>& batch) {
  arrow::flight::FlightClientOptions options =
      arrow::flight::FlightClientOptions::Defaults();
  auto dp_conn = DataProxyConn::Connect(ip, false, options);
  auto descriptor = arrow::flight::FlightDescriptor::Command("");

  auto put_result = dp_conn->DoPut(descriptor, batch->schema());
  put_result->WriteRecordBatch(*batch);
  put_result->Close();

  std::shared_ptr<arrow::RecordBatch> result_batch;
  auto get_result = dp_conn->DoGet(descriptor);
  result_batch = get_result->ReadRecordBatch();

  dp_conn->Close();
  return result_batch;
}

TEST_F(TestDataProxyConn, PutAndGet) {
  auto result = DataProxyConnPutAndGet(data_mesh_->GetServerAddress(), data_);

  EXPECT_TRUE(data_->Equals(*result));
}

TEST_F(TestDataProxyConnUseDP, PutAndGet) {
  auto result = DataProxyConnPutAndGet(data_mesh_->GetServerAddress(), data_);

  EXPECT_TRUE(data_->Equals(*result));
}

}  // namespace dataproxy_sdk
