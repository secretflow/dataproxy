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

#include "dataproxy_sdk/cc/data_proxy_conn.h"

#include <iostream>
#include <thread>

#include "gtest/gtest.h"

#include "dataproxy_sdk/cc/exception.h"
#include "dataproxy_sdk/test/data_mesh_mock.h"
#include "dataproxy_sdk/test/random.h"

namespace dataproxy_sdk {

static const std::string kDataMeshAddress = "127.0.0.1:23333";
static const std::string kDataProxyAddress = "127.0.0.1:23334";

class TestDataProxyConn : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer(kDataMeshAddress));

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
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer(kDataProxyAddress, true));

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
  auto result = DataProxyConnPutAndGet(kDataMeshAddress, data_);

  EXPECT_TRUE(data_->Equals(*result));
}

TEST_F(TestDataProxyConnUseDP, PutAndGet) {
  auto result = DataProxyConnPutAndGet(kDataProxyAddress, data_);

  EXPECT_TRUE(data_->Equals(*result));
}

}  // namespace dataproxy_sdk
