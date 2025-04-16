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

#include "dataproxy_sdk/data_proxy_stream.h"

#include <random>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "test/tools/data_mesh_mock.h"
#include "test/tools/random.h"

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

class TestDataProxyStream : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21021"));

    dataproxy_sdk::proto::DataProxyConfig sdk_config;
    sdk_config.set_data_proxy_addr(data_mesh_->GetServerAddress());
    data_proxy_stream_ = DataProxyStream::Make(sdk_config);

    data_ = RandomBatchGenerator::ExampleGenerate();
  }

 protected:
  std::shared_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
  std::shared_ptr<DataProxyStream> data_proxy_stream_;
};

TEST_F(TestDataProxyStream, PutAndGet) {
  proto::UploadInfo upload_info;
  upload_info.set_domaindata_id("");
  upload_info.set_type("table");
  for (const auto& field : data_->schema()->fields()) {
    auto column = upload_info.add_columns();
    column->set_name(field->name());
    column->set_type(field->type()->name());
  }
  auto writer = data_proxy_stream_->GetWriter(upload_info);
  writer->Put(data_);
  writer->Close();

  proto::DownloadInfo download_info;
  download_info.set_domaindata_id("test");
  auto reader = data_proxy_stream_->GetReader(download_info);
  std::shared_ptr<arrow::RecordBatch> result_batch;
  reader->Get(&result_batch);

  EXPECT_TRUE(data_->Equals(*result_batch));

  proto::SQLInfo sql_info;
  sql_info.set_datasource_id("test");
  sql_info.set_sql("select * from test;");
  reader = data_proxy_stream_->GetReader(sql_info);
  std::shared_ptr<arrow::RecordBatch> sql_batch;
  reader->Get(&sql_batch);

  EXPECT_TRUE(data_->Equals(*sql_batch));
}

class TestDataProxyStreamUseDP : public ::testing::Test {
 public:
  void SetUp() {
    std::mt19937 rnd(std::random_device{}());
    std::uniform_int_distribution<> dist(1, 10);
    dp_num_ = dist(rnd);
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21022", dp_num_));

    dataproxy_sdk::proto::DataProxyConfig sdk_config;
    sdk_config.set_data_proxy_addr(data_mesh_->GetServerAddress());
    data_proxy_stream_ = DataProxyStream::Make(sdk_config);

    data_ = RandomBatchGenerator::ExampleGenerate();
  }

 protected:
  std::shared_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
  std::shared_ptr<DataProxyStream> data_proxy_stream_;
  int dp_num_;
};

TEST_F(TestDataProxyStreamUseDP, PutAndGet) {
  proto::UploadInfo upload_info;
  upload_info.set_domaindata_id("");
  upload_info.set_type("table");
  for (const auto& field : data_->schema()->fields()) {
    auto column = upload_info.add_columns();
    column->set_name(field->name());
    column->set_type(field->type()->name());
  }
  auto writer = data_proxy_stream_->GetWriter(upload_info);
  writer->Put(data_);
  writer->Close();

  proto::DownloadInfo download_info;
  download_info.set_domaindata_id("test");
  auto reader = data_proxy_stream_->GetReader(download_info);

  // dm mock中每个dp都返回全部数据副本，所以得到的数据是原数据的dp_num_倍
  for (int64_t i = 0; i < dp_num_; ++i) {
    std::shared_ptr<arrow::RecordBatch> result_batch;
    reader->Get(&result_batch);
    ASSERT_TRUE(result_batch->Equals(*data_));
  }
}

}  // namespace dataproxy_sdk
