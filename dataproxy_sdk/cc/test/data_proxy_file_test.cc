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

#include "dataproxy_sdk/data_proxy_file.h"

#include <random>
#include <string>

#include "gtest/gtest.h"
#include "test/tools/data_mesh_mock.h"
#include "test/tools/random.h"
#include "test/tools/utils.h"

#include "dataproxy_sdk/exception.h"
#include "dataproxy_sdk/file_help.h"

namespace dataproxy_sdk {

class TestDataProxyFile : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21011"));

    dataproxy_sdk::proto::DataProxyConfig sdk_config;
    sdk_config.set_data_proxy_addr(data_mesh_->GetServerAddress());
    data_proxy_file_ = DataProxyFile::Make(sdk_config);

    data_ = RandomBatchGenerator::ExampleGenerate();
  }

 protected:
  std::shared_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
  std::unique_ptr<DataProxyFile> data_proxy_file_;
};

TEST_F(TestDataProxyFile, UploadAndDownload) {
  const std::string upload_file = "tmp_upload.orc";
  const std::string download_file = "tmp_download.orc";
  auto write_options = FileHelpWrite::Options::Defaults();
  auto file_writer = FileHelpWrite::Make(GetFileFormat(upload_file),
                                         upload_file, write_options);
  file_writer->DoWrite(data_);
  file_writer->DoClose();

  proto::UploadInfo upload_info;
  upload_info.set_domaindata_id("");
  upload_info.set_type("table");
  for (const auto& field : data_->schema()->fields()) {
    auto column = upload_info.add_columns();
    column->set_name(field->name());
    column->set_type(field->type()->name());
  }
  data_proxy_file_->UploadFile(upload_info, upload_file,
                               GetFileFormat(upload_file));

  proto::DownloadInfo download_info;
  download_info.set_domaindata_id("test");
  data_proxy_file_->DownloadFile(download_info, download_file,
                                 GetFileFormat(download_file));
  data_proxy_file_->Close();

  auto read_options = FileHelpRead::Options::Defaults();
  auto file_reader = FileHelpRead::Make(GetFileFormat(download_file),
                                        download_file, read_options);
  std::shared_ptr<arrow::RecordBatch> result_batch;
  file_reader->DoRead(&result_batch);
  file_reader->DoClose();

  std::cout << data_->ToString() << std::endl;
  std::cout << result_batch->ToString() << std::endl;

  EXPECT_TRUE(data_->Equals(*result_batch));
}

class TestDataProxyFileEmpty : public ::testing::Test {
 public:
  void SetUp() {
    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21012"));

    dataproxy_sdk::proto::DataProxyConfig sdk_config;
    sdk_config.set_data_proxy_addr(data_mesh_->GetServerAddress());
    data_proxy_file_ = DataProxyFile::Make(sdk_config);

    data_ = RandomBatchGenerator::ExampleGenerate(0);
  }

 protected:
  std::shared_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
  std::unique_ptr<DataProxyFile> data_proxy_file_;
};

TEST_F(TestDataProxyFileEmpty, UploadAndDownload) {
  const std::string upload_file = "empty_upload.orc";
  const std::string download_file = "empty_download.orc";
  auto write_options = FileHelpWrite::Options::Defaults();
  auto file_writer = FileHelpWrite::Make(GetFileFormat(upload_file),
                                         upload_file, write_options);
  file_writer->DoWrite(data_);
  file_writer->DoClose();

  proto::UploadInfo upload_info;
  upload_info.set_domaindata_id("");
  upload_info.set_type("table");
  for (const auto& field : data_->schema()->fields()) {
    auto column = upload_info.add_columns();
    column->set_name(field->name());
    column->set_type(field->type()->name());
  }
  data_proxy_file_->UploadFile(upload_info, upload_file,
                               GetFileFormat(upload_file));

  proto::DownloadInfo download_info;
  download_info.set_domaindata_id("test");
  data_proxy_file_->DownloadFile(download_info, download_file,
                                 GetFileFormat(download_file));
  data_proxy_file_->Close();

  auto read_options = FileHelpRead::Options::Defaults();
  auto file_reader = FileHelpRead::Make(GetFileFormat(download_file),
                                        download_file, read_options);
  std::shared_ptr<arrow::RecordBatch> result_batch;
  file_reader->DoRead(&result_batch);

  EXPECT_TRUE(file_reader->Schema()->Equals(data_->schema()));
  file_reader->DoClose();

  EXPECT_TRUE(result_batch == nullptr);
}

class TestDataProxyFileUseDP : public ::testing::Test {
 public:
  void SetUp() {
    std::mt19937 rnd(std::random_device{}());
    std::uniform_int_distribution<> dist(1, 10);
    dp_num_ = dist(rnd);

    data_mesh_ = DataMeshMock::Make();
    CHECK_ARROW_OR_THROW(data_mesh_->StartServer("127.0.0.1:21013", dp_num_));

    dataproxy_sdk::proto::DataProxyConfig sdk_config;
    sdk_config.set_data_proxy_addr(data_mesh_->GetServerAddress());
    data_proxy_file_ = DataProxyFile::Make(sdk_config);

    data_ = RandomBatchGenerator::ExampleGenerate();
  }

 protected:
  std::shared_ptr<DataMeshMock> data_mesh_;
  std::shared_ptr<arrow::RecordBatch> data_;
  std::unique_ptr<DataProxyFile> data_proxy_file_;
  int dp_num_;
};

TEST_F(TestDataProxyFileUseDP, UploadAndDownload) {
  const std::string upload_file = "tmp_multi_upload.orc";
  const std::string download_file = "tmp_multi_download.orc";
  auto write_options = FileHelpWrite::Options::Defaults();
  auto file_writer = FileHelpWrite::Make(GetFileFormat(upload_file),
                                         upload_file, write_options);
  file_writer->DoWrite(data_);
  file_writer->DoClose();

  proto::UploadInfo upload_info;
  upload_info.set_domaindata_id("");
  upload_info.set_type("table");
  for (const auto& field : data_->schema()->fields()) {
    auto column = upload_info.add_columns();
    column->set_name(field->name());
    column->set_type(field->type()->name());
  }
  data_proxy_file_->UploadFile(upload_info, upload_file,
                               GetFileFormat(upload_file));

  proto::DownloadInfo download_info;
  download_info.set_domaindata_id("test");
  data_proxy_file_->DownloadFile(download_info, download_file,
                                 GetFileFormat(download_file));
  data_proxy_file_->Close();

  auto read_options = FileHelpRead::Options::Defaults();
  auto file_reader = FileHelpRead::Make(GetFileFormat(download_file),
                                        download_file, read_options);
  std::shared_ptr<arrow::RecordBatch> result_batch;
  file_reader->DoRead(&result_batch);
  file_reader->DoClose();

  std::cout << data_->ToString() << std::endl;
  std::cout << result_batch->ToString() << std::endl;

  // dm mock中每个dp都返回全部数据副本，所以下载得到文件数据是原数据的dp_num_倍
  for (int64_t i = 0; i < data_->num_rows() * dp_num_; i += data_->num_rows()) {
    ASSERT_TRUE(result_batch->Slice(i, data_->num_rows())->Equals(*data_));
  }
}

}  // namespace dataproxy_sdk
