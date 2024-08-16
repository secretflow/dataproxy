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

#include "dataproxy_sdk/cc/data_proxy_file.h"

#include <fstream>
#include <iostream>

#include "arrow/buffer.h"
#include "arrow/flight/api.h"
#include "dataproxy_sdk/cc/data_proxy_conn.h"
#include "dataproxy_sdk/cc/exception.h"
#include "dataproxy_sdk/cc/file_help.h"
#include "dataproxy_sdk/cc/utils.h"
#include "spdlog/spdlog.h"

namespace dataproxy_sdk {

class DataProxyFile::Impl {
 public:
  void Init(const proto::DataProxyConfig &config) {
    arrow::flight::FlightClientOptions options =
        arrow::flight::FlightClientOptions::Defaults();
    if (config.has_tls_config()) {
      options.private_key =
          ReadFileContent(config.tls_config().private_key_path());
      options.cert_chain =
          ReadFileContent(config.tls_config().certificate_path());
      options.tls_root_certs =
          ReadFileContent(config.tls_config().ca_file_path());
    }

    dp_conn_ = DataProxyConn::Connect(config.data_proxy_addr(),
                                      config.has_tls_config(), options);
  }

  void DownloadFile(const proto::DownloadInfo &info,
                    const std::string &file_path,
                    proto::FileFormat file_format) {
    // 1. 从dm获取dp信息
    auto any = BuildDownloadAny(info, file_format);

    // 2. 连接dp
    auto descriptor =
        arrow::flight::FlightDescriptor::Command(any.SerializeAsString());
    auto stream_reader = dp_conn_->DoGet(descriptor);
    // 4. 从读取流下载数据

    std::unique_ptr<FileHelpWrite> file_write =
        FileHelpWrite::Make(file_format, file_path);
    while (true) {
      auto record_batch = stream_reader->ReadRecordBatch();
      if (record_batch == nullptr) {
        // read finished
        break;
      }
      file_write->DoWrite(record_batch);
    }

    file_write->DoClose();
  }

  FileHelpRead::Options BuildReadOptions(const proto::UploadInfo &info) {
    FileHelpRead::Options options = FileHelpRead::Options::Defaults();
    for (auto &column : info.columns()) {
      options.column_types.emplace(column.name(), GetDataType(column.type()));
    }
    return options;
  }

  void DoUpload(const proto::UploadInfo &info, const std::string &file_path,
                proto::FileFormat file_format) {
    // 2. 通过dm返回的dp信息连接dp
    auto any = BuildUploadAny(info, file_format);

    auto descriptor =
        arrow::flight::FlightDescriptor::Command(any.SerializeAsString());
    // 3. 打开文件读取流
    auto read_options = BuildReadOptions(info);
    std::unique_ptr<FileHelpRead> file_read =
        FileHelpRead::Make(file_format, file_path, read_options);

    auto put_result = dp_conn_->DoPut(descriptor, file_read->Schema());

    // 5. 向写入流写入文件数据
    while (true) {
      std::shared_ptr<arrow::RecordBatch> batch;
      file_read->DoRead(&batch);
      if (batch.get() == nullptr) {
        break;
      }
      put_result->WriteRecordBatch(*batch);
    }

    put_result->Close();
    file_read->DoClose();
  }

  void CreateDomainData(proto::UploadInfo &info,
                        proto::FileFormat file_format) {
    auto action_msg = BuildActionCreateDomainDataRequest(info, file_format);
    arrow::flight::Action action{
        "ActionCreateDomainDataRequest",
        arrow::Buffer::FromString(action_msg.SerializeAsString())};
    auto result_stream = dp_conn_->DoAction(action);

    std::unique_ptr<arrow::flight::Result> result;
    ASSIGN_ARROW_OR_THROW(result, result_stream->Next());

    auto response = GetActionCreateDomainDataResponse(result->body->ToString());
    CHECK_RESP_OR_THROW(response);
    if (info.domaindata_id().empty()) {
      info.set_domaindata_id(response.data().domaindata_id());
    } else if (response.data().domaindata_id() != info.domaindata_id()) {
      DATAPROXY_THROW("domaindata id error, request:{}, response:{}",
                      info.domaindata_id(), response.data().domaindata_id());
    }
  }

  void DeleteDomainData(const proto::UploadInfo &info) {
    auto action_request = BuildActionDeleteDomainDataRequest(info);
    arrow::flight::Action action{
        "ActionDeleteDomainDataRequest",
        arrow::Buffer::FromString(action_request.SerializeAsString())};
    auto result = dp_conn_->DoAction(action);
  }

  void UploadFile(proto::UploadInfo &info, const std::string &file_path,
                  proto::FileFormat file_format) {
    dataproxy_sdk::CheckUploadInfo(info);
    CreateDomainData(info, file_format);
    try {
      DoUpload(info, file_path, file_format);
    } catch (...) {
      try {
        DeleteDomainData(info);
      } catch (const std::exception &e) {
        SPDLOG_WARN("DeleteDomainData error. msg:{}", e.what());
      }
      throw;
    }
  }

  void Close() { dp_conn_->Close(); }

 private:
  std::unique_ptr<DataProxyConn> dp_conn_;
};

std::unique_ptr<DataProxyFile> DataProxyFile::Make(
    const proto::DataProxyConfig &config) {
  std::unique_ptr<DataProxyFile> ret = std::make_unique<DataProxyFile>();
  ret->impl_->Init(config);
  return ret;
}

DataProxyFile::DataProxyFile() {
  impl_ = std::make_unique<DataProxyFile::Impl>();
}

DataProxyFile::~DataProxyFile() = default;

void DataProxyFile::DownloadFile(const proto::DownloadInfo &info,
                                 const std::string &file_path,
                                 proto::FileFormat file_format) {
  impl_->DownloadFile(info, file_path, file_format);
}

void DataProxyFile::UploadFile(proto::UploadInfo &info,
                               const std::string &file_path,
                               proto::FileFormat file_format) {
  impl_->UploadFile(info, file_path, file_format);
}

void DataProxyFile::Close() { impl_->Close(); }

}  // namespace dataproxy_sdk
