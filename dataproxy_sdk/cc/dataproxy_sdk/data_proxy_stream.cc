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

#include "data_proxy_stream.h"

#include "arrow/buffer.h"
#include "arrow/flight/api.h"
#include "spdlog/spdlog.h"

#include "dataproxy_sdk/data_proxy_conn.h"
#include "dataproxy_sdk/exception.h"
#include "dataproxy_sdk/utils.h"

namespace dataproxy_sdk {

class SimpleStreamReader : public DataProxyStreamReader {
 public:
  explicit SimpleStreamReader(
      std::shared_ptr<FlightStreamReaderWrapper> wrapper)
      : DataProxyStreamReader(), wrapper_(std::move(wrapper)) {}

 public:
  void Get(std::shared_ptr<arrow::RecordBatch>* batch) {
    *batch = wrapper_->ReadRecordBatch();
  }

  std::shared_ptr<arrow::Schema> Schema() { return wrapper_->GetSchema(); }

 private:
  std::shared_ptr<FlightStreamReaderWrapper> wrapper_;
};

class SimpleStreamWriter : public DataProxyStreamWriter {
 public:
  SimpleStreamWriter(std::unique_ptr<DoPutResultWrapper> wrapper,
                     const proto::UploadInfo& upload_info)
      : wrapper_(std::move(wrapper)),
        upload_info_(upload_info),
        closed_(false) {}
  virtual ~SimpleStreamWriter() { Close(); };

  void SetStream(std::shared_ptr<DataProxyStream> stream) {
    stream_ = std::move(stream);
  }

 public:
  void Put(const std::shared_ptr<arrow::RecordBatch>& batch) {
    try {
      wrapper_->WriteRecordBatch(*batch);
    } catch (...) {
      try {
        if (stream_) {
          stream_->DeleteDomainData(upload_info_);
        }
      } catch (const std::exception& e) {
        SPDLOG_WARN("DeleteDomainData error. msg:{}", e.what());
      }
      throw;
    }
  }

  void Close() {
    if (!closed_) {
      wrapper_->Close();
      closed_ = true;
    }
  }

 private:
  std::unique_ptr<DoPutResultWrapper> wrapper_;
  std::shared_ptr<DataProxyStream> stream_;
  const proto::UploadInfo upload_info_;
  bool closed_;
};

class DataProxyStream::Impl {
 public:
  void Init(const proto::DataProxyConfig& config) {
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

  std::unique_ptr<DataProxyStreamReader> GetReader(
      const google::protobuf::Any& any) {
    auto descriptor =
        arrow::flight::FlightDescriptor::Command(any.SerializeAsString());
    auto stream_reader = dp_conn_->DoGet(descriptor);

    return std::make_unique<SimpleStreamReader>(std::move(stream_reader));
  }

  std::shared_ptr<arrow::Schema> BuildWriterSchema(
      const proto::UploadInfo& info) {
    arrow::SchemaBuilder schema_builder;
    for (auto& column : info.columns()) {
      CHECK_ARROW_OR_THROW(schema_builder.AddField(arrow::field(
          column.name(), GetDataType(column.type()), !column.not_nullable())));
    }
    ASSIGN_DP_OR_THROW(auto ret, schema_builder.Finish())
    return ret;
  }

  void CreateDomainData(proto::UploadInfo& info,
                        proto::FileFormat file_format) {
    auto action_msg = BuildActionCreateDomainDataRequest(info, file_format);
    arrow::flight::Action action{"ActionCreateDomainDataRequest",
                                 arrow::Buffer::FromString(action_msg)};
    auto result_stream = dp_conn_->DoAction(action);

    std::unique_ptr<arrow::flight::Result> result;
    ASSIGN_ARROW_OR_THROW(result, result_stream->Next());

    auto response_domaindata_id =
        GetDomaindataIdFromResponse(result->body->ToString());
    if (info.domaindata_id().empty()) {
      info.set_domaindata_id(response_domaindata_id);
      SPDLOG_INFO("DP create domaindata id:{}", info.domaindata_id());
    } else if (response_domaindata_id != info.domaindata_id()) {
      DATAPROXY_THROW("domaindata id error, request:{}, response:{}",
                      info.domaindata_id(), response_domaindata_id);
    }
  }

  void DeleteDomainData(const proto::UploadInfo& info) {
    auto action_request = BuildActionDeleteDomainDataRequest(info);
    arrow::flight::Action action{"ActionDeleteDomainDataRequest",
                                 arrow::Buffer::FromString(action_request)};
    auto result = dp_conn_->DoAction(action);
  }

  std::unique_ptr<SimpleStreamWriter> GetWriter(proto::UploadInfo& info) {
    auto file_format = proto::FileFormat::CSV;
    if (info.type() != "table") {
      file_format = proto::FileFormat::BINARY;
    }

    CheckUploadInfo(info);
    CreateDomainData(info, file_format);

    try {
      // 1. 从dm获取dp信息
      auto any = BuildUploadAny(info, file_format);

      // 2. 通过dm返回的dp信息连接dp
      auto descriptor =
          arrow::flight::FlightDescriptor::Command(any.SerializeAsString());

      auto schema = BuildWriterSchema(info);

      auto put_result = dp_conn_->DoPut(descriptor, schema);

      return std::make_unique<SimpleStreamWriter>(std::move(put_result), info);
    } catch (...) {
      try {
        DeleteDomainData(info);
      } catch (const std::exception& e) {
        SPDLOG_WARN("DeleteDomainData error. msg:{}", e.what());
      }
      throw;
    }
  }

  void Close() { dp_conn_->Close(); }

 private:
  std::unique_ptr<DataProxyConn> dp_conn_;
};

std::shared_ptr<DataProxyStream> DataProxyStream::Make(
    const proto::DataProxyConfig& config) {
  proto::DataProxyConfig dp_config = config;
  GetDPConfigValueFromEnv(&dp_config);

  std::shared_ptr<DataProxyStream> ret = std::make_shared<DataProxyStream>();
  ret->impl_->Init(dp_config);
  return ret;
}

std::shared_ptr<DataProxyStream> DataProxyStream::Make() {
  proto::DataProxyConfig config;
  return DataProxyStream::Make(config);
}

DataProxyStream::DataProxyStream() {
  impl_ = std::make_unique<DataProxyStream::Impl>();
}

DataProxyStream::~DataProxyStream() = default;

std::unique_ptr<DataProxyStreamReader> DataProxyStream::GetReader(
    const proto::DownloadInfo& info) {
  // TODO:
  // proto::FileFormat::CSV暂时固定，因为在read时dp的odps没有用到这个字段
  // 后续DataProxy会在CommandDomainDataQuery会调整该参数
  auto any = BuildDownloadAny(info, proto::FileFormat::CSV);

  return impl_->GetReader(any);
}

std::unique_ptr<DataProxyStreamReader> DataProxyStream::GetReader(
    const proto::SQLInfo& info) {
  auto any = BuildSQLAny(info);

  return impl_->GetReader(any);
}

std::unique_ptr<DataProxyStreamWriter> DataProxyStream::GetWriter(
    proto::UploadInfo& info) {
  auto ret = impl_->GetWriter(info);
  ret->SetStream(shared_from_this());
  return ret;
}

void DataProxyStream::Close() { impl_->Close(); }

void DataProxyStream::DeleteDomainData(const proto::UploadInfo& info) {
  impl_->DeleteDomainData(info);
  SPDLOG_WARN("stream write error. upload_info:{}", info.DebugString());
}

}  // namespace dataproxy_sdk
