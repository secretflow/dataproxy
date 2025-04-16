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

#include "data_proxy_pb.h"

#include "dataproxy_sdk/exception.h"

#include "kuscia/proto/api/v1alpha1/datamesh/flightdm.pb.h"

namespace dataproxy_sdk {

namespace dm_proto = kuscia::proto::api::v1alpha1::datamesh;
namespace kuscia_proto = kuscia::proto::api::v1alpha1;

inline dm_proto::ContentType FormatToContentType(proto::FileFormat format) {
  switch (format) {
    case proto::FileFormat::BINARY:
      return dm_proto::ContentType::RAW;
    case proto::FileFormat::CSV:
    case proto::FileFormat::ORC:
      return dm_proto::ContentType::Table;
    default:
      DATAPROXY_THROW("do not support this type of format:{}",
                      proto::FileFormat_Name(format));
  }
}

inline kuscia_proto::FileFormat ChangeToKusciaFileFormat(
    proto::FileFormat format) {
  switch (format) {
    case proto::FileFormat::BINARY:
      return kuscia_proto::FileFormat::BINARY;
    case proto::FileFormat::CSV:
    case proto::FileFormat::ORC:
      return kuscia_proto::FileFormat::CSV;
    default:
      DATAPROXY_THROW("do not support this type of format:{}",
                      proto::FileFormat_Name(format));
  }
}

google::protobuf::Any BuildDownloadAny(const proto::DownloadInfo& info,
                                       proto::FileFormat file_format) {
  dm_proto::CommandDomainDataQuery msg;
  msg.set_domaindata_id(info.domaindata_id());
  msg.set_partition_spec(info.partition_spec());
  msg.set_content_type(FormatToContentType(file_format));

  google::protobuf::Any any;
  any.PackFrom(msg);
  return any;
}

google::protobuf::Any BuildUploadAny(const proto::UploadInfo& info,
                                     proto::FileFormat file_format) {
  dm_proto::CommandDomainDataUpdate msg;
  msg.set_domaindata_id(info.domaindata_id());
  msg.set_content_type(FormatToContentType(file_format));
  if (file_format != proto::FileFormat::BINARY) {
    msg.mutable_file_write_options()
        ->mutable_csv_options()
        ->set_field_delimiter(",");
  }

  google::protobuf::Any any;
  any.PackFrom(msg);
  return any;
}

google::protobuf::Any BuildSQLAny(const proto::SQLInfo& info) {
  dm_proto::CommandDataSourceSqlQuery msg;
  msg.set_datasource_id(info.datasource_id());
  msg.set_sql(info.sql());

  google::protobuf::Any any;
  any.PackFrom(msg);
  return any;
}

std::string BuildActionCreateDomainDataRequest(const proto::UploadInfo& info,
                                               proto::FileFormat file_format) {
  dm_proto::CreateDomainDataRequest msg;
  msg.set_domaindata_id(info.domaindata_id());
  msg.set_name(info.name());
  msg.set_type(info.type());
  msg.set_datasource_id(info.datasource_id());
  msg.set_relative_uri(info.relative_uri());
  for (auto& attribute : info.attributes()) {
    msg.mutable_attributes()->insert(attribute);
  }
  for (auto& column : info.columns()) {
    auto* msg_column = msg.add_columns();
    msg_column->set_name(column.name());
    msg_column->set_type(column.type());
    msg_column->set_comment(column.comment());
    msg_column->set_not_nullable(column.not_nullable());
  }
  msg.set_vendor(info.vendor());
  msg.set_file_format(ChangeToKusciaFileFormat(file_format));
  return msg.SerializeAsString();
}

std::string BuildActionDeleteDomainDataRequest(const proto::UploadInfo& info) {
  dm_proto::DeleteDomainDataRequest msg;
  msg.set_domaindata_id(info.domaindata_id());
  return msg.SerializeAsString();
}

std::string GetDomaindataIdFromResponse(const std::string& msg) {
  dm_proto::CreateDomainDataResponse response;
  response.ParseFromString(msg);
  CHECK_RESP_OR_THROW(response);
  return response.data().domaindata_id();
}

void CheckUploadInfo(const proto::UploadInfo& info) {
  // Enum: table,model,rule,report,unknown
  if (info.type() != "table" && info.type() != "model" &&
      info.type() != "rule" && info.type() != "serving_model") {
    DATAPROXY_THROW("type[{}] not support in UploadInfo!", info.type());
  }

  if (info.type() == "table" && info.columns().empty()) {
    DATAPROXY_THROW(
        "when type is table, columns cannot be empty in UploadInfo!");
  }
}

static inline char* GetEnvValue(std::string_view key) {
  if (char* env_p = std::getenv(key.data())) {
    if (strlen(env_p) != 0) {
      return env_p;
    }
  }
  return nullptr;
}

void GetDPConfigValueFromEnv(proto::DataProxyConfig* config) {
  if (config == nullptr) return;

  if (char* env_value = GetEnvValue("CLIENT_CERT_FILE")) {
    config->mutable_tls_config()->set_certificate_path(env_value);
  }
  if (char* env_value = GetEnvValue("CLIENT_PRIVATE_KEY_FILE")) {
    config->mutable_tls_config()->set_private_key_path(env_value);
  }
  if (char* env_value = GetEnvValue("TRUSTED_CA_FILE")) {
    config->mutable_tls_config()->set_ca_file_path(env_value);
  }
  if (char* env_value = GetEnvValue("KUSCIA_DATA_MESH_ADDR")) {
    config->set_data_proxy_addr(env_value);
  }

  // 如果没有配置获取没有通过环境变量得到datamesh地址，则使用默认值
  static std::string kDefaultDataProxyAddr = "datamesh:8071";
  if (config->data_proxy_addr().empty()) {
    config->set_data_proxy_addr(kDefaultDataProxyAddr);
  }
}

}  // namespace dataproxy_sdk
