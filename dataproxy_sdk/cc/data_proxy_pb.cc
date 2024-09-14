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

#include "dataproxy_sdk/cc/data_proxy_pb.h"

#include "dataproxy_sdk/cc/exception.h"

namespace dataproxy_sdk {

inline proto::ContentType FormatToContentType(proto::FileFormat format) {
  switch (format) {
    case proto::FileFormat::BINARY:
      return proto::ContentType::RAW;
    case proto::FileFormat::CSV:
    case proto::FileFormat::ORC:
      return proto::ContentType::Table;
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
  google::protobuf::Any any;
  proto::CommandDomainDataQuery msg;
  msg.set_domaindata_id(info.domaindata_id());
  msg.set_partition_spec(info.partition_spec());
  msg.set_content_type(FormatToContentType(file_format));

  any.PackFrom(msg);
  return any;
}

google::protobuf::Any BuildUploadAny(const proto::UploadInfo& info,
                                     proto::FileFormat file_format) {
  google::protobuf::Any any;
  proto::CommandDomainDataUpdate msg;
  msg.set_domaindata_id(info.domaindata_id());
  msg.set_content_type(FormatToContentType(file_format));
  if (file_format != proto::FileFormat::BINARY) {
    msg.mutable_file_write_options()
        ->mutable_csv_options()
        ->set_field_delimiter(",");
  }

  any.PackFrom(msg);
  return any;
}

proto::CreateDomainDataRequest BuildActionCreateDomainDataRequest(
    const proto::UploadInfo& info, proto::FileFormat file_format) {
  proto::CreateDomainDataRequest msg;
  msg.set_domaindata_id(info.domaindata_id());
  msg.set_name(info.name());
  msg.set_type(info.type());
  msg.set_datasource_id(info.datasource_id());
  msg.set_relative_uri(info.relative_uri());
  for (auto& attribute : info.attributes()) {
    msg.mutable_attributes()->insert(attribute);
  }
  msg.mutable_columns()->CopyFrom(info.columns());
  msg.set_vendor(info.vendor());
  msg.set_file_format(ChangeToKusciaFileFormat(file_format));

  return msg;
}

proto::DeleteDomainDataRequest BuildActionDeleteDomainDataRequest(
    const proto::UploadInfo& info) {
  proto::DeleteDomainDataRequest msg;
  msg.set_domaindata_id(info.domaindata_id());
  return msg;
}

proto::CreateDomainDataResponse GetActionCreateDomainDataResponse(
    const std::string& msg) {
  proto::CreateDomainDataResponse response;
  response.ParseFromString(msg);
  return response;
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
}

}  // namespace dataproxy_sdk
