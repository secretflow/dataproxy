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
      return proto::ContentType::CSV;
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
  //   需要更新kuscia版本
  //   msg.set_partition_spec(info.partition_spec());
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
      info.type() != "rule" && info.type() != "report") {
    DATAPROXY_THROW("type[{}] not support in UploadInfo!", info.type());
  }

  if (info.type() == "table" && info.columns().empty()) {
    DATAPROXY_THROW(
        "when type is table, columns cannot be empty in UploadInfo!");
  }
}

}  // namespace dataproxy_sdk