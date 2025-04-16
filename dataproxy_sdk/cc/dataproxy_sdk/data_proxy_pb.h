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

#pragma once

#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "google/protobuf/any.pb.h"

namespace dataproxy_sdk {

namespace proto {

enum FileFormat {
  UNKNOWN = 0,
  CSV = 1,
  BINARY = 2,
  ORC = 3,
};

template <typename T = FileFormat>
std::string FileFormat_Name(T file_format) {
  switch (file_format) {
    case FileFormat::CSV:
      return "CSV";
    case FileFormat::BINARY:
      return "BINARY";
    case FileFormat::ORC:
      return "ORC";
    default:
      return "UNKNOWN";
  }
}

class TlSConfig {
 public:
  TlSConfig() = default;
  TlSConfig(std::string private_key_path, std::string certificate_path,
            std::string ca_file_path)
      : private_key_path_(std::move(private_key_path)),
        certificate_path_(std::move(certificate_path)),
        ca_file_path_(std::move(ca_file_path)) {}

 public:
  const std::string& private_key_path() const { return private_key_path_; }
  void set_private_key_path(std::string private_key_path) {
    private_key_path_ = std::move(private_key_path);
  }
  const std::string& certificate_path() const { return certificate_path_; }
  void set_certificate_path(std::string certificate_path) {
    certificate_path_ = std::move(certificate_path);
  }
  const std::string& ca_file_path() const { return ca_file_path_; }
  void set_ca_file_path(std::string ca_file_path) {
    ca_file_path_ = std::move(ca_file_path);
  }
  std::string DebugString() const {
    return "TlSConfig{private_key_path=" + private_key_path_ +
           ", certificate_path=" + certificate_path_ +
           ", ca_file_path=" + ca_file_path_ + "}";
  }

 private:
  std::string private_key_path_;
  std::string certificate_path_;
  std::string ca_file_path_;
};

class DataProxyConfig {
 public:
  DataProxyConfig() = default;
  DataProxyConfig(std::string data_proxy_addr,
                  std::optional<TlSConfig> tls_config = std::nullopt)
      : data_proxy_addr_(std::move(data_proxy_addr)),
        tls_config_(std::move(tls_config)) {}

 public:
  const std::string& data_proxy_addr() const { return data_proxy_addr_; }
  void set_data_proxy_addr(std::string data_proxy_addr) {
    data_proxy_addr_ = std::move(data_proxy_addr);
  }
  const TlSConfig& tls_config() const {
    if (has_tls_config()) {
      return tls_config_.value();
    }
    const static TlSConfig kDefaultTlsConfig;
    return kDefaultTlsConfig;
  }
  void set_tls_config(TlSConfig tls_config) {
    tls_config_.emplace(std::move(tls_config));
  }
  TlSConfig* mutable_tls_config() {
    if (!has_tls_config()) {
      tls_config_ = std::make_optional<TlSConfig>();
    }
    return &*tls_config_;
  }
  bool has_tls_config() const { return tls_config_.has_value(); }

  std::string DebugString() const {
    std::string tls_config_str =
        tls_config_.has_value() ? tls_config().DebugString() : "";
    return "DataProxyConfig{data_proxy_addr=" + data_proxy_addr_ +
           ", tls_config=" + tls_config_str + "}";
  }

 private:
  std::string data_proxy_addr_;
  std::optional<TlSConfig> tls_config_;
};

class ORCFileInfo {
 public:
  enum CompressionType {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    BROTLI = 3,
    ZSTD = 4,
    LZ4 = 5,
    LZ4_FRAME = 6,
    LZO = 7,
    BZ2 = 8,
    LZ4_HADOOP = 9,
  };

 public:
  ORCFileInfo() = default;
  ORCFileInfo(CompressionType compression, int64_t compression_block_size,
              int64_t stripe_size)
      : compression_(compression),
        compression_block_size_(compression_block_size),
        stripe_size_(stripe_size) {}

 public:
  CompressionType compression() const { return compression_; }
  void set_compression(CompressionType compression) {
    compression_ = compression;
  }
  int64_t compression_block_size() const { return compression_block_size_; }
  void set_compression_block_size(int64_t compression_block_size) {
    compression_block_size_ = compression_block_size;
  }
  int64_t stripe_size() const { return stripe_size_; }
  void set_stripe_size(int64_t stripe_size) { stripe_size_ = stripe_size; }

 public:
  CompressionType compression_;
  int64_t compression_block_size_;
  int64_t stripe_size_;
};

class DownloadInfo {
 public:
  DownloadInfo() : DownloadInfo("", "") {}
  DownloadInfo(
      std::string domaindata_id, std::string partition_spec,
      std::variant<std::monostate, ORCFileInfo> file_info = std::monostate())
      : domaindata_id_(std::move(domaindata_id)),
        partition_spec_(std::move(partition_spec)),
        file_info_(std::move(file_info)) {}

 public:
  const std::string& domaindata_id() const { return domaindata_id_; }
  void set_domaindata_id(std::string domaindata_id) {
    domaindata_id_ = std::move(domaindata_id);
  }
  const std::string& partition_spec() const { return partition_spec_; }
  void set_partition_spec(std::string partition_spec) {
    partition_spec_ = std::move(partition_spec);
  }
  const ORCFileInfo& orc_info() const {
    if (has_orc_info()) {
      return std::get<ORCFileInfo>(file_info_);
    }
    const static ORCFileInfo kDefaultOrcInfo = {};
    return kDefaultOrcInfo;
  }
  bool has_orc_info() const {
    return std::holds_alternative<ORCFileInfo>(file_info_);
  }
  void set_orc_info(ORCFileInfo target_orc_info) {
    file_info_ = std::move(target_orc_info);
  }
  ORCFileInfo* mutable_orc_info() {
    if (!has_orc_info()) {
      file_info_ = ORCFileInfo();
    }
    return std::get_if<ORCFileInfo>(&file_info_);
  }

 private:
  std::string domaindata_id_;
  // specific the partition column and value, such as "dmdt=20240520"
  std::string partition_spec_;
  std::variant<std::monostate, ORCFileInfo> file_info_;
};

class SQLInfo {
 public:
  SQLInfo() = default;
  SQLInfo(std::string datasource_id, std::string sql)
      : datasource_id_(std::move(datasource_id)), sql_(std::move(sql)) {}

 public:
  const std::string& datasource_id() const { return datasource_id_; }
  void set_datasource_id(std::string datasource_id) {
    datasource_id_ = std::move(datasource_id);
  }
  const std::string& sql() const { return sql_; }
  void set_sql(std::string sql) { sql_ = std::move(sql); }

 private:
  std::string datasource_id_;
  // only support select sql
  std::string sql_;
};

// DataColumn defines the column of data.
class DataColumn {
 public:
  DataColumn() = default;
  DataColumn(std::string name, std::string type, std::string comment,
             bool not_nullable)
      : name_(std::move(name)),
        type_(std::move(type)),
        comment_(std::move(comment)),
        not_nullable_(not_nullable) {}

 public:
  const std::string& name() const { return name_; }
  void set_name(std::string name) { name_ = std::move(name); }
  const std::string& type() const { return type_; }
  void set_type(std::string type) { type_ = std::move(type); }
  const std::string& comment() const { return comment_; }
  void set_comment(std::string comment) { comment_ = std::move(comment); }
  bool not_nullable() const { return not_nullable_; }
  void set_not_nullable(bool not_nullable) { not_nullable_ = not_nullable; }
  std::string DebugString() const {
    return "DataColumn{name=" + name_ + ", type=" + type_ +
           ", comment=" + comment_ +
           ", not_nullable=" + std::to_string(not_nullable_) + "}";
  }

 private:
  std::string name_;
  // enum: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32,
  // float64, date32, date64, bool, string, binary
  std::string type_;
  // The description of column
  std::string comment_;
  // can the column could be nullable, default is nullable
  bool not_nullable_;
};

class UploadInfo {
 public:
  UploadInfo() = default;
  UploadInfo(std::string domaindata_id, std::string name, std::string type,
             std::string relative_uri, std::string datasource_id,
             std::map<std::string, std::string> attributes,
             std::vector<DataColumn> columns, std::string vendor)
      : domaindata_id_(std::move(domaindata_id)),
        name_(std::move(name)),
        type_(std::move(type)),
        relative_uri_(std::move(relative_uri)),
        datasource_id_(std::move(datasource_id)),
        attributes_(std::move(attributes)),
        columns_(std::move(columns)),
        vendor_(std::move(vendor)) {}

 public:
  const std::string& domaindata_id() const { return domaindata_id_; }
  void set_domaindata_id(std::string domaindata_id) {
    domaindata_id_ = std::move(domaindata_id);
  }
  const std::string& name() const { return name_; }
  void set_name(std::string name) { name_ = std::move(name); }
  const std::string& type() const { return type_; }
  void set_type(std::string type) { type_ = std::move(type); }
  const std::string& relative_uri() const { return relative_uri_; }
  void set_relative_uri(std::string relative_uri) {
    relative_uri_ = std::move(relative_uri);
  }
  const std::string& datasource_id() const { return datasource_id_; }
  void set_datasource_id(std::string datasource_id) {
    datasource_id_ = std::move(datasource_id);
  }
  const std::map<std::string, std::string>& attributes() const {
    return attributes_;
  }
  void set_attributes(std::map<std::string, std::string> attributes) {
    attributes_ = std::move(attributes);
  }
  std::map<std::string, std::string>* mutable_attributes() {
    return &attributes_;
  }
  int attributes_size() { return attributes_.size(); }
  const std::vector<DataColumn>& columns() const { return columns_; }
  void set_columns(std::vector<DataColumn> columns) {
    columns_ = std::move(columns);
  }
  std::vector<DataColumn>* mutable_columns() { return &columns_; }
  int columns_size() { return columns_.size(); }
  DataColumn* add_columns() { return &columns_.emplace_back(); }
  const std::string& vendor() const { return vendor_; }
  void set_vendor(std::string vendor) { vendor_ = std::move(vendor); }
  std::string DebugString() const {
    std::string attributes_string;
    for (auto& [key, value] : attributes_) {
      attributes_string += key + ":" + value + ", ";
    }
    std::string columns_string;
    for (auto& column : columns_) {
      columns_string += column.DebugString() + ", ";
    }
    return "UploadInfo{domaindata_id=" + domaindata_id_ + ", name=" + name_ +
           ", type=" + type_ + ", relative_uri=" + relative_uri_ +
           ", datasource_id=" + datasource_id_ +
           ", attributes=" + attributes_string + ", columns=" + columns_string +
           ", vendor=" + vendor_ + "}";
  }

 private:
  // Optional, The domaindata_id would be generated by server if the
  // domaindata_id is empty. The unique identity of domaindata, it couldn't
  // duplicate in the same domain.
  std::string domaindata_id_;
  // The human readable, it could duplicate in the domain.
  std::string name_;
  // Enum: table,model,rule,report,unknown
  std::string type_;
  // The relative_uri is relative to the datasource URI, The datasourceURI
  // appends relative_uri is the domaindataURI. e.g. the relative_uri is
  // "train/table.csv"
  //      the URI of datasource is "/home/data"
  //      the URI of domaindata is "/home/data/train/table.csv"
  std::string relative_uri_;
  // Optional, server would use default datasource if datasource_id is empty.
  // The datasource is where the domain is stored.
  std::string datasource_id_;
  // Optional, The attributes of the domaindata, this field use as a extra
  // field, User could set this field to any data what they need.
  std::map<std::string, std::string> attributes_;
  // This field must be set if the type is 'table',
  // the columns describe the table's schema information.
  std::vector<DataColumn> columns_;
  // Optional , The vendor is the one who outputs the domain data, it may be the
  // SecretFlow engine, another vendor's engine, or manually registered. it's
  // could be manual, secretflow or other vendor string.
  std::string vendor_;
};

}  // namespace proto

google::protobuf::Any BuildDownloadAny(const proto::DownloadInfo& info,
                                       proto::FileFormat file_format);

google::protobuf::Any BuildUploadAny(const proto::UploadInfo& info,
                                     proto::FileFormat file_format);

google::protobuf::Any BuildSQLAny(const proto::SQLInfo& info);

std::string BuildActionCreateDomainDataRequest(const proto::UploadInfo& info,
                                               proto::FileFormat file_format);

std::string BuildActionDeleteDomainDataRequest(const proto::UploadInfo& info);

std::string GetDomaindataIdFromResponse(const std::string& msg);

void CheckUploadInfo(const proto::UploadInfo& info);

void GetDPConfigValueFromEnv(proto::DataProxyConfig* config);

}  // namespace dataproxy_sdk
