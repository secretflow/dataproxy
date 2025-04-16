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

#include <memory>
#include <string>

#include "data_proxy_pb.h"

namespace dataproxy_sdk {

class DataProxyFile {
 public:
  static std::unique_ptr<DataProxyFile> Make(
      const proto::DataProxyConfig& config);

  static std::unique_ptr<DataProxyFile> Make();

 public:
  DataProxyFile();
  ~DataProxyFile();

 public:
  void DownloadFile(const proto::DownloadInfo& info,
                    const std::string& file_path,
                    proto::FileFormat file_format);

  void UploadFile(proto::UploadInfo& info, const std::string& file_path,
                  proto::FileFormat file_format);

  void Close();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dataproxy_sdk