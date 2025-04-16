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

#include "arrow/status.h"

namespace dataproxy_sdk {

class DataMeshMock {
 public:
  arrow::Status StartServer(const std::string& dm_address, int dp_num = 0);
  arrow::Status StartServer(int dp_num = 0);
  std::string GetServerAddress();
  arrow::Status CloseServer();

 public:
  static std::unique_ptr<DataMeshMock> Make();
  DataMeshMock();
  ~DataMeshMock();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dataproxy_sdk
