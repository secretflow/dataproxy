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

#include "utils.h"

#include <filesystem>
#include <fstream>
#include <unordered_map>

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

std::string ReadFileContent(const std::string& file) {
  if (!std::filesystem::exists(file)) {
    DATAPROXY_ENFORCE("can not find file: {}", file);
  }
  std::ifstream file_is(file);
  DATAPROXY_ENFORCE(file_is.good(), "open failed, file: {}", file);
  return std::string((std::istreambuf_iterator<char>(file_is)),
                     std::istreambuf_iterator<char>());
}

std::shared_ptr<arrow::DataType> GetDataType(const std::string& type) {
  static std::unordered_map<std::string, std::shared_ptr<arrow::DataType>>
      type_map = {{"int8", arrow::int8()},       {"int16", arrow::int16()},
                  {"int32", arrow::int32()},     {"int64", arrow::int64()},
                  {"uint8", arrow::uint8()},     {"uint16", arrow::uint16()},
                  {"uint32", arrow::uint32()},   {"uint64", arrow::uint64()},
                  {"float16", arrow::float16()}, {"float32", arrow::float32()},
                  {"float64", arrow::float64()}, {"bool", arrow::boolean()},
                  {"int", arrow::int64()},       {"float", arrow::float64()},
                  {"str", arrow::utf8()},        {"string", arrow::utf8()}};

  auto iter = type_map.find(type);
  if (iter == type_map.end()) {
    DATAPROXY_THROW("Unsupported type: {}", type);
  }

  return iter->second;
}

}  // namespace dataproxy_sdk