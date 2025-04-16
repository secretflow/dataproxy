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

#include "random.h"

#include <random>

#include "arrow/builder.h"
#include "arrow/record_batch.h"

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

class RandomBatchGeneratorImpl {
 public:
  RandomBatchGeneratorImpl(const std::shared_ptr<arrow::Schema> &schema,
                           int32_t num_rows)
      : schema_(schema), num_rows_(num_rows) {}

 public:
  std::shared_ptr<arrow::RecordBatch> Generate() {
    for (std::shared_ptr<arrow::Field> field : schema_->fields()) {
      CHECK_ARROW_OR_THROW(arrow::VisitTypeInline(*field->type(), this));
    }
    auto ret = arrow::RecordBatch::Make(schema_, num_rows_, arrays_);
    return ret;
  }

  // Default implementation
  arrow::Status Visit(const arrow::DataType &type) {
    return arrow::Status::NotImplemented("Generating data for",
                                         type.ToString());
  }

  arrow::Status Visit(const arrow::BinaryType &) {
    auto builder = arrow::BinaryBuilder();
    uint32_t max = std::numeric_limits<uint8_t>::max() > num_rows_
                       ? num_rows_
                       : std::numeric_limits<uint8_t>::max();
    std::uniform_int_distribution<uint32_t> d(0, max);

    uint8_t *buff = (uint8_t *)alloca(sizeof(uint8_t) * num_rows_);
    for (int32_t i = 0; i < num_rows_; ++i) {
      buff[i] = d(gen_);
    }

    CHECK_ARROW_OR_THROW(builder.Append(buff, num_rows_));
    ASSIGN_DP_OR_THROW(auto array, builder.Finish());
    arrays_.push_back(array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &) {
    auto builder = arrow::DoubleBuilder();
    std::normal_distribution<> d{/*mean=*/5.0, /*stddev=*/2.0};  // 正态分布
    for (int32_t i = 0; i < num_rows_; ++i) {
      CHECK_ARROW_OR_THROW(builder.Append(d(gen_)));
    }

    ASSIGN_DP_OR_THROW(auto array, builder.Finish());
    arrays_.push_back(array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &) {
    // Generate offsets first, which determines number of values in sub-array
    std::poisson_distribution<> d{
        /*mean=*/4};  // 产生随机非负整数值i，按离散概率函数分布
    auto builder = arrow::Int64Builder();
    for (int32_t i = 0; i < num_rows_; ++i) {
      CHECK_ARROW_OR_THROW(builder.Append(d(gen_)));
    }

    ASSIGN_DP_OR_THROW(auto array, builder.Finish());
    arrays_.push_back(array);
    return arrow::Status::OK();
  }

 protected:
  std::random_device rd_{};
  std::mt19937 gen_{rd_()};  // 随机种子
  std::vector<std::shared_ptr<arrow::Array>> arrays_;
  std::shared_ptr<arrow::Schema> schema_;
  int32_t num_rows_;
};

std::shared_ptr<arrow::RecordBatch> RandomBatchGenerator::Generate(
    const std::shared_ptr<arrow::Schema> &schema, int32_t num_rows) {
  RandomBatchGeneratorImpl generator(schema, num_rows);

  return generator.Generate();
}

std::shared_ptr<arrow::RecordBatch> RandomBatchGenerator::ExampleGenerate(
    int row) {
  auto f0 = arrow::field("x", arrow::int64());
  auto f1 = arrow::field("y", arrow::int64());
  auto f2 = arrow::field("z", arrow::int64());
  std::shared_ptr<arrow::Schema> schema = arrow::schema({f0, f1, f2});
  return RandomBatchGenerator::Generate(schema, row);
}

}  // namespace dataproxy_sdk
