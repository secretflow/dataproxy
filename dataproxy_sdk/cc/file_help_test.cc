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

#include "dataproxy_sdk/cc/file_help.h"

#include <iostream>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "arrow/adapters/orc/adapter.h"
#include "arrow/builder.h"
#include "arrow/csv/api.h"
#include "arrow/io/api.h"
#include "dataproxy_sdk/cc/exception.h"
#include "gtest/gtest.h"

namespace dataproxy_sdk {

class RandomBatchGenerator {
 public:
  std::shared_ptr<arrow::Schema> schema;
  RandomBatchGenerator(std::shared_ptr<arrow::Schema> schema)
      : schema(schema){};

  static std::shared_ptr<arrow::RecordBatch> Generate(
      std::shared_ptr<arrow::Schema> schema, int32_t num_rows) {
    RandomBatchGenerator generator(schema);

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSIGN_ARROW_OR_THROW(batch, generator.Generate(num_rows));
    return batch;
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Generate(
      int32_t num_rows) {
    num_rows_ = num_rows;
    for (std::shared_ptr<arrow::Field> field : schema->fields()) {
      ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field->type(), this));
    }
    return arrow::RecordBatch::Make(schema, num_rows, arrays_);
  }

  // Default implementation
  arrow::Status Visit(const arrow::DataType &type) {
    return arrow::Status::NotImplemented("Generating data for",
                                         type.ToString());
  }

  arrow::Status Visit(const arrow::BinaryType &) {
    auto builder = arrow::BinaryBuilder();
    // std::normal_distribution<> d{
    //     /*mean=*/0x05,
    // };  // 正态分布
    for (int32_t i = 0; i < num_rows_; ++i) {
      ARROW_RETURN_NOT_OK(builder.Append("03", 2));
    }

    ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
    arrays_.push_back(array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &) {
    auto builder = arrow::DoubleBuilder();
    std::normal_distribution<> d{/*mean=*/5.0, /*stddev=*/2.0};  // 正态分布
    for (int32_t i = 0; i < num_rows_; ++i) {
      ARROW_RETURN_NOT_OK(builder.Append(d(gen_)));
    }

    ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
    arrays_.push_back(array);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &) {
    // Generate offsets first, which determines number of values in sub-array
    std::poisson_distribution<> d{
        /*mean=*/4};  // 产生随机非负整数值i，按离散概率函数分布
    auto builder = arrow::Int64Builder();
    for (int32_t i = 0; i < num_rows_; ++i) {
      ARROW_RETURN_NOT_OK(builder.Append(d(gen_)));
    }

    ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
    arrays_.push_back(array);
    return arrow::Status::OK();
  }

 protected:
  std::random_device rd_{};
  std::mt19937 gen_{rd_()};  // 随机种子
  std::vector<std::shared_ptr<arrow::Array>> arrays_;
  int32_t num_rows_;

};  // RandomBatchGenerator

proto::FileFormat GetFormat(const std::string &file) {
  if (file.find(".csv") != std::string::npos)
    return proto::FileFormat::CSV;
  else if (file.find(".orc") != std::string::npos)
    return proto::FileFormat::ORC;

  return proto::FileFormat::BINARY;
}

static std::shared_ptr<arrow::RecordBatch> GetRecordBatch(int data_num = 2) {
  static std::shared_ptr<arrow::Schema> gSchema = arrow::schema(
      {arrow::field("x", arrow::int64()), arrow::field("y", arrow::int64()),
       arrow::field("z", arrow::int64())});

  return RandomBatchGenerator::Generate(gSchema, data_num);
}

const std::string kCSVFilePath = "test.csv";
const std::string kORCFilePath = "test.orc";
const std::string kBianryFilePath = "test.txt";

TEST(FileHelpTest, Binary) {
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("binary_data", arrow::binary())});

  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::Generate(schema, 1);
  auto writer =
      FileHelpWrite::Make(GetFormat(kBianryFilePath), kBianryFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFormat(kBianryFilePath), kBianryFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

TEST(FileHelpTest, ZeroBinary) {
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("binary_data", arrow::binary())});

  auto binary_builder = arrow::BinaryBuilder();
  CHECK_ARROW_OR_THROW(binary_builder.Append("3\000\00045\0006\000", 8));
  std::shared_ptr<arrow::Array> array;
  ASSIGN_ARROW_OR_THROW(array, binary_builder.Finish());
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.push_back(array);

  std::shared_ptr<arrow::RecordBatch> batch =
      arrow::RecordBatch::Make(schema, arrays.size(), arrays);
  auto writer =
      FileHelpWrite::Make(GetFormat(kBianryFilePath), kBianryFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFormat(kBianryFilePath), kBianryFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

TEST(FileHelpTest, CSV) {
  std::shared_ptr<arrow::RecordBatch> batch = GetRecordBatch();

  auto writer = FileHelpWrite::Make(GetFormat(kCSVFilePath), kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFormat(kCSVFilePath), kCSVFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

TEST(FileHelpTest, ORC) {
  std::shared_ptr<arrow::RecordBatch> batch = GetRecordBatch();

  auto writer = FileHelpWrite::Make(GetFormat(kORCFilePath), kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFormat(kORCFilePath), kORCFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

FileHelpRead::Options GetReadOptions() {
  FileHelpRead::Options read_options = FileHelpRead::Options::Defaults();
  read_options.column_types.emplace("z", arrow::int64());
  return read_options;
}

std::vector<int> GetSelectColumns() {
  static std::vector<int> select_columns(1, 2);
  return select_columns;
}

TEST(FileHelpTestWithOption, CSV) {
  std::shared_ptr<arrow::RecordBatch> batch = GetRecordBatch();

  auto writer = FileHelpWrite::Make(GetFormat(kCSVFilePath), kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFormat(kCSVFilePath), kCSVFilePath,
                                   GetReadOptions());
  reader->DoRead(&read_batch);
  reader->DoClose();

  auto target_batch = batch->SelectColumns(GetSelectColumns()).ValueOrDie();
  std::cout << target_batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(target_batch->Equals(*read_batch));
}

TEST(FileHelpTestWithOption, ORC) {
  std::shared_ptr<arrow::RecordBatch> batch = GetRecordBatch();

  auto writer = FileHelpWrite::Make(GetFormat(kORCFilePath), kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFormat(kORCFilePath), kORCFilePath,
                                   GetReadOptions());
  reader->DoRead(&read_batch);
  reader->DoClose();

  auto target_batch = batch->SelectColumns(GetSelectColumns()).ValueOrDie();
  std::cout << target_batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(target_batch->Equals(*read_batch));
}

FileHelpRead::Options GetErrorOptions() {
  FileHelpRead::Options read_options = FileHelpRead::Options::Defaults();
  read_options.column_types.emplace("a", arrow::int64());
  return read_options;
}

TEST(FileHelpTestWithOption, ErrorCSV) {
  std::shared_ptr<arrow::RecordBatch> batch = GetRecordBatch();

  auto writer = FileHelpWrite::Make(GetFormat(kCSVFilePath), kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  EXPECT_THROW(FileHelpRead::Make(GetFormat(kCSVFilePath), kCSVFilePath,
                                  GetErrorOptions()),
               yacl::Exception);
}

TEST(FileHelpTestWithOption, ErrorORC) {
  std::shared_ptr<arrow::RecordBatch> batch = GetRecordBatch();

  auto writer = FileHelpWrite::Make(GetFormat(kORCFilePath), kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFormat(kORCFilePath), kORCFilePath,
                                   GetErrorOptions());
  EXPECT_THROW(reader->DoRead(&read_batch), yacl::Exception);
}

}  // namespace dataproxy_sdk