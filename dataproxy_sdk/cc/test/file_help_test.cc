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

#include "dataproxy_sdk/file_help.h"

#include <iostream>

#include "arrow/builder.h"
#include "gtest/gtest.h"
#include "test/tools/random.h"
#include "test/tools/utils.h"

#include "dataproxy_sdk/exception.h"

namespace dataproxy_sdk {

const std::string kCSVFilePath = "test.csv";
const std::string kORCFilePath = "test.orc";
const std::string kBianryFilePath = "test.txt";

template <typename T>
std::unique_ptr<T> GetDefaultFileHelp(const std::string& file_path) {
  auto options = T::Options::Defaults();
  auto ret = T::Make(GetFileFormat(file_path), file_path, options);
  return ret;
}

TEST(FileHelpTest, Binary) {
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("binary_data", arrow::binary())});

  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::Generate(schema, 1);
  auto writer = GetDefaultFileHelp<FileHelpWrite>(kBianryFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = GetDefaultFileHelp<FileHelpRead>(kBianryFilePath);
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
  auto writer = GetDefaultFileHelp<FileHelpWrite>(kBianryFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = GetDefaultFileHelp<FileHelpRead>(kBianryFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

TEST(FileHelpTest, CSV) {
  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = GetDefaultFileHelp<FileHelpRead>(kCSVFilePath);
  reader->DoRead(&read_batch);
  reader->DoClose();

  std::cout << batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(batch->Equals(*read_batch));
}

TEST(FileHelpTest, ORC) {
  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = GetDefaultFileHelp<FileHelpRead>(kORCFilePath);
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
  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFileFormat(kCSVFilePath), kCSVFilePath,
                                   GetReadOptions());
  reader->DoRead(&read_batch);
  reader->DoClose();

  auto target_batch = batch->SelectColumns(GetSelectColumns()).ValueOrDie();
  std::cout << target_batch->ToString() << std::endl;
  std::cout << read_batch->ToString() << std::endl;

  EXPECT_TRUE(target_batch->Equals(*read_batch));
}

TEST(FileHelpTestWithOption, ORC) {
  std::shared_ptr<arrow::RecordBatch> batch =
      RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFileFormat(kORCFilePath), kORCFilePath,
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
  auto batch = RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kCSVFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  EXPECT_THROW(FileHelpRead::Make(GetFileFormat(kCSVFilePath), kCSVFilePath,
                                  GetErrorOptions()),
               yacl::Exception);
}

TEST(FileHelpTestWithOption, ErrorORC) {
  auto batch = RandomBatchGenerator::ExampleGenerate();

  auto writer = GetDefaultFileHelp<FileHelpWrite>(kORCFilePath);
  writer->DoWrite(batch);
  writer->DoClose();

  std::shared_ptr<arrow::RecordBatch> read_batch;
  auto reader = FileHelpRead::Make(GetFileFormat(kORCFilePath), kORCFilePath,
                                   GetErrorOptions());
  EXPECT_THROW(reader->DoRead(&read_batch), yacl::Exception);
}

void System(const std::string& cmd) { ASSERT_TRUE(system(cmd.c_str()) == 0); }

TEST(FileHelpTest, LargeBinaryRead) {
  const std::string kLargeBinaryFile = "large_file.txt";
  System("dd if=/dev/urandom of=large_file.txt bs=1M count=1");
  auto reader = GetDefaultFileHelp<FileHelpRead>(kLargeBinaryFile);
  while (1) {
    std::shared_ptr<arrow::RecordBatch> read_batch;
    reader->DoRead(&read_batch);
    if (!read_batch) break;
  }

  reader->DoClose();
}

TEST(FileHelpTest, LargeBinary) {
  System("dd if=/dev/urandom of=large_file_source.txt bs=10M count=1");

  auto writer = GetDefaultFileHelp<FileHelpWrite>("large_file_equal.txt");
  auto reader = GetDefaultFileHelp<FileHelpRead>("large_file_source.txt");
  while (1) {
    std::shared_ptr<arrow::RecordBatch> read_batch;
    reader->DoRead(&read_batch);
    if (!read_batch) break;
    writer->DoWrite(read_batch);
  }
  writer->DoClose();
  reader->DoClose();

  System("diff large_file_source.txt large_file_equal.txt");
}

}  // namespace dataproxy_sdk
