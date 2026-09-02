/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/vortex/VortexReader.h"

#include <stdexcept>

#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Nulls.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempFilePath.h"
#include "velox/dwio/common/Mutation.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/vortex/VortexVector.h"
#include "velox/dwio/vortex/tests/VortexTestFile.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::dwio::vortex {
namespace {

using facebook::velox::common::testutil::TempFilePath;

class ThrowingReadFile final : public InMemoryReadFile {
 public:
  explicit ThrowingReadFile(std::string data)
      : InMemoryReadFile{std::move(data)} {}

  std::string_view pread(uint64_t, uint64_t, void*, const FileIoContext&)
      const override {
    throw std::runtime_error{"Injected Vortex read callback failure"};
  }
};

class CapturingHook final : public ValueHook {
 public:
  explicit CapturingHook(vector_size_t size)
      : values_(size), seen_(size, false) {}

  bool acceptsNulls() const override {
    return true;
  }

  void addValue(vector_size_t row, int64_t value) override {
    VELOX_CHECK_LT(row, values_.size());
    VELOX_CHECK(!seen_[row]);
    values_[row] = value;
    seen_[row] = true;
  }

  void addNull(vector_size_t row) override {
    VELOX_CHECK_LT(row, values_.size());
    VELOX_CHECK(!seen_[row]);
    values_[row] = std::nullopt;
    seen_[row] = true;
  }

  const std::vector<std::optional<int64_t>>& values() const {
    return values_;
  }

  const std::vector<bool>& seen() const {
    return seen_;
  }

 private:
  std::vector<std::optional<int64_t>> values_;
  std::vector<bool> seen_;
};

class AddRowOffsetUpdater final : public common::DeltaColumnUpdater {
 public:
  void update(const RowSet& baseRows, VectorPtr& result) override {
    DecodedVector decoded{*result};
    auto updated = BaseVector::create<FlatVector<int64_t>>(
        BIGINT(), baseRows.size(), result->pool());
    for (vector_size_t i = 0; i < baseRows.size(); ++i) {
      updated->set(i, decoded.valueAt<int64_t>(i) + 10 * baseRows[i]);
    }
    result = std::move(updated);
  }
};

class VortexReaderTest : public testing::Test,
                         public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void writeVortex(const std::string& path, const RowVectorPtr& data) {
    writeVortex(path, std::vector<RowVectorPtr>{data});
  }

  void writeVortex(
      const std::string& path,
      const std::vector<RowVectorPtr>& batches) {
    test::writeVortexFile(path, batches, pool());
  }

  std::unique_ptr<VortexReader> openReader(const std::string& path) {
    common::ReaderOptions options{pool()};
    return openReader(path, options);
  }

  std::unique_ptr<VortexReader> openReader(
      const std::string& path,
      const common::ReaderOptions& options) {
    auto readFile = std::make_shared<LocalReadFile>(path);
    auto input = std::make_unique<common::BufferedInput>(readFile, *pool());
    return std::make_unique<VortexReader>(std::move(input), options);
  }
};

TEST_F(VortexReaderTest, fullScan) {
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeNullableFlatVector<double>({1.5, std::nullopt, 3.5, 4.5, 5.5}),
      makeFlatVector<std::string>({"one", "two", "three", "four", "five"}),
      makeFlatVector<bool>({true, false, true, true, false}),
  });
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), expected);
  const auto memoryBeforeRead = pool()->usedBytes();

  {
    auto reader = openReader(file->getPath());
    EXPECT_EQ(reader->numberOfRows(), expected->size());
    EXPECT_EQ(reader->rowType()->toString(), expected->type()->toString());

    common::RowReaderOptions options;
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*expected->type());
    options.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(options);

    VectorPtr result;
    EXPECT_EQ(rowReader->next(2, result), 2);
    ASSERT_TRUE(isLazyNotLoaded(*result->as<RowVector>()->childAt(0)));
    test::assertEqualVectors(expected->slice(0, 2), result);
    EXPECT_EQ(rowReader->nextRowNumber(), 2);
    EXPECT_EQ(rowReader->next(10, result), 3);
    test::assertEqualVectors(expected->slice(2, 3), result);
    EXPECT_EQ(rowReader->next(10, result), 0);
    EXPECT_EQ(rowReader->nextRowNumber(), common::RowReader::kAtEnd);
  }
  EXPECT_EQ(pool()->usedBytes(), memoryBeforeRead);
}

TEST_F(VortexReaderTest, containsReadCallbackExceptions) {
  const auto bytes = test::writeVortexBytes(
      {makeRowVector({makeFlatVector<int64_t>({1, 2, 3})})}, pool());
  auto input = std::make_unique<common::BufferedInput>(
      std::make_shared<ThrowingReadFile>(bytes), *pool());
  common::ReaderOptions options{pool()};

  VELOX_ASSERT_THROW(
      VortexReader(std::move(input), options),
      "Injected Vortex read callback failure");
}

TEST_F(VortexReaderTest, estimatedRowSize) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<double>({1.5, 2.5, 3.5}),
      makeFlatVector<std::string>({"one", "two", "three"}),
  });
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);
  auto reader = openReader(file->getPath());

  common::RowReaderOptions fixedWidthOptions;
  fixedWidthOptions.setRequestedType(asRowType(input->type()));
  auto fixedWidthScanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  fixedWidthScanSpec->addField("c0", 0);
  fixedWidthScanSpec->addField("c1", 1);
  fixedWidthOptions.setScanSpec(fixedWidthScanSpec);
  auto fixedWidthReader = reader->createRowReader(fixedWidthOptions);
  EXPECT_EQ(fixedWidthReader->estimatedRowSize(), 16);

  common::RowReaderOptions variableWidthOptions;
  variableWidthOptions.setRequestedType(asRowType(input->type()));
  auto variableWidthScanSpec =
      std::make_shared<velox::common::ScanSpec>("<root>");
  variableWidthScanSpec->addField("c2", 0);
  variableWidthOptions.setScanSpec(variableWidthScanSpec);
  auto variableWidthReader = reader->createRowReader(variableWidthOptions);
  EXPECT_EQ(variableWidthReader->estimatedRowSize(), std::nullopt);
}

TEST_F(VortexReaderTest, filtersProjectionAndMutation) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5}),
      makeFlatVector<std::string>({"a", "b", "c", "d", "e", "f"}),
  });
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      facebook::velox::common::createBigintValues({1, 2, 3, 4}, false));
  scanSpec->childByName("c0")->setProjectOut(false);
  scanSpec->childByName("c1")->setChannel(0);
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  std::vector<uint64_t> deleted(bits::nwords(input->size()), 0);
  bits::setBit(deleted.data(), 2);
  common::Mutation mutation;
  mutation.deletedRows = deleted.data();
  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result, &mutation), 5);
  auto expected = makeRowVector({makeFlatVector<std::string>({"b", "d", "e"})});
  test::assertEqualVectors(expected, result);
  EXPECT_EQ(rowReader->next(input->size(), result, &mutation), 1);
  EXPECT_EQ(result->size(), 0);
  EXPECT_EQ(rowReader->next(input->size(), result, &mutation), 0);
}

TEST_F(VortexReaderTest, readsAcrossInternalBatchBoundaries) {
  auto first = makeRowVector({makeFlatVector<int64_t>({1, 2})});
  auto second = makeRowVector({makeFlatVector<int64_t>({3, 4, 5})});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), {first, second});

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*first->type());
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->nextReadSize(4), 4);
  EXPECT_EQ(rowReader->next(4, result), 4);
  test::assertEqualVectors(
      makeRowVector({makeFlatVector<int64_t>({1, 2, 3, 4})}), result);
  EXPECT_EQ(rowReader->nextReadSize(4), 1);
  EXPECT_EQ(rowReader->next(4, result), 1);
  test::assertEqualVectors(
      makeRowVector({makeFlatVector<int64_t>({5})}), result);
}

TEST_F(VortexReaderTest, rowNumbersFollowSelection) {
  auto input = makeRowVector({makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5})});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      facebook::velox::common::createBigintValues({1, 3, 4}, false));
  options.setScanSpec(scanSpec);
  options.setRowNumberColumnInfo(common::RowNumberColumnInfo{0, "$row_id"});
  auto rowReader = reader->createRowReader(options);

  std::vector<uint64_t> deleted(bits::nwords(input->size()), 0);
  bits::setBit(deleted.data(), 3);
  common::Mutation mutation;
  mutation.deletedRows = deleted.data();
  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result, &mutation), 5);
  test::assertEqualVectors(
      makeRowVector(
          {"$row_id", "c0"},
          {makeFlatVector<int64_t>({1, 4}), makeFlatVector<int64_t>({1, 4})}),
      result);
  EXPECT_EQ(rowReader->next(input->size(), result, &mutation), 1);
  EXPECT_EQ(result->size(), 0);
  EXPECT_EQ(rowReader->next(input->size(), result, &mutation), 0);
}

TEST_F(VortexReaderTest, fullyFilteredWindowAdvancesSourceRows) {
  auto input = makeRowVector({makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5})});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      facebook::velox::common::createBigintValues({10}, false));
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(4, result), 4);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 0);
  EXPECT_EQ(rowReader->nextRowNumber(), 4);
  EXPECT_EQ(rowReader->next(4, result), 2);
  EXPECT_EQ(result->size(), 0);
  EXPECT_EQ(rowReader->nextRowNumber(), common::RowReader::kAtEnd);
  EXPECT_EQ(rowReader->next(4, result), 0);
}

TEST_F(VortexReaderTest, resetFilterCachesRestartsAtUnreadRow) {
  auto input =
      makeRowVector({makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7})});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(3, result), 3);
  test::assertEqualVectors(input->slice(0, 3), result);

  scanSpec->childByName("c0")->setFilter(
      facebook::velox::common::createBigintValues({4, 6}, false));
  rowReader->resetFilterCaches();
  EXPECT_EQ(rowReader->next(5, result), 4);
  test::assertEqualVectors(
      makeRowVector({makeFlatVector<int64_t>({4, 6})}), result);
  EXPECT_EQ(rowReader->next(5, result), 1);
  EXPECT_EQ(result->size(), 0);
  EXPECT_EQ(rowReader->next(5, result), 0);
}

TEST_F(VortexReaderTest, randomSkipConsumesRawRowsBeforePushedFilter) {
  constexpr vector_size_t kSize = 12;
  auto input = makeRowVector({makeFlatVector<int64_t>(kSize, folly::identity)});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      facebook::velox::common::createBigintValues({0, 2, 4, 6, 8, 10}, false));
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  constexpr uint32_t kSeed = 314'159;
  std::vector<uint64_t> deleted(bits::nwords(kSize), 0);
  bits::setBit(deleted.data(), 3);
  random::setSeed(kSeed);
  random::RandomSkipTracker expectedTracker(0.5);
  std::vector<int64_t> expectedValues;
  for (vector_size_t row = 0; row < kSize; ++row) {
    if (bits::isBitSet(deleted.data(), row)) {
      continue;
    }
    if (expectedTracker.testOne() && row % 2 == 0) {
      expectedValues.push_back(row);
    }
  }

  random::setSeed(kSeed);
  random::RandomSkipTracker actualTracker(0.5);
  common::Mutation mutation;
  mutation.deletedRows = deleted.data();
  mutation.randomSkip = &actualTracker;
  VectorPtr result;
  EXPECT_EQ(rowReader->next(kSize, result, &mutation), 11);
  test::assertEqualVectors(
      makeRowVector({makeFlatVector<int64_t>(expectedValues)}), result);
  EXPECT_EQ(rowReader->next(kSize, result, &mutation), 1);
  EXPECT_EQ(result->size(), 0);
  EXPECT_EQ(rowReader->next(kSize, result, &mutation), 0);
}

TEST_F(VortexReaderTest, deltaUpdateUsesRowsBeforeAnotherPushedFilter) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5}),
      makeFlatVector<int64_t>({100, 101, 102, 103, 104, 105}),
  });
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      facebook::velox::common::createBigintValues({1, 3, 5}, false));
  AddRowOffsetUpdater updater;
  auto* updatedSpec = scanSpec->childByName("c1");
  updatedSpec->setDeltaUpdate(&updater);
  updatedSpec->setFilter(
      facebook::velox::common::createBigintValues({133, 155}, false));
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  test::assertEqualVectors(
      makeRowVector({
          makeFlatVector<int64_t>({3, 5}),
          makeFlatVector<int64_t>({133, 155}),
      }),
      result);
  EXPECT_EQ(rowReader->next(input->size(), result), 0);
}

TEST_F(VortexReaderTest, requestedTypeWidening) {
  auto input = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  const auto requestedType = ROW({"c0"}, {BIGINT()});
  common::RowReaderOptions options;
  options.setRequestedType(requestedType);
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*requestedType);
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  test::assertEqualVectors(
      makeRowVector({makeFlatVector<int64_t>({1, 2, 3})}), result);
}

TEST_F(VortexReaderTest, missingTopLevelFieldBecomesNull) {
  auto input = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  const auto requestedType = ROW({"c0", "missing"}, {INTEGER(), BIGINT()});
  common::RowReaderOptions options;
  options.setRequestedType(requestedType);
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*requestedType);
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  test::assertEqualVectors(
      makeRowVector(
          {makeFlatVector<int32_t>({1, 2, 3}),
           makeNullConstant(TypeKind::BIGINT, 3)}),
      result);
}

TEST_F(VortexReaderTest, positionalSchemaMappingRenamesNestedFields) {
  auto input = makeRowVector(
      {"physical_id", "physical_record"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeRowVector(
              {"physical_value", "physical_text"},
              {
                  makeFlatVector<int64_t>({10, 20, 30}),
                  makeFlatVector<std::string>({"a", "b", "c"}),
              }),
      });
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  const auto logicalType =
      ROW({"identifier", "record"},
          {
              INTEGER(),
              ROW({"value", "text"}, {BIGINT(), VARCHAR()}),
          });
  common::ReaderOptions readerOptions{pool()};
  readerOptions.setFileSchema(logicalType);
  readerOptions.setColumnMappingMode(common::ColumnMappingMode::kPosition);
  auto reader = openReader(file->getPath(), readerOptions);
  EXPECT_TRUE(reader->rowType()->equivalent(*logicalType));

  auto rowReader = reader->createRowReader(common::RowReaderOptions{});
  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  auto expected = makeRowVector(
      {"identifier", "record"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeRowVector(
              {"value", "text"},
              {
                  makeFlatVector<int64_t>({10, 20, 30}),
                  makeFlatVector<std::string>({"a", "b", "c"}),
              }),
      });
  test::assertEqualVectors(expected, result);
}

TEST_F(VortexReaderTest, selectorProjectsStrictSubset) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>({"a", "b", "c"}),
      makeFlatVector<double>({1.5, 2.5, 3.5}),
  });
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  options.select(
      std::make_shared<common::ColumnSelector>(
          reader->rowType(), std::vector<std::string>{"c1"}));
  options.setProjectSelectedType(true);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  test::assertEqualVectors(
      makeRowVector({"c1"}, {makeFlatVector<std::string>({"a", "b", "c"})}),
      result);
}

TEST_F(VortexReaderTest, scanSpecProjectsStrictSubset) {
  constexpr vector_size_t kNumRows = 50'000;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(
          kNumRows, [](vector_size_t row) { return row * 17; }),
      makeFlatVector<double>(
          kNumRows, [](vector_size_t row) { return row * 0.25; }),
      makeFlatVector<std::string>(
          kNumRows,
          [](vector_size_t row) {
            return std::to_string(row) +
                std::string(100, static_cast<char>('a' + row % 26));
          }),
  });
  const auto fileBytes = test::writeVortexBytes({input}, pool());

  const auto readBytes = [&](bool projectAllColumns) {
    auto readFile = std::make_shared<InMemoryReadFile>(fileBytes);
    auto bufferedInput =
        std::make_unique<common::BufferedInput>(readFile, *pool());
    common::ReaderOptions readerOptions{pool()};
    auto reader =
        std::make_unique<VortexReader>(std::move(bufferedInput), readerOptions);
    readFile->resetBytesRead();

    auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
    if (projectAllColumns) {
      scanSpec->addAllChildFields(*input->type());
    } else {
      scanSpec->addField("c1", 0);
    }
    common::RowReaderOptions rowReaderOptions;
    rowReaderOptions.setRequestedType(asRowType(input->type()));
    rowReaderOptions.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOptions);

    const auto expected =
        projectAllColumns ? input : makeRowVector({"c1"}, {input->childAt(1)});
    VectorPtr result;
    vector_size_t numRead{0};
    while (const auto batchSize = rowReader->next(kNumRows, result)) {
      test::assertEqualVectors(expected->slice(numRead, batchSize), result);
      numRead += batchSize;
    }
    EXPECT_EQ(numRead, kNumRows);
    return readFile->bytesRead();
  };

  const auto fullScanBytes = readBytes(true);
  const auto projectedScanBytes = readBytes(false);
  EXPECT_GT(fullScanBytes, 0);
  EXPECT_LT(projectedScanBytes, fullScanBytes);
}

TEST_F(VortexReaderTest, nestedFilterPreservesParentNulls) {
  auto record = makeRowVector(
      {"value"},
      {makeFlatVector<int64_t>({0, 99, 2, 77})},
      [](vector_size_t row) { return row == 1 || row == 3; });
  auto input = makeRowVector({"record"}, {record});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  auto unfilteredReader = reader->createRowReader(common::RowReaderOptions{});
  VectorPtr unfilteredResult;
  EXPECT_EQ(
      unfilteredReader->next(input->size(), unfilteredResult), input->size());
  test::assertEqualVectors(input, unfilteredResult);

  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("record")->childByName("value")->setFilter(
      facebook::velox::common::createBigintValues({0}, true));
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  auto expectedRecord = makeRowVector(
      {"value"}, {makeFlatVector<int64_t>({0, 99, 77})}, [](vector_size_t row) {
        return row != 0;
      });
  test::assertEqualVectors(makeRowVector({"record"}, {expectedRecord}), result);
}

TEST_F(VortexReaderTest, nestedConstantParticipatesInFilter) {
  auto input = makeRowVector(
      {"record"},
      {makeRowVector(
          {"value", "text"},
          {
              makeFlatVector<int64_t>({1, 2, 3}),
              makeFlatVector<std::string>({"a", "b", "c"}),
          })});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  auto* valueSpec = scanSpec->childByName("record")->childByName("value");
  valueSpec->setConstantValue(makeConstant<int64_t>(7, 1));
  valueSpec->setFilter(facebook::velox::common::createBigintValues({7}, false));
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  auto expected = makeRowVector(
      {"record"},
      {makeRowVector(
          {"value", "text"},
          {
              makeFlatVector<int64_t>({7, 7, 7}),
              makeFlatVector<std::string>({"a", "b", "c"}),
          })});
  test::assertEqualVectors(expected, result);
  EXPECT_EQ(rowReader->next(input->size(), result), 0);
}

TEST_F(VortexReaderTest, recursiveRequestedTypeWideningAfterSelection) {
  auto input = makeRowVector(
      {"selector", "items", "record"},
      {
          makeFlatVector<int64_t>({0, 1, 2, 3}),
          makeArrayVector<int32_t>({{1, 2}, {3}, {}, {4, 5}}),
          makeRowVector(
              {"small", "text"},
              {
                  makeFlatVector<int32_t>({10, 20, 30, 40}),
                  makeFlatVector<std::string>({"a", "b", "c", "d"}),
              }),
      });
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  const auto requestedType = ROW(
      {"selector", "items", "record"},
      {
          BIGINT(),
          ARRAY(BIGINT()),
          ROW({"missing", "text", "small"}, {INTEGER(), VARCHAR(), BIGINT()}),
      });
  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  options.setRequestedType(requestedType);
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*requestedType);
  scanSpec->childByName("selector")
      ->setFilter(facebook::velox::common::createBigintValues({1, 3}, false));
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  auto expected = makeRowVector(
      {"selector", "items", "record"},
      {
          makeFlatVector<int64_t>({1, 3}),
          makeArrayVector<int64_t>({{3}, {4, 5}}),
          makeRowVector(
              {"missing", "text", "small"},
              {
                  makeNullConstant(TypeKind::INTEGER, 2),
                  makeFlatVector<std::string>({"b", "d"}),
                  makeFlatVector<int64_t>({20, 40}),
              }),
      });
  test::assertEqualVectors(expected, result);
}

TEST_F(VortexReaderTest, recursiveMapWideningPreservesLogicalRows) {
  auto source =
      makeMapVector<int32_t, float>({{{1, 1.5}}, {{2, 2.5}, {3, 3.5}}, {}});
  auto indices = makeIndices({1, 0});
  auto selected = BaseVector::wrapInDictionary(
      nullptr, std::move(indices), 2, std::move(source));

  const auto result = adaptVortexVectorType(selected, MAP(BIGINT(), DOUBLE()));
  auto expected =
      makeMapVector<int64_t, double>({{{2, 2.5}, {3, 3.5}}, {{1, 1.5}}});
  test::assertEqualVectors(expected, result);
}

TEST_F(VortexReaderTest, emptyProjection) {
  auto input = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);
  auto reader = openReader(file->getPath());

  common::RowReaderOptions options;
  options.setRequestedType(ROW({}, {}));
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);
  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), input->size());
  EXPECT_EQ(result->as<RowVector>()->childrenSize(), 0);
}

TEST_F(VortexReaderTest, resultOutlivesReader) {
  auto expected =
      makeRowVector({makeFlatVector<std::string>({"a", "bb", "ccc"})});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), expected);

  VectorPtr retained;
  {
    auto reader = openReader(file->getPath());
    common::RowReaderOptions options;
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*expected->type());
    options.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(options);
    EXPECT_EQ(rowReader->next(expected->size(), retained), expected->size());
    ASSERT_TRUE(isLazyNotLoaded(*retained->as<RowVector>()->childAt(0)));
  }

  test::assertEqualVectors(expected, retained);
}

TEST_F(VortexReaderTest, nativeBuffersOutliveReader) {
  auto expected = makeFlatVector<int64_t>({11, 22, 33});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), makeRowVector({expected}));

  VectorPtr retained;
  {
    auto reader = openReader(file->getPath());
    common::RowReaderOptions options;
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*reader->rowType());
    options.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(options);
    VectorPtr result;
    EXPECT_EQ(rowReader->next(expected->size(), result), expected->size());
    auto* lazy = result->as<RowVector>()->childAt(0)->asChecked<LazyVector>();
    retained = lazy->loadedVectorShared();
  }

  test::assertEqualVectors(expected, retained);
}

TEST_F(VortexReaderTest, nativeImportChargesExactRetainedBytes) {
  auto expected =
      makeNullableFlatVector<int64_t>({11, std::nullopt, 33, 44, 55});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), makeRowVector({expected}));
  const auto statsBefore = pool()->stats();

  VectorPtr retained;
  {
    auto reader = openReader(file->getPath());
    common::RowReaderOptions options;
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*reader->rowType());
    options.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(options);
    VectorPtr result;
    EXPECT_EQ(rowReader->next(expected->size(), result), expected->size());
    retained =
        BaseVector::loadedVectorShared(result->as<RowVector>()->childAt(0));
  }

  const auto statsRetained = pool()->stats();
  EXPECT_EQ(statsRetained.numExternalAllocs - statsBefore.numExternalAllocs, 1);
  EXPECT_EQ(
      statsRetained.cumulativeExternalBytes -
          statsBefore.cumulativeExternalBytes,
      5 * sizeof(int64_t) + 1);
  test::assertEqualVectors(expected, retained);

  retained.reset();
  const auto statsReleased = pool()->stats();
  EXPECT_EQ(statsReleased.numExternalFrees - statsBefore.numExternalFrees, 1);
}

TEST_F(VortexReaderTest, arrowMemoryHandlesRefundAndDeficit) {
  auto root =
      memory::memoryManager()->addRootPool("vortexArrowReservation", 1 << 20);
  auto leaf = root->addLeafChild("vortexArrowReservationLeaf");
  VortexArrowMemory memory{*leaf};
  const auto& callbacks = memory.callbacks();
  ASSERT_NE(callbacks.report_allocation, nullptr);
  ASSERT_NE(callbacks.report_free, nullptr);

  EXPECT_EQ(callbacks.report_allocation(callbacks.context, 100), 0);
  EXPECT_EQ(leaf->usedBytes(), 100);
  callbacks.report_free(callbacks.context, 40);
  EXPECT_EQ(leaf->usedBytes(), 60);
  EXPECT_EQ(callbacks.report_allocation(callbacks.context, 25), 0);
  EXPECT_EQ(leaf->usedBytes(), 85);
  callbacks.report_free(callbacks.context, 0);
  EXPECT_EQ(leaf->usedBytes(), 85);
  callbacks.report_free(callbacks.context, 85);
  EXPECT_EQ(leaf->usedBytes(), 0);

  const auto stats = leaf->stats();
  EXPECT_EQ(stats.numExternalAllocs, 2);
  EXPECT_EQ(stats.numExternalFrees, 2);
  EXPECT_EQ(stats.cumulativeExternalBytes, 125);
}

TEST_F(VortexReaderTest, arrowMemoryRejectsReservation) {
  auto root = memory::memoryManager()->addRootPool(
      "vortexArrowReservationLimit", 1 << 20);
  auto leaf = root->addLeafChild("vortexArrowReservationLimitLeaf");
  VortexArrowMemory memory{*leaf};
  const auto& callbacks = memory.callbacks();

  EXPECT_NE(callbacks.report_allocation(callbacks.context, 2 << 20), 0);
  EXPECT_EQ(leaf->usedBytes(), 0);
  ASSERT_NE(callbacks.last_error, nullptr);
  EXPECT_NE(
      std::string{callbacks.last_error(callbacks.context)}.find(
          "Exceeded memory pool capacity"),
      std::string::npos);
}

TEST_F(VortexReaderTest, arrowFallbackTracksExternalMemory) {
  auto expected =
      makeFlatVector<std::string>({"one", "twenty-two", "three hundred"});
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), makeRowVector({expected}));

  const auto statsBefore = pool()->stats();
  VectorPtr retained;
  {
    auto reader = openReader(file->getPath());
    common::RowReaderOptions options;
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*reader->rowType());
    options.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(options);
    VectorPtr result;
    EXPECT_EQ(rowReader->next(expected->size(), result), expected->size());
    retained =
        BaseVector::loadedVectorShared(result->as<RowVector>()->childAt(0));
  }

  const auto statsRetained = pool()->stats();
  EXPECT_EQ(statsRetained.numExternalAllocs - statsBefore.numExternalAllocs, 1);
  EXPECT_EQ(statsRetained.numExternalFrees - statsBefore.numExternalFrees, 1);
  EXPECT_GT(
      statsRetained.cumulativeExternalBytes,
      statsBefore.cumulativeExternalBytes);
  test::assertEqualVectors(expected, retained);

  retained.reset();
  const auto statsReleased = pool()->stats();
  EXPECT_EQ(statsReleased.numExternalFrees - statsBefore.numExternalFrees, 2);
}

TEST_F(VortexReaderTest, sparseValueHookUsesLazyVectorRows) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
      makeNullableFlatVector<int64_t>({10, std::nullopt, 30, 40, 50}),
  });
  const auto file = TempFilePath::create();
  writeVortex(file->getPath(), input);

  auto reader = openReader(file->getPath());
  common::RowReaderOptions options;
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  scanSpec->addAllChildFields(*input->type());
  scanSpec->childByName("c0")->setFilter(
      facebook::velox::common::createBigintValues({1, 3, 4}, false));
  scanSpec->childByName("c0")->setProjectOut(false);
  scanSpec->childByName("c1")->setChannel(0);
  options.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(input->size(), result), input->size());
  auto* lazy = result->as<RowVector>()->childAt(0)->asChecked<LazyVector>();
  ASSERT_FALSE(lazy->isLoaded());
  std::vector<vector_size_t> rows{0, 2};
  CapturingHook hook(lazy->size());
  const auto memoryBeforeHook = pool()->usedBytes();
  lazy->load(rows, &hook);

  EXPECT_EQ(hook.seen(), std::vector<bool>({true, true, false}));
  EXPECT_EQ(
      hook.values(),
      std::vector<std::optional<int64_t>>({std::nullopt, 50, std::nullopt}));
  EXPECT_TRUE(lazy->isLoaded());
  EXPECT_EQ(pool()->usedBytes(), memoryBeforeHook);
}

} // namespace
} // namespace facebook::velox::dwio::vortex
