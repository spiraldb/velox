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

#include "velox/dwio/vortex/VortexFilter.h"

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/vector/arrow/Bridge.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::dwio::vortex {
namespace {

static_assert(std::is_same_v<vx_velox_ptype, uint32_t>);
static_assert(std::is_same_v<vx_velox_binary_operator, uint32_t>);
static_assert(std::is_same_v<vx_velox_scan_selection_include, uint32_t>);
static_assert(std::is_same_v<vx_velox_primitive_type, uint32_t>);
static_assert(std::is_same_v<vx_velox_validity_kind, uint32_t>);

std::string errorMessage(const vx_error* error) {
  if (error == nullptr) {
    return "Vortex returned an unspecified error";
  }
  const auto message = vx_error_message(error);
  return std::string{message.ptr, message.len};
}

void checkVortexError(vx_error*& error) {
  if (error == nullptr) {
    return;
  }
  const auto errorText = errorMessage(error);
  vx_error_free(error);
  error = nullptr;
  VELOX_FAIL("Vortex filter test failed: {}", errorText);
}

void retainMemoryContext(void*) {}

void releaseMemoryContext(void*) {}

int32_t reportMemoryAllocation(void*, size_t) {
  return 0;
}

void reportMemoryFree(void*, size_t) {}

const char* memoryError(void*) {
  return nullptr;
}

velox::common::FilterPtr sharedFilter(
    std::unique_ptr<velox::common::Filter> filter) {
  return velox::common::FilterPtr{std::move(filter)};
}

class VortexFilterTest : public testing::Test,
                         public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    session_ = vx_session_new();
    VELOX_CHECK_NOT_NULL(session_);
  }

  void TearDown() override {
    vx_session_free(session_);
    session_ = nullptr;
  }

  VectorPtr applyExpression(
      const RowVectorPtr& input,
      const vx_expression* expression) {
    ArrowArray inputArray{};
    ArrowSchema inputSchema{};
    exportToArrow(input, inputArray, pool());
    exportToArrow(input, inputSchema);
    vx_error* error{nullptr};
    const auto* vortexInput =
        vx_array_from_arrow(session_, &inputArray, &inputSchema, false, &error);
    checkVortexError(error);
    VELOX_CHECK_NOT_NULL(vortexInput);

    const auto* vortexResult = vx_array_apply(vortexInput, expression, &error);
    vx_array_free(vortexInput);
    checkVortexError(error);
    VELOX_CHECK_NOT_NULL(vortexResult);

    ArrowArray resultArray{};
    ArrowSchema resultSchema{};
    const vx_velox_arrow_memory_callbacks memoryCallbacks{
        .struct_size = sizeof(vx_velox_arrow_memory_callbacks),
        .abi_version = VX_VELOX_ABI_VERSION,
        .context = nullptr,
        .retain_context = retainMemoryContext,
        .release_context = releaseMemoryContext,
        .report_allocation = reportMemoryAllocation,
        .report_free = reportMemoryFree,
        .last_error = memoryError,
    };
    const auto status = vx_velox_array_export_arrow(
        session_,
        vortexResult,
        &memoryCallbacks,
        &resultSchema,
        &resultArray,
        &error);
    vx_array_free(vortexResult);
    checkVortexError(error);
    VELOX_CHECK_EQ(status, 0);
    return importFromArrowAsOwner(resultSchema, resultArray, pool());
  }

  VortexFilterConversion convert(
      const RowVectorPtr& input,
      const velox::common::FilterPtr& filter) {
    velox::common::ScanSpec scanSpec{"<root>"};
    scanSpec.getOrCreateChild("value")->setFilter(filter);
    return convertVortexFilter(scanSpec, input->type()->asRow());
  }

  template <typename T>
  void testFilter(
      const std::vector<std::optional<T>>& values,
      const TypePtr& type,
      const velox::common::FilterPtr& filter,
      const std::vector<bool>& expected) {
    auto input =
        makeRowVector({"value"}, {makeNullableFlatVector<T>(values, type)});
    auto conversion = convert(input, filter);
    ASSERT_NE(conversion.expression, nullptr);
    EXPECT_TRUE(conversion.fullyConverted());
    EXPECT_EQ(conversion.pushedFilters.size(), 1);
    test::assertEqualVectors(
        makeFlatVector<bool>(expected),
        applyExpression(input, conversion.expression.get()));
  }

  vx_session* session_{nullptr};
};

struct IntegerFilterCase {
  std::string name;
  velox::common::FilterPtr filter;
  std::vector<bool> expected;
};

class VortexIntegerFilterTest
    : public VortexFilterTest,
      public testing::WithParamInterface<IntegerFilterCase> {};

TEST_P(VortexIntegerFilterTest, matchesVeloxNullSemantics) {
  const auto& testCase = GetParam();
  testFilter<int64_t>(
      {-2, -1, 0, 1, 2, std::nullopt},
      BIGINT(),
      testCase.filter,
      testCase.expected);
}

INSTANTIATE_TEST_SUITE_P(
    IntegerFilters,
    VortexIntegerFilterTest,
    testing::Values(
        IntegerFilterCase{
            "range",
            std::make_shared<velox::common::BigintRange>(-1, 1, false),
            {false, true, true, true, false, false}},
        IntegerFilterCase{
            "nullableRange",
            std::make_shared<velox::common::BigintRange>(-1, 1, true),
            {false, true, true, true, false, true}},
        IntegerFilterCase{
            "values",
            sharedFilter(velox::common::createBigintValues({-2, 2}, false)),
            {true, false, false, false, true, false}},
        IntegerFilterCase{
            "hashValues",
            std::make_shared<velox::common::BigintValuesUsingHashTable>(
                -2,
                2,
                std::vector<int64_t>{-2, 2},
                false),
            {true, false, false, false, true, false}},
        IntegerFilterCase{
            "negatedRange",
            std::make_shared<velox::common::NegatedBigintRange>(-1, 1, false),
            {true, false, false, false, true, false}},
        IntegerFilterCase{
            "nullableNegatedValues",
            sharedFilter(
                velox::common::createNegatedBigintValues({-2, 2}, true)),
            {false, true, true, true, false, true}},
        IntegerFilterCase{
            "nullableNegatedHashValues",
            std::make_shared<velox::common::NegatedBigintValuesUsingHashTable>(
                -2,
                2,
                std::vector<int64_t>{-2, 2},
                true),
            {false, true, true, true, false, true}},
        IntegerFilterCase{
            "multiRange",
            std::make_shared<velox::common::BigintMultiRange>(
                [] {
                  std::vector<std::unique_ptr<velox::common::BigintRange>>
                      ranges;
                  ranges.push_back(
                      std::make_unique<velox::common::BigintRange>(
                          -2, -1, false));
                  ranges.push_back(
                      std::make_unique<velox::common::BigintRange>(
                          1, 2, false));
                  return ranges;
                }(),
                false),
            {true, true, false, true, true, false}}),
    [](const testing::TestParamInfo<IntegerFilterCase>& info) {
      return info.param.name;
    });

TEST_F(VortexFilterTest, rejectsInvalidFixedWidthAbiValues) {
  vx_error* error{nullptr};
  std::unique_ptr<const vx_dtype, decltype(&vx_velox_dtype_free)> dtype{
      vx_velox_dtype_new_primitive(
          std::numeric_limits<uint32_t>::max(), false, &error),
      vx_velox_dtype_free};
  EXPECT_EQ(dtype, nullptr);
  ASSERT_NE(error, nullptr);
  EXPECT_GT(vx_velox_error_message(error).len, 0);
  vx_velox_error_free(error);
  error = nullptr;

  VortexExpressionPtr left{vx_velox_expression_root()};
  VortexExpressionPtr right{vx_velox_expression_root()};
  ASSERT_NE(left, nullptr);
  ASSERT_NE(right, nullptr);
  VortexExpressionPtr result{vx_velox_expression_binary(
      std::numeric_limits<uint32_t>::max(), left.get(), right.get(), &error)};
  EXPECT_EQ(result, nullptr);
  ASSERT_NE(error, nullptr);
  EXPECT_GT(vx_velox_error_message(error).len, 0);
  vx_velox_error_free(error);
}

TEST_F(VortexFilterTest, integerWidthsAndBoolValues) {
  testFilter<int8_t>(
      {-128, -2, 0, 2, 127, std::nullopt},
      TINYINT(),
      std::make_shared<velox::common::BigintRange>(-200, 2, false),
      {true, true, true, true, false, false});
  testFilter<int16_t>(
      {-2, 0, 2, std::nullopt},
      SMALLINT(),
      sharedFilter(velox::common::createBigintValues({-2, 2, 70'000}, false)),
      {true, false, true, false});
  testFilter<int32_t>(
      {-2, 0, 2, std::nullopt},
      INTEGER(),
      std::make_shared<velox::common::BoolValue>(true, true),
      {true, false, true, true});
  testFilter<bool>(
      {false, true, std::nullopt},
      BOOLEAN(),
      std::make_shared<velox::common::BoolValue>(true, true),
      {false, true, true});
  testFilter<int64_t>(
      {-1, 0, 1, std::nullopt},
      BIGINT(),
      std::make_shared<velox::common::BoolValue>(false, false),
      {false, true, false, false});
  testFilter<bool>(
      {false, true, std::nullopt},
      BOOLEAN(),
      std::make_shared<velox::common::BoolValue>(false, false),
      {true, false, false});
}

TEST_F(VortexFilterTest, nullTests) {
  testFilter<int64_t>(
      {1, std::nullopt},
      BIGINT(),
      std::make_shared<velox::common::IsNull>(),
      {false, true});
  testFilter<int64_t>(
      {1, std::nullopt},
      BIGINT(),
      std::make_shared<velox::common::IsNotNull>(),
      {true, false});
}

TEST_F(VortexFilterTest, floatingRanges) {
  testFilter<float>(
      {-1.0F,
       -std::numeric_limits<float>::quiet_NaN(),
       0.0F,
       1.0F,
       std::numeric_limits<float>::quiet_NaN(),
       std::nullopt},
      REAL(),
      std::make_shared<velox::common::FloatRange>(
          0.0F, false, false, 0.0F, true, true, true),
      {false, true, true, true, true, true});
  testFilter<double>(
      {-1.0, 0.0, 1.0, std::numeric_limits<double>::quiet_NaN(), std::nullopt},
      DOUBLE(),
      std::make_shared<velox::common::DoubleRange>(
          -1.0, false, true, 1.0, false, false, false),
      {false, true, true, false, false});

  testFilter<double>(
      {-0.0, 0.0, -1.0, 1.0},
      DOUBLE(),
      std::make_shared<velox::common::DoubleRange>(
          0.0, false, false, 0.0, false, false, false),
      {true, true, false, false});
  testFilter<double>(
      {-0.0, 0.0, -1.0, 1.0},
      DOUBLE(),
      std::make_shared<velox::common::DoubleRange>(
          0.0, false, true, 1.0, true, true, false),
      {false, false, false, true});
}

TEST_F(VortexFilterTest, byteRangesAndValues) {
  const std::vector<std::optional<std::string>> values{
      "a", "b", "c", "d", std::nullopt};
  testFilter<std::string>(
      values,
      VARCHAR(),
      std::make_shared<velox::common::BytesRange>(
          "b", false, false, "d", false, true, true),
      {false, true, true, false, true});
  testFilter<std::string>(
      values,
      VARCHAR(),
      std::make_shared<velox::common::BytesValues>(
          std::vector<std::string>{"a", "c"}, false),
      {true, false, true, false, false});
  testFilter<std::string>(
      values,
      VARBINARY(),
      std::make_shared<velox::common::NegatedBytesValues>(
          std::vector<std::string>{"a", "c"}, true),
      {false, true, false, true, true});
  testFilter<std::string>(
      values,
      VARCHAR(),
      std::make_shared<velox::common::NegatedBytesRange>(
          "b", false, false, "c", false, false, false),
      {true, false, false, true, false});

  const std::vector<std::optional<std::string>> binaryValues{
      "",
      std::string{"\0", 1},
      std::string{"\x80", 1},
      std::string{"\xff", 1},
      std::nullopt};
  testFilter<std::string>(
      binaryValues,
      VARBINARY(),
      std::make_shared<velox::common::BytesRange>(
          std::string{"\0", 1},
          false,
          false,
          std::string{"\xff", 1},
          false,
          true,
          false),
      {false, true, true, false, false});
}

TEST_F(VortexFilterTest, topLevelPushdownWithNestedResidual) {
  auto nested = makeRowVector(
      {"score"}, {makeNullableFlatVector<int64_t>({1, 2, 3, std::nullopt})});
  auto input = makeRowVector(
      {"nested", "limit"}, {nested, makeFlatVector<int64_t>({3, 2, 3, 4})});

  velox::common::ScanSpec scanSpec{"<root>"};
  scanSpec.getOrCreateChild("nested")->getOrCreateChild("score")->setFilter(
      std::make_shared<velox::common::BigintRange>(2, 3, false));
  scanSpec.getOrCreateChild("limit")->setFilter(
      std::make_shared<velox::common::BigintRange>(3, 4, false));
  auto conversion = convertVortexFilter(scanSpec, input->type()->asRow());

  ASSERT_NE(conversion.expression, nullptr);
  EXPECT_FALSE(conversion.fullyConverted());
  ASSERT_EQ(conversion.pushedFilters.size(), 1);
  ASSERT_EQ(conversion.residualFilters.size(), 1);
  EXPECT_EQ(conversion.pushedFilters.front(), scanSpec.childByName("limit"));
  EXPECT_EQ(
      conversion.residualFilters.front(),
      scanSpec.childByName("nested")->childByName("score"));
  test::assertEqualVectors(
      makeFlatVector<bool>({true, false, true, true}),
      applyExpression(input, conversion.expression.get()));
}

TEST_F(VortexFilterTest, nestedFilterRemainsResidual) {
  auto nested = makeRowVector(
      {"score"},
      {makeFlatVector<int64_t>({0, 99, 2, 77})},
      [](vector_size_t row) { return row == 1 || row == 3; });
  auto input = makeRowVector({"nested"}, {nested});

  velox::common::ScanSpec scanSpec{"<root>"};
  scanSpec.getOrCreateChild("nested")->getOrCreateChild("score")->setFilter(
      velox::common::createBigintValues({0}, true));
  auto conversion = convertVortexFilter(scanSpec, input->type()->asRow());

  EXPECT_EQ(conversion.expression, nullptr);
  EXPECT_FALSE(conversion.fullyConverted());
  ASSERT_EQ(conversion.residualFilters.size(), 1);
  EXPECT_EQ(
      conversion.residualFilters.front(),
      scanSpec.childByName("nested")->childByName("score"));
}

TEST_F(VortexFilterTest, preservesUnsupportedFilters) {
  const auto rowType =
      ROW({"value", "items", "missing"}, {REAL(), ARRAY(BIGINT()), BIGINT()});
  velox::common::ScanSpec scanSpec{"<root>"};
  auto* mismatched = scanSpec.getOrCreateChild("value");
  mismatched->setFilter(
      std::make_shared<velox::common::DoubleRange>(
          0.0, false, false, 1.0, false, false, false));
  auto* element = scanSpec.getOrCreateChild("items")->getOrCreateChild(
      velox::common::ScanSpec::kArrayElementsFieldName);
  element->setFilter(
      std::make_shared<velox::common::BigintRange>(0, 10, false));
  auto* unsupported = scanSpec.getOrCreateChild("missing");
  auto bloom =
      std::make_shared<velox::common::BigintValuesUsingBloomFilter>(10, false);
  bloom->insert(1);
  unsupported->setFilter(std::move(bloom));
  auto* missing = scanSpec.getOrCreateChild("absent");
  missing->setFilter(std::make_shared<velox::common::IsNotNull>());

  auto conversion = convertVortexFilter(scanSpec, *rowType);
  EXPECT_EQ(conversion.expression, nullptr);
  EXPECT_FALSE(conversion.fullyConverted());
  EXPECT_TRUE(conversion.pushedFilters.empty());
  EXPECT_EQ(conversion.residualFilters.size(), 4);
  EXPECT_EQ(conversion.residualFilters[0], mismatched);
  EXPECT_EQ(conversion.residualFilters[1], element);
  EXPECT_EQ(conversion.residualFilters[2], unsupported);
  EXPECT_EQ(conversion.residualFilters[3], missing);
}

TEST_F(VortexFilterTest, bindsPhysicalSubscripts) {
  auto input = makeRowVector(
      {"first", "second"},
      {makeFlatVector<int64_t>({1, 2}), makeFlatVector<int64_t>({3, 4})});
  velox::common::ScanSpec scanSpec{"<root>"};
  auto* field = scanSpec.getOrCreateChild("logical_name");
  field->setSubscript(1);
  field->setFilter(std::make_shared<velox::common::BigintRange>(4, 4, false));

  auto conversion = convertVortexFilter(scanSpec, input->type()->asRow());
  ASSERT_NE(conversion.expression, nullptr);
  EXPECT_TRUE(conversion.fullyConverted());
  test::assertEqualVectors(
      makeFlatVector<bool>({false, true}),
      applyExpression(input, conversion.expression.get()));
}

TEST_F(VortexFilterTest, preservesConstantsSyntheticAndLogicalTypes) {
  const auto rowType =
      ROW({"constant", "row_index", "date", "decimal"},
          {BIGINT(), BIGINT(), DATE(), DECIMAL(10, 2)});
  velox::common::ScanSpec scanSpec{"<root>"};
  auto* constant = scanSpec.getOrCreateChild("constant");
  constant->setConstantValue(makeConstant<int64_t>(7, 1));
  constant->setFilter(
      std::make_shared<velox::common::BigintRange>(7, 7, false));
  auto* rowIndex = scanSpec.getOrCreateChild("row_index");
  rowIndex->setColumnType(velox::common::ScanSpec::ColumnType::kRowIndex);
  rowIndex->setFilter(
      std::make_shared<velox::common::BigintRange>(0, 10, false));
  auto* date = scanSpec.getOrCreateChild("date");
  date->setFilter(std::make_shared<velox::common::BigintRange>(0, 10, false));
  auto* decimal = scanSpec.getOrCreateChild("decimal");
  decimal->setFilter(
      std::make_shared<velox::common::BigintRange>(0, 10, false));

  auto conversion = convertVortexFilter(scanSpec, *rowType);
  EXPECT_EQ(conversion.expression, nullptr);
  EXPECT_TRUE(conversion.pushedFilters.empty());
  EXPECT_EQ(
      conversion.residualFilters,
      (std::vector<const velox::common::ScanSpec*>{
          constant, rowIndex, date, decimal}));
}

TEST_F(VortexFilterTest, preservesUnsupportedGenericIntegerMultiRange) {
  std::vector<std::unique_ptr<velox::common::Filter>> ranges;
  ranges.push_back(std::make_unique<velox::common::BigintRange>(-2, -1, false));
  ranges.push_back(std::make_unique<velox::common::BigintRange>(1, 2, false));
  auto filter =
      std::make_shared<velox::common::MultiRange>(std::move(ranges), false);
  auto input = makeRowVector({"value"}, {makeFlatVector<int64_t>({1, 2})});

  auto conversion = convert(input, filter);
  EXPECT_EQ(conversion.expression, nullptr);
  EXPECT_FALSE(conversion.fullyConverted());
  EXPECT_TRUE(conversion.pushedFilters.empty());
  EXPECT_EQ(conversion.residualFilters.size(), 1);
}

TEST_F(VortexFilterTest, mixesPushedAndResidualFilters) {
  const auto rowType = ROW({"pushed", "date"}, {BIGINT(), DATE()});
  velox::common::ScanSpec scanSpec{"<root>"};
  auto* pushed = scanSpec.getOrCreateChild("pushed");
  pushed->setFilter(std::make_shared<velox::common::BigintRange>(1, 2, false));
  auto* residual = scanSpec.getOrCreateChild("date");
  residual->setFilter(
      std::make_shared<velox::common::BigintRange>(1, 2, false));

  auto conversion = convertVortexFilter(scanSpec, *rowType);
  EXPECT_NE(conversion.expression, nullptr);
  EXPECT_EQ(
      conversion.pushedFilters,
      (std::vector<const velox::common::ScanSpec*>{pushed}));
  EXPECT_EQ(
      conversion.residualFilters,
      (std::vector<const velox::common::ScanSpec*>{residual}));
}

} // namespace
} // namespace facebook::velox::dwio::vortex
