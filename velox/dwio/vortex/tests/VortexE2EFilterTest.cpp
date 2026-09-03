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

#include "velox/dwio/common/tests/utils/E2EFilterTestBase.h"

#include <algorithm>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "velox/dwio/vortex/VortexFile.h"
#include "velox/dwio/vortex/VortexReader.h"
#include "velox/dwio/vortex/VortexSplitMapper.h"
#include "velox/dwio/vortex/tests/VortexTestFile.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::dwio::vortex {
namespace {

class VortexE2EFilterTest : public common::E2EFilterTestBase,
                            public velox::test::VectorTestBase {
 protected:
  void writeToMemory(
      const TypePtr& /*type*/,
      const std::vector<RowVectorPtr>& batches,
      bool /*forRowGroupSkip*/,
      const std::vector<std::string>& /*indexColumns*/) override {
    sinkData_ = test::writeVortexBytes(batches, leafPool_.get());
  }

  std::unique_ptr<common::Reader> makeReader(
      const common::ReaderOptions& options,
      std::unique_ptr<common::BufferedInput> input) override {
    return std::make_unique<VortexReader>(std::move(input), options);
  }
};

TEST_F(VortexE2EFilterTest, scalarFilters) {
  testRowGroupSkip_ = false;
  testScenario(
      "integer_value:integer,bigint_value:bigint,string_value:string",
      [&]() { makeStringDistribution("string_value", 100, true, true); },
      true,
      {"integer_value", "bigint_value", "string_value"},
      20);
}

TEST_F(VortexE2EFilterTest, nestedFilters) {
  testRowGroupSkip_ = false;
  testScenario(
      "key:bigint,outer:struct<nested:bigint,text:string>",
      nullptr,
      false,
      {"key", "outer.nested", "outer.text"},
      20);
}

TEST_F(VortexE2EFilterTest, metadataPruningStates) {
  constexpr vector_size_t kRowCount = 300'000;
  auto input = makeRowVector({makeFlatVector<int64_t>(
      kRowCount, [](vector_size_t row) { return row; })});
  sinkData_ = test::writeVortexBytes({input}, leafPool_.get());

  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();
  common::ReaderOptions readerOptions{leafPool_.get()};
  VortexFile file{
      std::make_unique<common::BufferedInput>(
          std::make_shared<InMemoryReadFile>(sinkData_),
          readerOptions.memoryPool()),
      *leafPool_,
      {}};
  const auto& naturalSplits = file.naturalSplits();
  ASSERT_GE(naturalSplits.size(), 3);

  struct ScanResult {
    std::vector<int64_t> values;
    common::RuntimeStats stats;
  };
  auto scan = [&](const std::string& expression,
                  std::optional<std::pair<uint64_t, uint64_t>> byteRange) {
    auto untypedExpression =
        parse::DuckSqlExpressionsParser().parseExpr(expression);
    auto typedExpression = core::Expressions::inferTypes(
        untypedExpression, input->type(), leafPool_.get());
    auto queryContext = core::QueryCtx::create();
    exec::SimpleExpressionEvaluator evaluator{
        queryContext.get(), leafPool_.get()};
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
    scanSpec->addAllChildFields(*input->type());
    auto metadataFilter = std::make_shared<velox::common::MetadataFilter>(
        *scanSpec, *typedExpression, &evaluator);

    auto reader = std::make_unique<VortexReader>(
        std::make_unique<common::BufferedInput>(
            std::make_shared<InMemoryReadFile>(sinkData_),
            readerOptions.memoryPool()),
        readerOptions);
    common::RowReaderOptions rowReaderOptions;
    rowReaderOptions.setScanSpec(scanSpec);
    rowReaderOptions.setMetadataFilter(metadataFilter);
    if (byteRange.has_value()) {
      rowReaderOptions.range(byteRange->first, byteRange->second);
    }
    auto rowReader = reader->createRowReader(rowReaderOptions);

    ScanResult scanResult;
    VectorPtr result;
    while (rowReader->next(1'000, result) != 0) {
      DecodedVector decoded{*result->as<RowVector>()->childAt(0)};
      for (vector_size_t i = 0; i < result->size(); ++i) {
        scanResult.values.push_back(decoded.valueAt<int64_t>(i));
      }
    }
    rowReader->updateRuntimeStats(scanResult.stats);
    return scanResult;
  };

  const auto& first = naturalSplits.front().rows;
  const auto& last = naturalSplits.back().rows;
  const auto disjointExpression = "(c0 >= " + std::to_string(first.begin) +
      " and c0 < " + std::to_string(first.end) +
      ") or (c0 >= " + std::to_string(last.begin) + " and c0 < " +
      std::to_string(last.end) + ")";
  const auto disjoint = scan(disjointExpression, std::nullopt);
  ASSERT_EQ(
      disjoint.values.size(), first.end - first.begin + last.end - last.begin);
  size_t outputIndex{0};
  for (const auto* split : {&first, &last}) {
    for (auto row = split->begin; row < split->end; ++row) {
      ASSERT_EQ(disjoint.values[outputIndex++], row);
    }
  }
  EXPECT_EQ(disjoint.stats.skippedStrides, naturalSplits.size() - 2);
  EXPECT_EQ(disjoint.stats.processedStrides, 2);

  const auto negatedExpression = "not (c0 >= " + std::to_string(first.end) +
      " and c0 < " + std::to_string(last.begin) + ")";
  const auto negated = scan(negatedExpression, std::nullopt);
  EXPECT_EQ(negated.values, disjoint.values);
  EXPECT_EQ(negated.stats.skippedStrides, naturalSplits.size() - 2);
  EXPECT_EQ(negated.stats.processedStrides, 2);

  const auto unsupported = scan("c0 < 0 or c0 + 1 > 0", std::nullopt);
  ASSERT_EQ(unsupported.values.size(), kRowCount);
  EXPECT_EQ(unsupported.values.front(), 0);
  EXPECT_EQ(unsupported.values.back(), kRowCount - 1);
  EXPECT_EQ(unsupported.stats.skippedStrides, 0);
  EXPECT_EQ(unsupported.stats.processedStrides, naturalSplits.size());

  const auto byteOffset = file.fileSize() / 2;
  const auto byteLength = file.fileSize() - byteOffset;
  const auto ownedRange =
      VortexSplitMapper::map(naturalSplits, byteOffset, byteLength);
  ASSERT_TRUE(ownedRange.has_value());
  ASSERT_GT(ownedRange->begin, 0);
  const auto ownedSplitCount = std::count_if(
      naturalSplits.begin(), naturalSplits.end(), [&](const auto& split) {
        return split.rows.begin >= ownedRange->begin &&
            split.rows.end <= ownedRange->end;
      });
  const auto excluded = scan("c0 < 0", {{byteOffset, byteLength}});
  EXPECT_TRUE(excluded.values.empty());
  EXPECT_EQ(excluded.stats.skippedStrides, ownedSplitCount);
  EXPECT_EQ(excluded.stats.processedStrides, 0);
}

} // namespace
} // namespace facebook::velox::dwio::vortex
