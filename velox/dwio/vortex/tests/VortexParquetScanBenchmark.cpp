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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <filesystem>

#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/common/testutil/TempFilePath.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/vortex/RegisterVortexReader.h"
#include "velox/dwio/vortex/tests/VortexTestFile.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/DecodedVector.h"

#ifdef VELOX_ENABLE_NIMBLE
#include "velox/dwio/nimble/common/tests/NimbleFileWriter.h"
#include "velox/dwio/nimble/velox/selective/SelectiveNimbleReader.h"
#endif

namespace facebook::velox::dwio::vortex {
namespace {

using exec::Task;
using exec::test::AssertQueryBuilder;
using exec::test::HiveConnectorTestBase;
using exec::test::PlanBuilder;
using facebook::velox::common::testutil::TempDirectoryPath;
using facebook::velox::common::testutil::TempFilePath;

constexpr vector_size_t kRowsPerBatch{65'536};
constexpr vector_size_t kNumBatches{64};
constexpr vector_size_t kNumRows{kRowsPerBatch * kNumBatches};
constexpr vector_size_t kFilteredRows{41'944};
constexpr vector_size_t kSparseFilterRows{83'887};

class VortexFormatScanBenchmark : public HiveConnectorTestBase {
 public:
  VortexFormatScanBenchmark() {
    HiveConnectorTestBase::SetUp();
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
    registerVortexReaderFactory();
#ifdef VELOX_ENABLE_NIMBLE
    facebook::nimble::registerSelectiveNimbleReaderFactory();
#endif

    inputType_ =
        ROW({"filter_key", "group_key", "measure", "payload"},
            {BIGINT(), BIGINT(), BIGINT(), BIGINT()});
    const auto input = makeInput();

    vortexFile_ = TempFilePath::create();
    parquetDirectory_ = TempDirectoryPath::create();
    parquetFilePath_ = parquetDirectory_->getPath() + "/data.parquet";
    test::writeVortexFile(vortexFile_->getPath(), input, pool());
    auto writePlan = PlanBuilder(pool())
                         .values(input)
                         .tableWrite(
                             parquetDirectory_->getPath(),
                             common::FileFormat::PARQUET,
                             {},
                             nullptr,
                             "data.parquet")
                         .planNode();
    AssertQueryBuilder(writePlan).copyResults(pool());
#ifdef VELOX_ENABLE_NIMBLE
    nimbleFile_ = TempFilePath::create();
    const std::vector<VectorPtr> nimbleInput{input.begin(), input.end()};
    nimbleFile_->append(
        facebook::nimble::test::createNimbleFile(
            *pool(), nimbleInput, {}, /*flushAfterWrite=*/false));
#endif

    fullScanPlan_ = PlanBuilder(pool()).tableScan(inputType_).planNode();
    projectionPlan_ =
        PlanBuilder(pool())
            .tableScan(ROW({"measure"}, {BIGINT()}), {}, "", inputType_)
            .planNode();
    filterPlan_ = PlanBuilder(pool())
                      .tableScan(
                          ROW({"measure"}, {BIGINT()}),
                          {"filter_key = 0"},
                          "",
                          inputType_)
                      .planNode();
    aggregationPlan_ = PlanBuilder(pool())
                           .tableScan(
                               ROW({"filter_key", "group_key", "measure"},
                                   {BIGINT(), BIGINT(), BIGINT()}),
                               {},
                               "filter_key = 0 OR filter_key = 99",
                               inputType_)
                           .singleAggregation({"group_key"}, {"sum(measure)"})
                           .planNode();
    verifyPreflight();
    LOG(INFO) << "Vortex benchmark file bytes: " << vortexFile_->fileSize();
    LOG(INFO) << "Parquet benchmark file bytes: "
              << std::filesystem::file_size(parquetFilePath_);
#ifdef VELOX_ENABLE_NIMBLE
    LOG(INFO) << "Nimble benchmark file bytes: " << nimbleFile_->fileSize();
#endif
  }

  ~VortexFormatScanBenchmark() override {
#ifdef VELOX_ENABLE_NIMBLE
    facebook::nimble::unregisterSelectiveNimbleReaderFactory();
#endif
    unregisterVortexReaderFactory();
    parquet::unregisterParquetReaderFactory();
    parquet::unregisterParquetWriterFactory();
    aggregationPlan_.reset();
    filterPlan_.reset();
    projectionPlan_.reset();
    fullScanPlan_.reset();
#ifdef VELOX_ENABLE_NIMBLE
    nimbleFile_.reset();
#endif
    parquetDirectory_.reset();
    vortexFile_.reset();
    HiveConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  void addBenchmarks() {
    addFormatBenchmarks("full_scan", fullScanPlan_);
    addFormatBenchmarks("project_1_of_4", projectionPlan_);
    addFormatBenchmarks("filter_1pct", filterPlan_);
    addFormatBenchmarks("sparse_grouped_sum_hook", aggregationPlan_);
  }

 private:
  std::vector<RowVectorPtr> makeInput() {
    std::vector<RowVectorPtr> input;
    input.reserve(kNumBatches);
    for (vector_size_t batchIndex = 0; batchIndex < kNumBatches; ++batchIndex) {
      const auto batchOffset =
          static_cast<uint64_t>(batchIndex) * kRowsPerBatch;
      input.push_back(makeRowVector(
          inputType_->names(),
          {
              makeFlatVector<int64_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return static_cast<int64_t>((batchOffset + row) % 100);
                  }),
              makeFlatVector<int64_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return static_cast<int64_t>((batchOffset + row) % 1'024);
                  }),
              makeFlatVector<int64_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    const auto value = batchOffset + row;
                    return static_cast<int64_t>(
                        (value * 6'364'136'223'846'793'005ULL +
                         1'442'695'040'888'963'407ULL) &
                        0xFF'FFFF'FFFFULL);
                  }),
              makeFlatVector<int64_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    const auto value = batchOffset + row;
                    return static_cast<int64_t>(
                        (value * 11'400'714'819'323'198'485ULL) ^
                        (value >> 17));
                  }),
          }));
    }
    return input;
  }

  std::shared_ptr<connector::ConnectorSplit> split(
      common::FileFormat format) const {
    if (format == common::FileFormat::VORTEX) {
      return makeHiveConnectorSplits(vortexFile_->getPath(), 1, format).front();
    }
    if (format == common::FileFormat::PARQUET) {
      return makeHiveConnectorSplits(parquetFilePath_, 1, format).front();
    }
#ifdef VELOX_ENABLE_NIMBLE
    if (format == common::FileFormat::NIMBLE) {
      return makeHiveConnectorSplits(nimbleFile_->getPath(), 1, format).front();
    }
#endif
    VELOX_UNREACHABLE("Unsupported benchmark format: {}", format);
  }

  struct BenchmarkFormat {
    std::string_view name;
    common::FileFormat format;
  };

  static std::vector<BenchmarkFormat> benchmarkFormats() {
    std::vector<BenchmarkFormat> formats{
        {"Parquet", common::FileFormat::PARQUET},
    };
#ifdef VELOX_ENABLE_NIMBLE
    formats.push_back({"Nimble", common::FileFormat::NIMBLE});
#endif
    formats.push_back({"Vortex", common::FileFormat::VORTEX});
    return formats;
  }

  uint64_t run(
      const core::PlanNodePtr& plan,
      common::FileFormat format,
      std::shared_ptr<Task>* task = nullptr) const {
    exec::CursorParameters params;
    params.copyResult = false;
    params.maxDrivers = 1;
    params.planNode = plan;
    auto cursor = exec::TaskCursor::create(params);
    const auto leafPlanNodeIds = plan->leafPlanNodeIds();
    VELOX_CHECK_EQ(leafPlanNodeIds.size(), 1);
    const auto& leafPlanNodeId = *leafPlanNodeIds.begin();
    cursor->task()->addSplit(leafPlanNodeId, exec::Split(split(format)));
    cursor->task()->noMoreSplits(leafPlanNodeId);

    uint64_t rowCount{0};
    while (cursor->moveNext()) {
      auto* result = cursor->current()->asChecked<RowVector>();
      for (column_index_t column = 0; column < result->childrenSize();
           ++column) {
        folly::doNotOptimizeAway(result->childAt(column)->loadedVector());
      }
      rowCount += result->size();
    }
    if (task != nullptr) {
      *task = cursor->task();
    }
    folly::doNotOptimizeAway(rowCount);
    return rowCount;
  }

  RowVectorPtr aggregate(common::FileFormat format, std::shared_ptr<Task>& task)
      const {
    return AssertQueryBuilder(aggregationPlan_)
        .split(split(format))
        .maxDrivers(1)
        .copyResults(pool(), task);
  }

  struct ScanDigest {
    uint64_t rowCount{0};
    std::vector<uint64_t> columnHashes;

    bool operator==(const ScanDigest&) const = default;
  };

  ScanDigest scanDigest(
      const core::PlanNodePtr& plan,
      common::FileFormat format) const {
    exec::CursorParameters params;
    params.copyResult = false;
    params.maxDrivers = 1;
    params.planNode = plan;
    auto cursor = exec::TaskCursor::create(params);
    const auto leafPlanNodeIds = plan->leafPlanNodeIds();
    VELOX_CHECK_EQ(leafPlanNodeIds.size(), 1);
    const auto& leafPlanNodeId = *leafPlanNodeIds.begin();
    cursor->task()->addSplit(leafPlanNodeId, exec::Split(split(format)));
    cursor->task()->noMoreSplits(leafPlanNodeId);

    ScanDigest digest;
    while (cursor->moveNext()) {
      auto* result = cursor->current()->asChecked<RowVector>();
      if (digest.columnHashes.empty()) {
        digest.columnHashes.assign(
            result->childrenSize(), 0xcbf29ce484222325ULL);
      }
      VELOX_CHECK_EQ(digest.columnHashes.size(), result->childrenSize());
      for (column_index_t column = 0; column < result->childrenSize();
           ++column) {
        auto loaded = BaseVector::loadedVectorShared(result->childAt(column));
        VELOX_CHECK_EQ(loaded->typeKind(), TypeKind::BIGINT);
        DecodedVector decoded{*loaded};
        for (vector_size_t row = 0; row < result->size(); ++row) {
          const auto value = decoded.isNullAt(row)
              ? 0x9ae16a3b2f90404fULL
              : static_cast<uint64_t>(decoded.valueAt<int64_t>(row));
          digest.columnHashes[column] ^= value;
          digest.columnHashes[column] *= 0x100000001b3ULL;
        }
      }
      digest.rowCount += result->size();
    }
    return digest;
  }

  static int64_t loadedToValueHook(const std::shared_ptr<Task>& task) {
    int64_t loadedRows{0};
    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& operatorStats : pipeline.operatorStats) {
        const auto metric =
            operatorStats.runtimeStats.find("loadedToValueHook");
        if (metric != operatorStats.runtimeStats.end()) {
          loadedRows += metric->second.sum;
        }
      }
    }
    return loadedRows;
  }

  void verifyPreflight() const {
    const auto formats = benchmarkFormats();
    for (const auto& benchmarkFormat : formats) {
      VELOX_CHECK_EQ(run(fullScanPlan_, benchmarkFormat.format), kNumRows);
      VELOX_CHECK_EQ(run(projectionPlan_, benchmarkFormat.format), kNumRows);
      VELOX_CHECK_EQ(run(filterPlan_, benchmarkFormat.format), kFilteredRows);
    }

    for (const auto& plan : {fullScanPlan_, projectionPlan_, filterPlan_}) {
      const auto reference = scanDigest(plan, common::FileFormat::PARQUET);
      for (const auto& benchmarkFormat : formats) {
        VELOX_CHECK(
            reference == scanDigest(plan, benchmarkFormat.format),
            "Parquet and {} produced different materialized values",
            benchmarkFormat.name);
      }
    }

    std::shared_ptr<Task> referenceTask;
    auto referenceResult =
        aggregate(common::FileFormat::PARQUET, referenceTask);
    VELOX_CHECK_EQ(referenceResult->size(), 512);
    VELOX_CHECK_EQ(loadedToValueHook(referenceTask), kSparseFilterRows);
    for (const auto& benchmarkFormat : formats) {
      std::shared_ptr<Task> task;
      auto result = aggregate(benchmarkFormat.format, task);
      VELOX_CHECK(
          exec::test::assertEqualResults(
              std::vector<RowVectorPtr>{referenceResult},
              std::vector<RowVectorPtr>{result}));
      VELOX_CHECK_EQ(result->size(), 512);
      VELOX_CHECK_EQ(loadedToValueHook(task), kSparseFilterRows);
    }
  }

  void addFormatBenchmarks(
      std::string_view caseName,
      const core::PlanNodePtr& plan) {
    for (const auto& benchmarkFormat : benchmarkFormats()) {
      folly::addBenchmark(
          __FILE__,
          fmt::format("{}/{}", benchmarkFormat.name, caseName),
          [this, plan, format = benchmarkFormat.format]() {
            run(plan, format);
            return 1;
          });
    }
  }

  RowTypePtr inputType_;
  std::shared_ptr<TempFilePath> vortexFile_;
#ifdef VELOX_ENABLE_NIMBLE
  std::shared_ptr<TempFilePath> nimbleFile_;
#endif
  std::shared_ptr<TempDirectoryPath> parquetDirectory_;
  std::string parquetFilePath_;
  core::PlanNodePtr fullScanPlan_;
  core::PlanNodePtr projectionPlan_;
  core::PlanNodePtr filterPlan_;
  core::PlanNodePtr aggregationPlan_;
};

} // namespace
} // namespace facebook::velox::dwio::vortex

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});
  facebook::velox::memory::SharedArbitrator::registerFactory();
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::aggregate::prestosql::registerAllAggregateFunctions();

  facebook::velox::dwio::vortex::VortexFormatScanBenchmark benchmark;
  benchmark.addBenchmarks();
  folly::runBenchmarks();
  return 0;
}
