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

#include "velox/dwio/vortex/RegisterVortexReader.h"

#include <gtest/gtest.h>

#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/common/testutil/TempFilePath.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/iceberg/DeletionVectorWriter.h"
#include "velox/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "velox/connectors/hive/iceberg/tests/IcebergTestBase.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/vortex/VortexFile.h"
#include "velox/dwio/vortex/tests/VortexTestFile.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::dwio::vortex {
namespace {

using connector::hive::iceberg::DeletionVectorWriter;
using connector::hive::iceberg::FileContent;
using connector::hive::iceberg::IcebergDeleteFile;
using connector::hive::iceberg::IcebergMetadataColumn;
using connector::hive::iceberg::writePuffinFile;
using connector::hive::iceberg::test::IcebergTestBase;
using exec::test::AssertQueryBuilder;
using exec::test::PlanBuilder;
using facebook::velox::common::testutil::TempDirectoryPath;
using facebook::velox::common::testutil::TempFilePath;

class VortexTableScanTest : public IcebergTestBase {
 protected:
  void SetUp() override {
    IcebergTestBase::SetUp();
    registerVortexReaderFactory();
    fileFormat_ = common::FileFormat::VORTEX;
  }

  void TearDown() override {
    unregisterVortexReaderFactory();
    IcebergTestBase::TearDown();
  }

  std::pair<std::shared_ptr<TempDirectoryPath>, IcebergDeleteFile>
  writeDeletionVector(
      const std::string& dataFilePath,
      const std::vector<int64_t>& deletedPositions) {
    DeletionVectorWriter writer;
    writer.addDeletedPositions(deletedPositions);
    const auto blob = writer.serialize();

    auto directory = TempDirectoryPath::create();
    const auto path = directory->getPath() + "/deletes.puffin";
    auto sink = common::FileSink::create("file:" + path, {.pool = pool()});
    const auto [offset, length] = writePuffinFile(
        *sink,
        *pool(),
        blob,
        dataFilePath,
        static_cast<int64_t>(deletedPositions.size()));
    sink->close();

    return {
        std::move(directory),
        IcebergDeleteFile{
            FileContent::kDeletionVector,
            path,
            common::FileFormat::VORTEX,
            deletedPositions.size(),
            getFileSize(path),
            {},
            {},
            {},
            0,
            static_cast<int64_t>(offset),
            static_cast<int64_t>(length),
            dataFilePath,
        }};
  }
};

std::vector<std::shared_ptr<connector::ConnectorSplit>> makeExactSplits(
    const std::string& path,
    uint64_t fileSize,
    uint32_t splitCount) {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
  splits.reserve(splitCount);
  for (uint32_t i = 0; i < splitCount; ++i) {
    const auto begin = fileSize * i / splitCount;
    const auto end = fileSize * (i + 1) / splitCount;
    if (begin == end) {
      continue;
    }
    splits.push_back(
        connector::hive::HiveConnectorSplitBuilder(path)
            .connectorId("test-hive")
            .fileFormat(common::FileFormat::VORTEX)
            .start(begin)
            .length(end - begin)
            .build());
  }
  return splits;
}

TEST_F(VortexTableScanTest, hiveScanWithFiltersAndMultipleSplits) {
  constexpr vector_size_t kRowCount = 300'000;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kRowCount, folly::identity),
      makeFlatVector<int64_t>(
          kRowCount, [](vector_size_t row) { return row % 17; }),
  });
  const auto file = TempFilePath::create();
  test::writeVortexFile(file->getPath(), {input}, pool());
  createDuckDbTable({input});

  VortexFile vortexFile{
      std::make_unique<common::BufferedInput>(
          std::make_shared<LocalReadFile>(file->getPath()), *pool()),
      *pool()};
  ASSERT_GT(vortexFile.naturalSplits().size(), 1);

  auto plan = PlanBuilder(pool())
                  .tableScan(asRowType(input->type()), {"c0 >= 1234"})
                  .planNode();
  for (const auto splitCount : {1, 2, 3, 7, 16}) {
    SCOPED_TRACE(splitCount);
    assertQuery(
        plan,
        makeExactSplits(file->getPath(), file->fileSize(), splitCount),
        "SELECT * FROM tmp WHERE c0 >= 1234");
  }
}

TEST_F(VortexTableScanTest, icebergPositionalDeletes) {
  auto input = makeRowVector({makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5})});
  const auto dataFile = TempFilePath::create();
  test::writeVortexFile(dataFile->getPath(), {input}, pool());

  const auto deleteFile = TempFilePath::create();
  const auto pathColumn = IcebergMetadataColumn::icebergDeleteFilePathColumn();
  const auto positionColumn = IcebergMetadataColumn::icebergDeletePosColumn();
  writeToFile(
      deleteFile->getPath(),
      makeRowVector(
          {pathColumn->name, positionColumn->name},
          {
              makeFlatVector<std::string>(
                  2, [&](vector_size_t) { return dataFile->getPath(); }),
              makeFlatVector<int64_t>({1, 4}),
          }));
  const IcebergDeleteFile positionalDeletes{
      FileContent::kPositionalDeletes,
      deleteFile->getPath(),
      common::FileFormat::DWRF,
      2,
      getFileSize(deleteFile->getPath()),
  };

  auto plan = makeIcebergTableScanPlan(asRowType(input->type()));
  AssertQueryBuilder(plan)
      .splits(
          makeIcebergSplits(dataFile->getPath(), {positionalDeletes}, {}, 3))
      .assertResults(makeRowVector({makeFlatVector<int64_t>({0, 2, 3, 5})}));
}

TEST_F(VortexTableScanTest, icebergDeletionVector) {
  auto input =
      makeRowVector({makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9})});
  const auto dataFile = TempFilePath::create();
  test::writeVortexFile(dataFile->getPath(), {input}, pool());
  const auto deletionVector =
      writeDeletionVector(dataFile->getPath(), {1, 4, 8});

  auto plan = makeIcebergTableScanPlan(asRowType(input->type()));
  AssertQueryBuilder(plan)
      .splits(makeIcebergSplits(
          dataFile->getPath(), {deletionVector.second}, {}, 3))
      .assertResults(
          makeRowVector({makeFlatVector<int64_t>({0, 2, 3, 5, 6, 7, 9})}));
}

TEST_F(VortexTableScanTest, sparseGroupedAggregationLoadsLazyRows) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 0, 1, 1, 2}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50}),
  });
  const auto file = TempFilePath::create();
  test::writeVortexFile(file->getPath(), {input}, pool());
  createDuckDbTable({input});

  auto plan = PlanBuilder(pool())
                  .startTableScan()
                  .outputType(asRowType(input->type()))
                  .remainingFilter("c0 = 0 OR c0 = 2")
                  .endTableScan()
                  .singleAggregation({"c0"}, {"sum(c1)"})
                  .planNode();
  const auto hiveSplits =
      makeHiveConnectorSplits(file->getPath(), 1, common::FileFormat::VORTEX);
  std::vector<std::shared_ptr<connector::ConnectorSplit>> connectorSplits{
      hiveSplits.begin(), hiveSplits.end()};
  auto task = AssertQueryBuilder(plan)
                  .splits(connectorSplits)
                  .assertResults(makeRowVector({
                      makeFlatVector<int64_t>({0, 2}),
                      makeFlatVector<int64_t>({30, 50}),
                  }));
  int64_t loadedToValueHook{0};
  for (const auto& pipeline : task->taskStats().pipelineStats) {
    for (const auto& operatorStats : pipeline.operatorStats) {
      if (const auto metric =
              operatorStats.runtimeStats.find("loadedToValueHook");
          metric != operatorStats.runtimeStats.end()) {
        loadedToValueHook += metric->second.sum;
      }
    }
  }
  EXPECT_GT(loadedToValueHook, 0);
}

TEST_F(VortexTableScanTest, joinDynamicFilterReachesReader) {
  constexpr vector_size_t kRowCount = 20;
  auto probe = makeRowVector(
      {"k", "probe_payload"},
      {
          makeFlatVector<int64_t>(
              kRowCount, [](vector_size_t row) { return row % 10; }),
          makeFlatVector<int64_t>(kRowCount, folly::identity),
      });
  auto build = makeRowVector(
      {"dk", "build_payload"},
      {
          makeFlatVector<int64_t>({1, 3, 6, 9}),
          makeFlatVector<int64_t>({101, 103, 106, 109}),
      });
  const auto file = TempFilePath::create();
  test::writeVortexFile(file->getPath(), {probe}, pool());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;
  core::PlanNodeId joinId;
  auto plan = PlanBuilder(planNodeIdGenerator, pool())
                  .tableScan(asRowType(probe->type()))
                  .capturePlanNodeId(probeScanId)
                  .hashJoin(
                      {"k"},
                      {"dk"},
                      PlanBuilder(planNodeIdGenerator, pool())
                          .values({build})
                          .planNode(),
                      "",
                      {"k", "probe_payload", "build_payload"})
                  .capturePlanNodeId(joinId)
                  .planNode();

  auto task = AssertQueryBuilder(plan)
                  .split(
                      probeScanId,
                      makeHiveConnectorSplits(
                          file->getPath(), 1, common::FileFormat::VORTEX)
                          .front())
                  .maxDrivers(1)
                  .assertResults(makeRowVector(
                      {"k", "probe_payload", "build_payload"},
                      {
                          makeFlatVector<int64_t>({1, 3, 6, 9, 1, 3, 6, 9}),
                          makeFlatVector<int64_t>({1, 3, 6, 9, 11, 13, 16, 19}),
                          makeFlatVector<int64_t>(
                              {101, 103, 106, 109, 101, 103, 106, 109}),
                      }));

  const auto planStats = exec::toPlanStats(task->taskStats());
  const auto& scanStats = planStats.at(probeScanId);
  EXPECT_EQ(
      planStats.at(joinId).customStats.at("dynamicFiltersProduced").sum, 1);
  EXPECT_EQ(scanStats.customStats.at("dynamicFiltersAccepted").sum, 1);
  EXPECT_EQ(
      scanStats.dynamicFilterStats.producerNodeIds,
      std::unordered_set<core::PlanNodeId>({joinId}));
  EXPECT_EQ(scanStats.rawInputRows, kRowCount);
  EXPECT_EQ(scanStats.inputRows, 8);
  EXPECT_EQ(scanStats.outputRows, 8);
}

} // namespace
} // namespace facebook::velox::dwio::vortex
