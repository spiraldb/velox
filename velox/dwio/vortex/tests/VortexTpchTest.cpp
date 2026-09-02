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

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/ConnectorRegistry.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/vortex/RegisterVortexReader.h"
#include "velox/dwio/vortex/tests/VortexTestFile.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::dwio::vortex {
namespace {

using exec::CursorParameters;
using exec::Split;
using exec::Task;
using exec::TaskCursor;
using exec::test::AssertQueryBuilder;
using exec::test::DuckDbQueryRunner;
using exec::test::HiveConnectorTestBase;
using exec::test::PlanBuilder;
using exec::test::TpchPlan;
using exec::test::TpchQueryBuilder;
using facebook::velox::common::testutil::TempDirectoryPath;

class VortexTpchTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});

    duckDb_ = std::make_shared<DuckDbQueryRunner>();
    tempDirectory_ = TempDirectoryPath::create();

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();
    registerVortexReaderFactory();

    connector::hive::HiveConnectorFactory hiveFactory;
    auto hiveConnector = hiveFactory.newConnector(
        exec::test::kHiveConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>()));
    connector::ConnectorRegistry::global().insert(
        hiveConnector->connectorId(), hiveConnector);

    connector::tpch::TpchConnectorFactory tpchFactory;
    auto tpchConnector = tpchFactory.newConnector(
        kTpchConnectorId,
        std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>()));
    connector::ConnectorRegistry::global().insert(
        tpchConnector->connectorId(), tpchConnector);

    saveTpchTables();
    tpchBuilder_ =
        std::make_shared<TpchQueryBuilder>(common::FileFormat::VORTEX);
    tpchBuilder_->initialize(tempDirectory_->getPath());
  }

  static void TearDownTestSuite() {
    tpchBuilder_.reset();
    duckDb_.reset();
    tempDirectory_.reset();
    connector::ConnectorRegistry::global().erase(exec::test::kHiveConnectorId);
    connector::ConnectorRegistry::global().erase(kTpchConnectorId);
    unregisterVortexReaderFactory();
  }

  static void saveTpchTables() {
    auto rootPool = memory::memoryManager()->addRootPool();
    auto pool = rootPool->addLeafChild("vortex_tpch");

    for (const auto& table : tpch::tables) {
      const auto tableName = toTableName(table);
      const auto tableDirectory =
          fmt::format("{}/{}", tempDirectory_->getPath(), tableName);
      std::filesystem::create_directories(tableDirectory);

      auto columnNames = tpch::getTableSchema(table)->names();
      auto plan = PlanBuilder()
                      .tpchTableScan(table, std::move(columnNames), 0.01)
                      .planNode();
      auto split = Split(
          std::make_shared<connector::tpch::TpchConnectorSplit>(
              kTpchConnectorId, true, 1, 0));
      auto rows =
          AssertQueryBuilder(plan).splits({split}).copyResults(pool.get());
      duckDb_->createTable(tableName.data(), {rows});
      test::writeVortexFile(
          fmt::format("{}/data.vortex", tableDirectory), {rows}, pool.get());
    }
  }

  void assertTpchQuery(
      int queryId,
      const std::optional<std::vector<uint32_t>>& sortingKeys = {}) {
    assertTpchQuery(
        tpchBuilder_->getQueryPlan(queryId),
        tpch::getQuery(queryId),
        sortingKeys);
  }

  std::shared_ptr<Task> assertTpchQuery(
      const TpchPlan& tpchPlan,
      const std::string& duckDbQuery,
      const std::optional<std::vector<uint32_t>>& sortingKeys) const {
    constexpr int kNumSplits = 10;
    constexpr int kNumDrivers = 4;
    auto addSplits = [&](TaskCursor* taskCursor) {
      if (taskCursor->noMoreSplits()) {
        return;
      }
      auto& task = taskCursor->task();
      for (const auto& [planNodeId, paths] : tpchPlan.dataFiles) {
        for (const auto& path : paths) {
          const auto splits = HiveConnectorTestBase::makeHiveConnectorSplits(
              path, kNumSplits, tpchPlan.dataFileFormat);
          for (const auto& split : splits) {
            task->addSplit(planNodeId, Split(split));
          }
        }
        task->noMoreSplits(planNodeId);
      }
      taskCursor->setNoMoreSplits();
    };
    CursorParameters parameters;
    parameters.maxDrivers = kNumDrivers;
    parameters.planNode = tpchPlan.plan;
    return exec::test::assertQuery(
        parameters, addSplits, duckDbQuery, *duckDb_, sortingKeys);
  }

  static std::shared_ptr<DuckDbQueryRunner> duckDb_;
  static std::shared_ptr<TempDirectoryPath> tempDirectory_;
  static std::shared_ptr<TpchQueryBuilder> tpchBuilder_;

  static constexpr const char* kTpchConnectorId{"test-tpch"};
};

std::shared_ptr<DuckDbQueryRunner> VortexTpchTest::duckDb_;
std::shared_ptr<TempDirectoryPath> VortexTpchTest::tempDirectory_;
std::shared_ptr<TpchQueryBuilder> VortexTpchTest::tpchBuilder_;

TEST_F(VortexTpchTest, q1) {
  assertTpchQuery(1);
}

TEST_F(VortexTpchTest, q2) {
  assertTpchQuery(2, std::vector<uint32_t>{0, 1, 2, 3});
}

TEST_F(VortexTpchTest, q3) {
  assertTpchQuery(3, std::vector<uint32_t>{1, 2});
}

TEST_F(VortexTpchTest, q4) {
  assertTpchQuery(4, std::vector<uint32_t>{0});
}

TEST_F(VortexTpchTest, q5) {
  assertTpchQuery(5, std::vector<uint32_t>{1});
}

TEST_F(VortexTpchTest, q6) {
  assertTpchQuery(6);
}

TEST_F(VortexTpchTest, q7) {
  assertTpchQuery(7, std::vector<uint32_t>{0, 1, 2});
}

TEST_F(VortexTpchTest, q8) {
  assertTpchQuery(8, std::vector<uint32_t>{0});
}

TEST_F(VortexTpchTest, q9) {
  assertTpchQuery(9, std::vector<uint32_t>{0, 1});
}

TEST_F(VortexTpchTest, q10) {
  assertTpchQuery(10, std::vector<uint32_t>{2});
}

TEST_F(VortexTpchTest, q11) {
  assertTpchQuery(11, std::vector<uint32_t>{1});
}

TEST_F(VortexTpchTest, q12) {
  assertTpchQuery(12, std::vector<uint32_t>{0});
}

TEST_F(VortexTpchTest, q13) {
  assertTpchQuery(13, std::vector<uint32_t>{0, 1});
}

TEST_F(VortexTpchTest, q14) {
  assertTpchQuery(14);
}

TEST_F(VortexTpchTest, q15) {
  assertTpchQuery(15, std::vector<uint32_t>{0});
}

TEST_F(VortexTpchTest, q16) {
  assertTpchQuery(16, std::vector<uint32_t>{0, 1, 2, 3});
}

TEST_F(VortexTpchTest, q17) {
  assertTpchQuery(17);
}

TEST_F(VortexTpchTest, q18) {
  assertTpchQuery(18);
}

TEST_F(VortexTpchTest, q19) {
  assertTpchQuery(19);
}

TEST_F(VortexTpchTest, q20) {
  assertTpchQuery(20, std::vector<uint32_t>{0});
}

TEST_F(VortexTpchTest, q21) {
  assertTpchQuery(21, std::vector<uint32_t>{0, 1});
}

TEST_F(VortexTpchTest, q22) {
  assertTpchQuery(22, std::vector<uint32_t>{0});
}

} // namespace
} // namespace facebook::velox::dwio::vortex

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
