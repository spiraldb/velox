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

#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Writer.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/nimble/writer/Writer.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/arrow/Bridge.h"

DEFINE_string(
    output_path,
    "",
    "Directory that receives parquet, nimble, and vortex TPC-H data.");
DEFINE_double(scale_factor, 1.0, "TPC-H scale factor.");
DEFINE_uint64(batch_size, 100'000, "Generator rows per input batch.");

namespace facebook::velox::dwio::vortex {
namespace {

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
  VELOX_FAIL("Vortex write failed: {}", errorText);
}

struct VortexSinkReleaser {
  void operator()(vx_array_sink* sink) const {
    if (sink != nullptr) {
      vx_array_sink_abort(sink);
    }
  }
};

class VortexWriter {
 public:
  VortexWriter(std::string path, memory::MemoryPool& pool)
      : path_{std::move(path)},
        pool_{pool},
        session_{vx_session_new(), vx_session_free} {
    VELOX_CHECK_NOT_NULL(session_);
  }

  void write(const RowVectorPtr& batch) {
    ArrowArray arrowArray{};
    ArrowSchema arrowSchema{};
    exportToArrow(batch, arrowArray, &pool_);
    exportToArrow(batch, arrowSchema);

    vx_error* error{nullptr};
    std::unique_ptr<const vx_array, decltype(&vx_array_free)> array{
        vx_array_from_arrow(
            session_.get(), &arrowArray, &arrowSchema, false, &error),
        vx_array_free};
    checkVortexError(error);
    VELOX_CHECK_NOT_NULL(array);

    if (sink_ == nullptr) {
      std::unique_ptr<const vx_dtype, decltype(&vx_dtype_free)> dtype{
          vx_array_dtype(array.get()), vx_dtype_free};
      VELOX_CHECK_NOT_NULL(dtype);
      const vx_view pathView{path_.data(), path_.size()};
      sink_.reset(vx_array_sink_open_file(
          session_.get(), pathView, dtype.get(), &error));
      checkVortexError(error);
      VELOX_CHECK_NOT_NULL(sink_);
    }

    vx_array_sink_push(sink_.get(), array.get(), &error);
    checkVortexError(error);
  }

  void close() {
    vx_error* error{nullptr};
    auto* sink = sink_.release();
    VELOX_CHECK_NOT_NULL(sink);
    vx_array_sink_close(sink, &error);
    checkVortexError(error);
  }

 private:
  const std::string path_;
  memory::MemoryPool& pool_;
  std::unique_ptr<vx_session, decltype(&vx_session_free)> session_;
  std::unique_ptr<vx_array_sink, VortexSinkReleaser> sink_;
};

class DwioWriter {
 public:
  DwioWriter(
      common::FileFormat format,
      const std::string& path,
      const RowTypePtr& type,
      memory::MemoryPool& pool) {
    auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
    if (format == common::FileFormat::NIMBLE) {
      writer_ = std::make_unique<facebook::nimble::Writer>(
          type, std::move(writeFile), pool, facebook::nimble::WriterOptions{});
      return;
    }
    auto sink =
        std::make_unique<common::WriteFileSink>(std::move(writeFile), path);
    const auto factory = common::getWriterFactory(format);
    auto options = factory->createWriterOptions();
    options->schema = type;
    options->memoryPool = &pool;
    writer_ = factory->createWriter(std::move(sink), std::move(options));
  }

  void write(const RowVectorPtr& batch) {
    writer_->write(batch);
  }

  void close() {
    while (!writer_->finish()) {
    }
    writer_->close();
  }

 private:
  std::unique_ptr<common::Writer> writer_;
};

RowVectorPtr generateBatch(
    tpch::Table table,
    memory::MemoryPool& pool,
    size_t batchSize,
    size_t offset,
    double scaleFactor) {
  switch (table) {
    case tpch::Table::TBL_PART:
      return tpch::genTpchPart(&pool, batchSize, offset, scaleFactor);
    case tpch::Table::TBL_SUPPLIER:
      return tpch::genTpchSupplier(&pool, batchSize, offset, scaleFactor);
    case tpch::Table::TBL_PARTSUPP:
      return tpch::genTpchPartSupp(&pool, batchSize, offset, scaleFactor);
    case tpch::Table::TBL_CUSTOMER:
      return tpch::genTpchCustomer(&pool, batchSize, offset, scaleFactor);
    case tpch::Table::TBL_ORDERS:
      return tpch::genTpchOrders(&pool, batchSize, offset, scaleFactor);
    case tpch::Table::TBL_LINEITEM:
      return tpch::genTpchLineItem(&pool, batchSize, offset, scaleFactor);
    case tpch::Table::TBL_NATION:
      return tpch::genTpchNation(&pool, batchSize, offset, scaleFactor);
    case tpch::Table::TBL_REGION:
      return tpch::genTpchRegion(&pool, batchSize, offset, scaleFactor);
  }
  VELOX_UNREACHABLE();
}

size_t generationRows(tpch::Table table, double scaleFactor) {
  if (table == tpch::Table::TBL_LINEITEM) {
    return tpch::getRowCount(tpch::Table::TBL_ORDERS, scaleFactor);
  }
  return tpch::getRowCount(table, scaleFactor);
}

void generateTable(
    tpch::Table table,
    const std::filesystem::path& outputPath,
    memory::MemoryPool& generationPool,
    memory::MemoryPool& parquetPool,
    memory::MemoryPool& nimblePool,
    memory::MemoryPool& vortexPool) {
  const auto tableName = tpch::toTableName(table);
  const auto parquetDirectory = outputPath / "parquet" / tableName;
  const auto nimbleDirectory = outputPath / "nimble" / tableName;
  const auto vortexDirectory = outputPath / "vortex" / tableName;
  std::filesystem::create_directories(parquetDirectory);
  std::filesystem::create_directories(nimbleDirectory);
  std::filesystem::create_directories(vortexDirectory);

  const auto parquetPath = (parquetDirectory / "data.parquet").string();
  const auto nimblePath = (nimbleDirectory / "data.nimble").string();
  const auto vortexPath = (vortexDirectory / "data.vortex").string();
  const auto type = tpch::getTableSchema(table);
  DwioWriter parquetWriter{
      common::FileFormat::PARQUET, parquetPath, type, parquetPool};
  DwioWriter nimbleWriter{
      common::FileFormat::NIMBLE, nimblePath, type, nimblePool};
  VortexWriter vortexWriter{vortexPath, vortexPool};

  const auto totalRows = generationRows(table, FLAGS_scale_factor);
  size_t outputRows{0};
  for (size_t offset = 0; offset < totalRows; offset += FLAGS_batch_size) {
    auto batch = generateBatch(
        table,
        generationPool,
        std::min<size_t>(FLAGS_batch_size, totalRows - offset),
        offset,
        FLAGS_scale_factor);
    VELOX_CHECK_NOT_NULL(batch);
    parquetWriter.write(batch);
    nimbleWriter.write(batch);
    vortexWriter.write(batch);
    outputRows += batch->size();
  }

  parquetWriter.close();
  nimbleWriter.close();
  vortexWriter.close();
  LOG(INFO) << tableName << ": " << outputRows << " rows, parquet "
            << std::filesystem::file_size(parquetPath) << " bytes, nimble "
            << std::filesystem::file_size(nimblePath) << " bytes, vortex "
            << std::filesystem::file_size(vortexPath) << " bytes";
}

} // namespace
} // namespace facebook::velox::dwio::vortex

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  VELOX_USER_CHECK(!FLAGS_output_path.empty(), "output_path must not be empty");
  VELOX_USER_CHECK_GT(FLAGS_scale_factor, 0, "scale_factor must be positive");
  VELOX_USER_CHECK_GT(FLAGS_batch_size, 0, "batch_size must be positive");

  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});
  auto rootPool =
      facebook::velox::memory::memoryManager()->addRootPool("tpch_generator");
  auto pools = rootPool->addAggregateChild("benchmark_data");
  auto generationPool = pools->addLeafChild("generation");
  auto parquetPool = pools->addAggregateChild("parquet");
  auto nimblePool = pools->addAggregateChild("nimble");
  auto vortexPool = pools->addLeafChild("vortex");
  facebook::velox::parquet::registerParquetWriterFactory();

  const std::filesystem::path outputPath{FLAGS_output_path};
  for (const auto table : facebook::velox::tpch::tables) {
    facebook::velox::dwio::vortex::generateTable(
        table,
        outputPath,
        *generationPool,
        *parquetPool,
        *nimblePool,
        *vortexPool);
  }

  facebook::velox::parquet::unregisterParquetWriterFactory();
  return 0;
}
