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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/nimble/common/tests/NimbleFileWriter.h"
#include "velox/dwio/nimble/velox/selective/SelectiveNimbleReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/dwio/vortex/RegisterVortexReader.h"
#include "velox/dwio/vortex/tests/VortexTestFile.h"
#include "velox/type/Filter.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::dwio::vortex {
namespace {

constexpr vector_size_t kRowsPerBatch{65'536};
constexpr vector_size_t kNumBatches{16};
constexpr uint64_t kNumRows{static_cast<uint64_t>(kRowsPerBatch) * kNumBatches};
constexpr uint32_t kDefaultOutputBatchSize{16'384};
constexpr uint64_t kInitialDigest{0xcbf29ce484222325ULL};

struct FilterRange {
  int64_t lower;
  int64_t upper;
};

struct Workload {
  std::string name;
  std::vector<std::string> projectedColumns;
  std::optional<FilterRange> filterRange;
  uint32_t outputBatchSize;
  uint64_t expectedOutputRows;
};

struct FormatData {
  std::string name;
  common::FileFormat format;
  std::string fileBytes;
};

struct ScanResult {
  uint64_t scannedRows{0};
  uint64_t outputRows{0};
  uint64_t outputBatches{0};
  uint64_t nullValues{0};
  uint64_t digest{kInitialDigest};

  bool sameValues(const ScanResult& other) const {
    return outputRows == other.outputRows && nullValues == other.nullValues &&
        digest == other.digest;
  }
};

class VortexRowReaderBenchmark final : public test::VectorTestBase {
 public:
  VortexRowReaderBenchmark() {
    parquet::registerParquetReaderFactory();
    facebook::nimble::registerSelectiveNimbleReaderFactory();
    registerVortexReaderFactory();

    inputType_ =
        ROW({"filter_key",
             "bigint_value",
             "dictionary_bigint_value",
             "constant_bigint_value",
             "double_value",
             "string_value",
             "dictionary_string_value",
             "constant_string_value",
             "unique_string_value",
             "boolean_value",
             "date_value",
             "short_decimal_value",
             "timestamp_value",
             "array_value",
             "map_value",
             "row_value"},
            {BIGINT(),
             BIGINT(),
             BIGINT(),
             BIGINT(),
             DOUBLE(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             BOOLEAN(),
             DATE(),
             DECIMAL(18, 2),
             TIMESTAMP(),
             ARRAY(BIGINT()),
             MAP(BIGINT(), BIGINT()),
             ROW({"number", "text"}, {BIGINT(), VARCHAR()})});
    batches_ = makeInput();

    formats_.push_back(
        {"Parquet", common::FileFormat::PARQUET, writeParquetBytes()});
    formats_.push_back(
        {"Nimble", common::FileFormat::NIMBLE, writeNimbleBytes()});
    formats_.push_back(
        {"Vortex",
         common::FileFormat::VORTEX,
         test::writeVortexBytes(batches_, pool())});

    workloads_ = makeWorkloads();
    for (const auto& format : formats_) {
      LOG(INFO) << format.name
                << " benchmark file bytes: " << format.fileBytes.size();
    }
    verifyPreflight();
  }

  ~VortexRowReaderBenchmark() {
    unregisterVortexReaderFactory();
    facebook::nimble::unregisterSelectiveNimbleReaderFactory();
    parquet::unregisterParquetReaderFactory();
  }

  void addBenchmarks() {
    for (size_t workloadIndex = 0; workloadIndex < workloads_.size();
         ++workloadIndex) {
      for (size_t formatIndex = 0; formatIndex < formats_.size();
           ++formatIndex) {
        folly::addBenchmark(
            __FILE__,
            fmt::format(
                "{}/{}",
                workloads_[workloadIndex].name,
                formats_[formatIndex].name),
            [this, workloadIndex, formatIndex]() {
              const auto result =
                  scan(formats_[formatIndex], workloads_[workloadIndex], false);
              folly::doNotOptimizeAway(result.scannedRows);
              folly::doNotOptimizeAway(result.outputRows);
              folly::doNotOptimizeAway(result.outputBatches);
              return 1;
            });
      }
    }
  }

 private:
  std::vector<RowVectorPtr> makeInput() {
    std::vector<RowVectorPtr> batches;
    batches.reserve(kNumBatches);
    for (vector_size_t batch = 0; batch < kNumBatches; ++batch) {
      const auto batchOffset = static_cast<uint64_t>(batch) * kRowsPerBatch;
      batches.push_back(makeRowVector(
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
                    const auto value = batchOffset + row;
                    return static_cast<int64_t>(
                        value * 6'364'136'223'846'793'005ULL +
                        1'442'695'040'888'963'407ULL);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 5 == 0;
                  }),
              makeFlatVector<int64_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return static_cast<int64_t>((batchOffset + row) % 16);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 19 == 0;
                  }),
              makeConstant<int64_t>(42, kRowsPerBatch),
              makeFlatVector<double>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    const auto value = batchOffset + row;
                    return static_cast<double>((value * 17) % 1'000'003) / 10.0;
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 7 == 0;
                  }),
              makeFlatVector<std::string>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return fmt::format(
                        "string-value-{:04x}", (batchOffset + row) % 4'096);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 11 == 0;
                  }),
              makeFlatVector<std::string>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return fmt::format(
                        "dictionary-string-value-{:02x}",
                        (batchOffset + row) % 16);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 19 == 0;
                  }),
              makeConstant<std::string>("constant-string-value", kRowsPerBatch),
              makeFlatVector<std::string>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return fmt::format(
                        "unique-string-value-{:08x}", batchOffset + row);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 17 == 0;
                  }),
              makeFlatVector<bool>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return ((batchOffset + row) & 3) != 0;
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 13 == 0;
                  }),
              makeFlatVector<int32_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return static_cast<int32_t>(
                        (batchOffset + row) % 36'525 - 18'262);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 23 == 0;
                  },
                  DATE()),
              makeFlatVector<int64_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    const auto value = batchOffset + row;
                    return static_cast<int64_t>(
                               (value * 7'919) % 1'000'000'000'000ULL) -
                        500'000'000'000LL;
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 29 == 0;
                  },
                  DECIMAL(18, 2)),
              makeFlatVector<Timestamp>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    const auto value = batchOffset + row;
                    const auto nanos =
                        static_cast<int64_t>(
                            (value * 65'537) % 1'000'000'000'000'000ULL) -
                        500'000'000'000'000LL;
                    return Timestamp::fromNanos(nanos);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 31 == 0;
                  }),
              makeArrayVector<int64_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    const auto absoluteRow = batchOffset + row;
                    return absoluteRow % 37 == 0
                        ? 0
                        : static_cast<vector_size_t>(absoluteRow % 5);
                  },
                  [batchOffset](vector_size_t index) {
                    return static_cast<int64_t>(batchOffset + index);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 37 == 0;
                  },
                  [batchOffset](vector_size_t index) {
                    return (batchOffset + index) % 41 == 0;
                  }),
              makeMapVector<int64_t, int64_t>(
                  kRowsPerBatch,
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 59 == 0
                        ? 0
                        : static_cast<vector_size_t>((batchOffset + row) % 5);
                  },
                  [batchOffset](vector_size_t index) {
                    return static_cast<int64_t>(batchOffset + index);
                  },
                  [batchOffset](vector_size_t index) {
                    return static_cast<int64_t>((batchOffset + index) * 17 + 3);
                  },
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 59 == 0;
                  },
                  [batchOffset](vector_size_t index) {
                    return (batchOffset + index) % 61 == 0;
                  }),
              makeRowVector(
                  {"number", "text"},
                  {makeFlatVector<int64_t>(
                       kRowsPerBatch,
                       [batchOffset](vector_size_t row) {
                         return static_cast<int64_t>(batchOffset + row);
                       },
                       [batchOffset](vector_size_t row) {
                         return (batchOffset + row) % 43 == 0;
                       }),
                   makeFlatVector<std::string>(
                       kRowsPerBatch,
                       [batchOffset](vector_size_t row) {
                         return fmt::format(
                             "row-value-{:05x}", (batchOffset + row) % 8'192);
                       },
                       [batchOffset](vector_size_t row) {
                         return (batchOffset + row) % 47 == 0;
                       })},
                  [batchOffset](vector_size_t row) {
                    return (batchOffset + row) % 53 == 0;
                  }),
          }));
    }
    return batches;
  }

  std::vector<Workload> makeWorkloads() const {
    const std::vector<std::string> allValues{
        "bigint_value",
        "double_value",
        "string_value",
        "boolean_value",
    };
    return {
        {"type/bigint_nullable",
         {"bigint_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/bigint_dictionary_nullable",
         {"dictionary_bigint_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/bigint_constant",
         {"constant_bigint_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/double_nullable",
         {"double_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/varchar_nullable",
         {"string_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/varchar_dictionary_nullable",
         {"dictionary_string_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/varchar_constant",
         {"constant_string_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/varchar_unique_nullable",
         {"unique_string_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/boolean_nullable",
         {"boolean_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/date_nullable",
         {"date_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/short_decimal_nullable",
         {"short_decimal_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/timestamp_nullable",
         {"timestamp_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/array_nullable",
         {"array_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/map_nullable",
         {"map_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"type/row_nullable",
         {"row_value"},
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"projection/4_value_columns",
         allValues,
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"filter/0_percent",
         {"bigint_value"},
         FilterRange{100, 100},
         kDefaultOutputBatchSize,
         0},
        {"filter/1_percent",
         {"bigint_value"},
         FilterRange{0, 0},
         kDefaultOutputBatchSize,
         (kNumRows + 99) / 100},
        {"filter/50_percent",
         {"bigint_value"},
         FilterRange{0, 49},
         kDefaultOutputBatchSize,
         (kNumRows / 100) * 50 + std::min<uint64_t>(kNumRows % 100, 50)},
        {"filter/100_percent",
         {"bigint_value"},
         FilterRange{0, 99},
         kDefaultOutputBatchSize,
         kNumRows},
        {"batch/1K", allValues, std::nullopt, 1'024, kNumRows},
        {"batch/16K",
         allValues,
         std::nullopt,
         kDefaultOutputBatchSize,
         kNumRows},
        {"batch/64K", allValues, std::nullopt, 65'536, kNumRows},
    };
  }

  std::string writeParquetBytes() {
    std::string fileBytes;
    auto sink = std::make_unique<common::WriteFileSink>(
        std::make_unique<InMemoryWriteFile>(&fileBytes), "benchmark.parquet");
    common::WriterOptions options;
    options.memoryPool = rootPool_.get();
    auto parquetOptions = std::make_shared<parquet::ParquetWriterOptions>();
    parquetOptions->parquetWriteTimestampUnit =
        TimestampPrecision::kNanoseconds;
    options.formatSpecificOptions = std::move(parquetOptions);
    {
      parquet::Writer writer(std::move(sink), options, inputType_);
      for (const auto& batch : batches_) {
        writer.write(batch);
      }
      writer.close();
    }
    return fileBytes;
  }

  std::string writeNimbleBytes() {
    const std::vector<VectorPtr> input{batches_.begin(), batches_.end()};
    return facebook::nimble::test::createNimbleFile(*pool(), input, {}, false);
  }

  std::shared_ptr<common::ScanSpec> makeScanSpec(
      const Workload& workload) const {
    auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
    for (column_index_t channel = 0; channel < workload.projectedColumns.size();
         ++channel) {
      const auto& name = workload.projectedColumns[channel];
      scanSpec->addFieldRecursively(
          name, *inputType_->findChild(name), channel);
    }
    if (workload.filterRange.has_value()) {
      auto* filterField = scanSpec->getOrCreateChild("filter_key");
      filterField->setFilter(
          std::make_shared<facebook::velox::common::BigintRange>(
              workload.filterRange->lower, workload.filterRange->upper, false));
      filterField->setProjectOut(false);
    }
    return scanSpec;
  }

  RowTypePtr makeTypeForColumns(const std::vector<std::string>& columns) const {
    std::vector<TypePtr> types;
    types.reserve(columns.size());
    for (const auto& column : columns) {
      types.push_back(inputType_->findChild(column));
    }
    return ROW(columns, std::move(types));
  }

  ScanResult scan(
      const FormatData& format,
      const Workload& workload,
      bool computeDigest) const {
    auto scanSpec = makeScanSpec(workload);
    common::ReaderOptions readerOptions{pool()};
    auto readFile =
        std::make_shared<InMemoryReadFile>(std::string_view{format.fileBytes});
    auto reader =
        common::getReaderFactory(format.format)
            ->createReader(
                std::make_unique<common::BufferedInput>(readFile, *pool()),
                readerOptions);
    if (computeDigest) {
      VELOX_CHECK_EQ(reader->numberOfRows().value_or(0), kNumRows);
    }

    common::RowReaderOptions rowReaderOptions;
    rowReaderOptions.setTimestampPrecision(TimestampPrecision::kNanoseconds);
    auto selectedColumns = workload.projectedColumns;
    if (workload.filterRange.has_value()) {
      selectedColumns.push_back("filter_key");
    }
    rowReaderOptions.select(
        std::make_shared<common::ColumnSelector>(inputType_, selectedColumns));
    rowReaderOptions.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOptions);

    ScanResult scanResult;
    VectorPtr output = BaseVector::create(
        makeTypeForColumns(workload.projectedColumns), 1, pool());
    while (true) {
      const auto scannedRows =
          rowReader->next(workload.outputBatchSize, output);
      if (scannedRows == 0) {
        break;
      }
      scanResult.scannedRows += scannedRows;
      if (output == nullptr || output->size() == 0) {
        continue;
      }

      auto* rowVector = output->asChecked<RowVector>();
      for (column_index_t column = 0; column < rowVector->childrenSize();
           ++column) {
        auto loaded =
            BaseVector::loadedVectorShared(rowVector->childAt(column));
        folly::doNotOptimizeAway(loaded.get());
        if (computeDigest) {
          for (vector_size_t row = 0; row < loaded->size(); ++row) {
            scanResult.nullValues += loaded->isNullAt(row);
          }
        }
      }
      if (computeDigest) {
        for (vector_size_t row = 0; row < rowVector->size(); ++row) {
          scanResult.digest =
              bits::hashMix(scanResult.digest, rowVector->hashValueAt(row));
        }
      }
      scanResult.outputRows += rowVector->size();
      ++scanResult.outputBatches;
    }
    return scanResult;
  }

  void verifyPreflight() const {
    VELOX_CHECK(!formats_.empty());
    for (const auto& workload : workloads_) {
      const auto reference = scan(formats_.front(), workload, true);
      VELOX_CHECK_EQ(reference.outputRows, workload.expectedOutputRows);
      for (size_t formatIndex = 1; formatIndex < formats_.size();
           ++formatIndex) {
        const auto& format = formats_[formatIndex];
        const auto result = scan(format, workload, true);
        VELOX_CHECK(
            reference.sameValues(result),
            "{} and Parquet differ for workload {}: Parquet rows {}, {} rows {}",
            format.name,
            workload.name,
            reference.outputRows,
            format.name,
            result.outputRows);
      }
    }
  }

  RowTypePtr inputType_;
  std::vector<RowVectorPtr> batches_;
  std::vector<FormatData> formats_;
  std::vector<Workload> workloads_;
};

} // namespace
} // namespace facebook::velox::dwio::vortex

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});
  facebook::velox::dwio::vortex::VortexRowReaderBenchmark benchmark;
  benchmark.addBenchmarks();
  folly::runBenchmarks();
  return 0;
}
