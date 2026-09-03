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

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "velox/dwio/common/Reader.h"
#include "velox/dwio/vortex/VortexArray.h"
#include "velox/dwio/vortex/VortexFile.h"
#include "velox/dwio/vortex/VortexSplitMapper.h"
#include "velox/dwio/vortex/VortexVector.h"

struct vx_velox_data_source;
struct vx_velox_partition;
struct vx_velox_scan;

namespace facebook::velox::dwio::vortex {

/// Reads metadata and creates row readers for one Vortex file.
class VortexReader : public common::Reader {
 public:
  /// Opens a Vortex file from a DWIO buffered input.
  VortexReader(
      std::unique_ptr<common::BufferedInput> input,
      const common::ReaderOptions& options);

  /// Returns the exact number of rows in the file.
  std::optional<uint64_t> numberOfRows() const override;

  /// Returns no column statistics until the adapter exposes them.
  std::unique_ptr<common::ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  /// Returns the logical row type in the file.
  const RowTypePtr& rowType() const override;

  /// Returns the logical row type with stable field identifiers.
  const std::shared_ptr<const common::TypeWithId>& typeWithId() const override;

  /// Creates a row reader for the supplied DWIO options.
  std::unique_ptr<common::RowReader> createRowReader(
      const common::RowReaderOptions& options) const override;

 private:
  // Points to the pool that owns reader metadata and output vectors.
  memory::MemoryPool* pool_;

  // Shares the open file across its row readers.
  std::shared_ptr<VortexFile> file_;

  // Stores the logical row type in the file.
  RowTypePtr rowType_;

  // Stores stable field identifiers for the logical row type.
  std::shared_ptr<const common::TypeWithId> typeWithId_;

  // Maps renamed row fields by ordinal position when requested.
  bool mapRowFieldsByPosition_{false};
};

/// Reads a byte-range-owned sequence of rows from one Vortex file.
class VortexRowReader : public common::RowReader {
 public:
  /// Creates a row reader for the supplied DWIO options.
  VortexRowReader(
      std::shared_ptr<VortexFile> file,
      RowTypePtr logicalRowType,
      bool mapRowFieldsByPosition,
      memory::MemoryPool& pool,
      const common::RowReaderOptions& options);

  ~VortexRowReader() override;

  /// Reads and filters up to the requested number of source rows.
  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const common::Mutation* mutation = nullptr) override;

  /// Returns the absolute row number for the next source row.
  int64_t nextRowNumber() override;

  /// Returns the exact source-row count for the next read.
  int64_t nextReadSize(uint64_t size) override;

  /// Reports natural splits that metadata retained or excluded.
  void updateRuntimeStats(common::RuntimeStats& stats) const override;

  /// Leaves filter state unchanged because the reader uses the live scan spec.
  void resetFilterCaches() override;

  /// Returns the flat row width for projected fixed-width fields.
  std::optional<size_t> estimatedRowSize() const override;

 private:
  // Retains one natural Vortex array and its prepared field exporters.
  struct PendingBatch {
    VortexArray values;
    VortexRowPositions rowPositions;
    std::vector<std::optional<VortexArray>> fields;
    std::vector<std::shared_ptr<VortexExportCursor>> exporters;
  };

  // Identifies one Velox output window inside a retained Vortex array.
  struct InputBatch {
    std::shared_ptr<PendingBatch> pending;
    size_t begin{0};
    size_t end{0};
  };

  // Starts a filtered scan at the next unread absolute source row.
  void startScan();

  // Computes natural split exclusions from the metadata filter.
  void initializeMetadataPruning();

  // Returns the retained range that contains the current row.
  const VortexRowRange* activeRetainedRange() const;

  // Advances the retained-range cursor past completed ranges.
  void advanceRetainedRange();

  // Releases the active scan and any buffered filtered rows.
  void closeScan();

  // Reads one source-row window without crossing a Vortex batch boundary.
  InputBatch readInputBatch(vector_size_t size);

  // Applies eager filters and installs lazy vectors for deferred fields.
  common::RowReader::ProjectColumnsResult projectBatch(
      const InputBatch& batch,
      const common::Mutation* mutation,
      uint64_t firstRow,
      vector_size_t rowsToRead) const;

  // Inserts absolute row numbers after deletion and filter selection.
  void addRowNumber(
      VectorPtr& result,
      const BufferPtr& selectedRows,
      const InputBatch& batch) const;

  // Returns the next non-empty array from the Vortex scan.
  std::optional<VortexArray> nextVortexBatch();

  // Loads row indexes for the current pending batch when needed.
  void preparePendingBatch();

  // Caps one read to the remaining row range and Velox vector limits.
  vector_size_t rowsForNextRead(uint64_t size);

  // Confirms that the Vortex scan produced exactly its assigned row range.
  void verifyScanEnded();

  // Shares file metadata and callback input ownership with the parent reader.
  std::shared_ptr<VortexFile> file_;

  // Stores the physical row type returned by Vortex.
  RowTypePtr rowType_;

  // Stores the logical file schema after reader-level name mapping.
  RowTypePtr logicalRowType_;

  // Maps nested row fields by ordinal position during schema adaptation.
  bool mapRowFieldsByPosition_{false};

  // Points to the pool that owns imported and adapted vectors.
  memory::MemoryPool* pool_;

  // Stores the logical schema requested by the file connector.
  RowTypePtr requestedType_;

  // Stores the standard DWIO filters, projections, and mutations.
  std::shared_ptr<velox::common::ScanSpec> scanSpec_;

  // Stores the Boolean expression that combines metadata-filter leaves.
  std::shared_ptr<velox::common::MetadataFilter> metadataFilter_;

  // Stores the optional synthesized row-number column contract.
  std::optional<common::RowNumberColumnInfo> rowNumberColumnInfo_;

  // Maps physical file channels to projected Vortex scan channels.
  std::vector<std::optional<column_index_t>> scanChannelsBySource_;

  // Identifies exact filters that Vortex evaluates before batch import.
  std::vector<velox::common::ScanSpec*> pushedFilters_;

  // Stores the natural row range owned by this byte split.
  VortexRowRange rowRange_;

  // Stores merged natural split ranges that metadata did not exclude.
  std::vector<VortexRowRange> retainedRanges_;

  // Identifies the next retained range that can contain unread rows.
  size_t retainedRangeIndex_{0};

  // Tracks the next absolute source row in the owned range.
  uint64_t currentRow_{0};

  // Owns the Vortex data source for this row reader.
  const vx_velox_data_source* dataSource_{nullptr};

  // Owns the active Vortex scan.
  vx_velox_scan* scan_{nullptr};

  // Owns the active scan partition.
  vx_velox_partition* partition_{nullptr};

  // Records whether the owned row range finished.
  bool exhausted_{false};

  // Records whether the current Vortex scan reached its end.
  bool scanFinished_{false};

  // Records whether scan batches contain explicit absolute row indexes.
  bool scanIncludesRowIndex_{true};

  // Tracks the first row of the next unfiltered Vortex batch.
  uint64_t nextScanRow_{0};

  // Retains the current Vortex batch across read calls and lazy loads.
  std::shared_ptr<PendingBatch> pendingBatch_;

  // Tracks the first unread row in the retained batch.
  size_t pendingOffset_{0};

  // Counts natural splits excluded through metadata statistics.
  int64_t skippedStrides_{0};

  // Counts natural splits retained after metadata evaluation.
  int64_t processedStrides_{0};
};

} // namespace facebook::velox::dwio::vortex
