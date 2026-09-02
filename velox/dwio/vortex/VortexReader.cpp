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

#include <algorithm>
#include <limits>
#include <numeric>
#include <string>

#include <folly/String.h>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/dwio/vortex/VortexFilter.h"
#include "velox/dwio/vortex/VortexVector.h"

namespace facebook::velox::dwio::vortex {
namespace {

constexpr std::string_view kVortexRowIndexField{"$velox_vortex_row_index"};

std::string errorMessage(const vx_error* error) {
  if (error == nullptr) {
    return "Vortex returned an unspecified error";
  }
  const auto message = vx_velox_error_message(error);
  return std::string{message.ptr, message.len};
}

[[noreturn]] void failVortex(std::string_view operation, vx_error* error) {
  const auto errorText = errorMessage(error);
  vx_velox_error_free(error);
  VELOX_USER_FAIL("Failed to {}: {}", operation, errorText);
}

std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
    const RowTypePtr& physicalRowType,
    const RowTypePtr& logicalRowType,
    const RowTypePtr& requestedType,
    const std::shared_ptr<common::ColumnSelector>& selector,
    bool mapRowFieldsByPosition) {
  auto scanSpec = std::make_shared<velox::common::ScanSpec>("<root>");
  for (column_index_t i = 0; i < requestedType->size(); ++i) {
    const auto& name = requestedType->nameOf(i);
    const auto logicalChannel = logicalRowType->getChildIdxIfExists(name);
    std::optional<column_index_t> physicalChannel;
    if (mapRowFieldsByPosition && logicalChannel.has_value() &&
        logicalChannel.value() < physicalRowType->size()) {
      physicalChannel = logicalChannel;
    } else {
      physicalChannel = physicalRowType->getChildIdxIfExists(name);
    }
    auto* child = scanSpec->addFieldRecursively(
        name,
        *requestedType->childAt(i),
        physicalChannel.has_value() ? physicalChannel.value() : -1);
    if (physicalChannel.has_value()) {
      child->setSubscript(physicalChannel.value());
    }
    child->setChannel(i);
    if (selector != nullptr && logicalChannel.has_value() &&
        !selector->shouldReadColumn(logicalChannel.value())) {
      child->setColumnType(common::ScanSpec::ColumnType::kComposite);
    }
  }
  return scanSpec;
}

TypePtr lowerCaseRowFieldNames(const TypePtr& type) {
  if (type->isRow()) {
    const auto& row = type->asRow();
    auto names = row.names();
    std::vector<TypePtr> children;
    children.reserve(row.size());
    for (column_index_t i = 0; i < row.size(); ++i) {
      folly::toLowerAscii(names[i]);
      children.push_back(lowerCaseRowFieldNames(row.childAt(i)));
    }
    return ROW(std::move(names), std::move(children));
  }
  if (type->isArray()) {
    return ARRAY(lowerCaseRowFieldNames(type->childAt(0)));
  }
  if (type->isMap()) {
    return MAP(
        lowerCaseRowFieldNames(type->childAt(0)),
        lowerCaseRowFieldNames(type->childAt(1)));
  }
  return type;
}

RowTypePtr logicalFileType(
    const RowTypePtr& physicalRowType,
    const common::ReaderOptions& options) {
  auto fileType = options.fileColumnNamesReadAsLowerCase()
      ? asRowType(lowerCaseRowFieldNames(physicalRowType))
      : physicalRowType;
  VELOX_USER_CHECK_NE(
      static_cast<int32_t>(options.columnMappingMode()),
      static_cast<int32_t>(common::ColumnMappingMode::kFieldId),
      "Vortex files do not expose field IDs for schema mapping");
  if (options.columnMappingMode() == common::ColumnMappingMode::kPosition &&
      options.fileSchema() != nullptr) {
    return asRowType(
        common::Reader::updateColumnNames(fileType, options.fileSchema()));
  }
  return fileType;
}

void validateDeltaUpdates(const common::ScanSpec& scanSpec, bool topLevel) {
  for (const auto& child : scanSpec.children()) {
    VELOX_USER_CHECK(
        topLevel || child->deltaUpdate() == nullptr,
        "Vortex does not support nested delta updates: {}",
        child->fieldName());
    validateDeltaUpdates(*child, false);
  }
}

class PushedFilterGuard {
 public:
  PushedFilterGuard(
      common::ScanSpec& root,
      const std::vector<common::ScanSpec*>& pushedFilters)
      : root_{root} {
    filters_.reserve(pushedFilters.size());
    for (auto* scanSpec : pushedFilters) {
      if (scanSpec->filter() == nullptr) {
        continue;
      }
      filters_.emplace_back(scanSpec, scanSpec->filter()->clone());
      scanSpec->setFilter(nullptr);
    }
    root_.resetCachedValues(false);
  }

  ~PushedFilterGuard() {
    for (auto& [scanSpec, filter] : filters_) {
      scanSpec->setFilter(std::move(filter));
    }
    root_.resetCachedValues(false);
  }

 private:
  common::ScanSpec& root_;
  std::vector<std::pair<
      common::ScanSpec*,
      std::shared_ptr<facebook::velox::common::Filter>>>
      filters_;
};

std::vector<VortexRowRange> naturalRowRanges(const VortexFile& file) {
  std::vector<VortexRowRange> ranges;
  ranges.reserve(file.naturalSplits().size());
  for (const auto& [begin, end] : file.naturalSplits()) {
    ranges.push_back({begin, end});
  }
  return ranges;
}

std::optional<size_t> fixedWidthTypeSize(const TypePtr& type) {
  if (type->isFixedWidth()) {
    return type->cppSizeInBytes();
  }
  if (!type->isRow()) {
    return std::nullopt;
  }

  size_t size{0};
  for (const auto& child : type->asRow().children()) {
    const auto childSize = fixedWidthTypeSize(child);
    if (!childSize.has_value()) {
      return std::nullopt;
    }
    size += childSize.value();
  }
  return std::max<size_t>(size, 1);
}

std::optional<size_t> fixedWidthProjectedRowSize(
    const RowType& requestedType,
    const velox::common::ScanSpec& scanSpec) {
  size_t size{0};
  for (const auto& child : scanSpec.children()) {
    if (!child->projectOut()) {
      continue;
    }
    VELOX_CHECK_NE(child->channel(), velox::common::ScanSpec::kNoChannel);
    TypePtr outputType;
    if (child->hasTransform()) {
      outputType = child->transformOutputType();
    } else if (
        const auto requestedChannel =
            requestedType.getChildIdxIfExists(child->fieldName())) {
      outputType = requestedType.childAt(requestedChannel.value());
    } else if (child->isConstant()) {
      outputType = child->constantValue()->type();
    } else {
      return std::nullopt;
    }
    const auto childSize = fixedWidthTypeSize(outputType);
    if (!childSize.has_value()) {
      return std::nullopt;
    }
    size += childSize.value();
  }
  return std::max<size_t>(size, 1);
}

VortexRowRange ownedRowRange(
    const VortexFile& file,
    const common::RowReaderOptions& options) {
  if (file.rowCount() == 0 || options.offset() >= file.fileSize()) {
    return {};
  }
  const auto byteRangeEnd = std::min(options.limit(), file.fileSize());
  if (options.offset() == 0 && byteRangeEnd == file.fileSize()) {
    return {0, file.rowCount()};
  }
  const auto mapped = VortexSplitMapper::map(
      file.rowCount(),
      file.fileSize(),
      naturalRowRanges(file),
      options.offset(),
      byteRangeEnd - options.offset());
  return mapped.value_or(VortexRowRange{});
}

std::optional<column_index_t> sourceChannelFor(
    const RowTypePtr& rowType,
    const velox::common::ScanSpec* childSpec,
    std::string_view fieldName) {
  if (childSpec != nullptr && childSpec->subscript() >= 0) {
    if (childSpec->subscript() >= rowType->size()) {
      return std::nullopt;
    }
    return static_cast<column_index_t>(childSpec->subscript());
  }
  return rowType->getChildIdxIfExists(fieldName);
}

std::unique_ptr<vx_expression, decltype(&vx_velox_expression_free)>
scanProjection(
    const RowTypePtr& rowType,
    const std::shared_ptr<velox::common::ScanSpec>& scanSpec,
    std::vector<std::optional<column_index_t>>& scanChannelsBySource) {
  scanChannelsBySource.assign(rowType->size(), std::nullopt);
  std::vector<bool> included(rowType->size(), false);

  for (const auto& childSpec : scanSpec->children()) {
    if (!childSpec->readFromFile() ||
        (!childSpec->projectOut() && !childSpec->hasFilter() &&
         childSpec->deltaUpdate() == nullptr && !childSpec->hasTransform())) {
      continue;
    }
    const auto channel =
        sourceChannelFor(rowType, childSpec.get(), childSpec->fieldName());
    if (channel.has_value()) {
      included[channel.value()] = true;
    }
  }
  std::vector<vx_view> fieldNames;
  fieldNames.reserve(rowType->size());
  for (column_index_t sourceChannel = 0; sourceChannel < rowType->size();
       ++sourceChannel) {
    if (!included[sourceChannel]) {
      continue;
    }
    const auto& name = rowType->nameOf(sourceChannel);
    scanChannelsBySource[sourceChannel] = fieldNames.size() + 1;
    fieldNames.push_back(vx_view{name.data(), name.size()});
  }

  const vx_view rowIndexName{
      kVortexRowIndexField.data(), kVortexRowIndexField.size()};
  vx_error* error{nullptr};
  std::unique_ptr<vx_expression, decltype(&vx_velox_expression_free)>
      projection{
          vx_velox_expression_select_with_row_index(
              fieldNames.empty() ? nullptr : fieldNames.data(),
              fieldNames.size(),
              rowIndexName,
              &error),
          vx_velox_expression_free};
  if (error != nullptr) {
    failVortex("create the Vortex scan projection", error);
  }
  VELOX_USER_CHECK_NOT_NULL(
      projection, "Failed to create a Vortex projection expression");
  return projection;
}

void scatterLazyRows(RowSet rows, vector_size_t resultSize, VectorPtr& result) {
  if (rows.size() == resultSize) {
    return;
  }
  auto indices =
      AlignedBuffer::allocate<vector_size_t>(resultSize, result->pool(), 0);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t index = 0; index < rows.size(); ++index) {
    rawIndices[rows[index]] = index;
  }
  result->disableMemo();
  result = BaseVector::wrapInDictionary(
      nullptr, std::move(indices), resultSize, std::move(result));
}

VectorPtr preserveParentNullsForFiltering(
    VectorPtr input,
    const velox::common::ScanSpec* scanSpec) {
  if (scanSpec == nullptr || !scanSpec->hasFilter() ||
      input->encoding() != VectorEncoding::Simple::ROW ||
      input->rawNulls() == nullptr) {
    return input;
  }
  auto indices =
      AlignedBuffer::allocate<vector_size_t>(input->size(), input->pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  std::iota(rawIndices, rawIndices + input->size(), 0);
  return BaseVector::wrapInDictionary(
      input->nulls(), std::move(indices), input->size(), std::move(input));
}

bool supportsValueHook(TypeKind kind) {
  return kind != TypeKind::TIMESTAMP && kind != TypeKind::ARRAY &&
      kind != TypeKind::ROW && kind != TypeKind::MAP &&
      kind != TypeKind::HUGEINT && kind != TypeKind::UNKNOWN;
}

template <TypeKind Kind>
void addDecodedValuesToHook(DecodedVector& decoded, ValueHook& hook) {
  if constexpr (Kind == TypeKind::TIMESTAMP) {
    VELOX_UNREACHABLE();
  } else {
    using NativeType = typename TypeTraits<Kind>::NativeType;
    for (vector_size_t row = 0; row < decoded.size(); ++row) {
      if (decoded.isNullAt(row)) {
        if (hook.acceptsNulls()) {
          hook.addNull(row);
        }
      } else {
        hook.addValueTyped(row, decoded.valueAt<NativeType>(row));
      }
    }
  }
}

void loadVectorValueHook(const VectorPtr& values, ValueHook& hook) {
  DecodedVector decoded{*values};
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      addDecodedValuesToHook, values->typeKind(), decoded, hook);
}

VectorPtr applyNestedConstants(
    VectorPtr input,
    const velox::common::ScanSpec* scanSpec) {
  if (scanSpec == nullptr || !input->type()->isRow()) {
    return input;
  }
  if (input->encoding() != VectorEncoding::Simple::ROW) {
    input = BaseVector::copy(*input);
  }
  auto* row = input->asChecked<RowVector>();
  auto children = row->children();
  const auto& rowType = input->type()->asRow();
  for (column_index_t channel = 0; channel < rowType.size(); ++channel) {
    const auto* childSpec = scanSpec->childByName(rowType.nameOf(channel));
    if (childSpec == nullptr) {
      continue;
    }
    if (childSpec->isConstant()) {
      children[channel] = BaseVector::wrapInConstant(
          input->size(), 0, childSpec->constantValue());
    } else {
      children[channel] =
          applyNestedConstants(std::move(children[channel]), childSpec);
    }
  }
  return std::make_shared<RowVector>(
      input->pool(),
      input->type(),
      input->nulls(),
      input->size(),
      std::move(children),
      input->getNullCount());
}

class VortexLazyLoader final : public VectorLoader {
 public:
  VortexLazyLoader(
      std::shared_ptr<VortexFile> file,
      VortexArray field,
      TypePtr sourceType,
      TypePtr targetType,
      bool mapRowFieldsByPosition,
      std::shared_ptr<velox::common::ScanSpec> rootScanSpec,
      std::string fieldName,
      BufferPtr selectedRows,
      memory::MemoryPool& pool)
      : file_{std::move(file)},
        field_{std::move(field)},
        sourceType_{std::move(sourceType)},
        targetType_{std::move(targetType)},
        mapRowFieldsByPosition_{mapRowFieldsByPosition},
        rootScanSpec_{std::move(rootScanSpec)},
        fieldName_{std::move(fieldName)},
        selectedRows_{std::move(selectedRows)},
        pool_{pool} {}

  bool supportsHook() const override {
    return sourceType_->equivalent(*targetType_) &&
        supportsValueHook(sourceType_->kind());
  }

 private:
  void loadInternal(
      RowSet rows,
      ValueHook* hook,
      vector_size_t resultSize,
      VectorPtr* result) override {
    std::vector<vector_size_t> sourceRows;
    RowSet effectiveRows = rows;
    if (selectedRows_ != nullptr) {
      const auto* selection = selectedRows_->as<vector_size_t>();
      sourceRows.reserve(rows.size());
      for (const auto row : rows) {
        sourceRows.push_back(selection[row]);
      }
      effectiveRows = sourceRows;
    }
    if (hook != nullptr) {
      VELOX_CHECK(supportsHook());
      if (supportsNativeVortexType(sourceType_)) {
        loadVortexValueHook(
            file_->session(),
            field_,
            sourceType_,
            effectiveRows,
            {},
            *hook,
            pool_);
      } else {
        auto loaded = importVortexVector(
            file_->session(), field_, sourceType_, effectiveRows, pool_);
        loadVectorValueHook(loaded, *hook);
      }
      return;
    }

    auto loaded = importVortexVector(
        file_->session(), field_, sourceType_, effectiveRows, pool_);
    loaded =
        adaptVortexVectorType(loaded, targetType_, mapRowFieldsByPosition_);
    loaded = applyNestedConstants(
        std::move(loaded), rootScanSpec_->childByName(fieldName_));
    scatterLazyRows(rows, resultSize, loaded);
    *result = std::move(loaded);
  }

  std::shared_ptr<VortexFile> file_;
  VortexArray field_;
  TypePtr sourceType_;
  TypePtr targetType_;
  bool mapRowFieldsByPosition_;
  std::shared_ptr<velox::common::ScanSpec> rootScanSpec_;
  std::string fieldName_;
  BufferPtr selectedRows_;
  memory::MemoryPool& pool_;
};

} // namespace

VortexReader::VortexReader(
    std::unique_ptr<common::BufferedInput> input,
    const common::ReaderOptions& options)
    : pool_{&options.memoryPool()},
      file_{std::make_shared<VortexFile>(std::move(input), *pool_)},
      rowType_{logicalFileType(file_->rowType(), options)},
      typeWithId_{common::TypeWithId::create(rowType_)},
      mapRowFieldsByPosition_{
          options.columnMappingMode() == common::ColumnMappingMode::kPosition &&
          options.fileSchema() != nullptr} {}

std::optional<uint64_t> VortexReader::numberOfRows() const {
  return file_->rowCount();
}

std::unique_ptr<common::ColumnStatistics> VortexReader::columnStatistics(
    uint32_t /*index*/) const {
  return nullptr;
}

const RowTypePtr& VortexReader::rowType() const {
  return rowType_;
}

const std::shared_ptr<const common::TypeWithId>& VortexReader::typeWithId()
    const {
  return typeWithId_;
}

std::unique_ptr<common::RowReader> VortexReader::createRowReader(
    const common::RowReaderOptions& options) const {
  return std::make_unique<VortexRowReader>(
      file_, rowType_, mapRowFieldsByPosition_, *pool_, options);
}

VortexRowReader::VortexRowReader(
    std::shared_ptr<VortexFile> file,
    RowTypePtr logicalRowType,
    bool mapRowFieldsByPosition,
    memory::MemoryPool& pool,
    const common::RowReaderOptions& options)
    : file_{std::move(file)},
      rowType_{file_->rowType()},
      logicalRowType_{std::move(logicalRowType)},
      mapRowFieldsByPosition_{mapRowFieldsByPosition},
      pool_{&pool},
      requestedType_{
          options.selector() != nullptr && options.projectSelectedType()
              ? options.selector()->buildSelectedReordered()
              : options.requestedType() ? options.requestedType()
                                        : logicalRowType_},
      scanSpec_{
          options.scanSpec() ? options.scanSpec()
                             : makeScanSpec(
                                   rowType_,
                                   logicalRowType_,
                                   requestedType_,
                                   options.selector(),
                                   mapRowFieldsByPosition_)},
      metadataFilter_{options.metadataFilter()},
      rowNumberColumnInfo_{options.rowNumberColumnInfo()},
      rowRange_{ownedRowRange(*file_, options)},
      currentRow_{rowRange_.begin},
      exhausted_{rowRange_.begin == rowRange_.end} {
  validateDeltaUpdates(*scanSpec_, true);
  VELOX_USER_CHECK_LE(
      file_->rowCount(),
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
      "A Vortex file exceeds the supported row count: {}",
      file_->rowCount());
  if (exhausted_) {
    return;
  }

  initializeMetadataPruning();
  if (retainedRanges_.empty()) {
    currentRow_ = rowRange_.end;
    exhausted_ = true;
    return;
  }
  dataSource_ = file_->createDataSource();
  startScan();
}

VortexRowReader::~VortexRowReader() {
  closeScan();
  if (dataSource_ != nullptr) {
    vx_velox_data_source_free(dataSource_);
  }
}

void VortexRowReader::startScan() {
  closeScan();
  if (currentRow_ == rowRange_.end) {
    scanFinished_ = true;
    return;
  }

  advanceRetainedRange();
  const auto* retainedRange = activeRetainedRange();
  if (retainedRange == nullptr) {
    return;
  }

  auto filter = convertVortexFilter(*scanSpec_, *rowType_);
  pushedFilters_.clear();
  pushedFilters_.reserve(filter.pushedFilters.size());
  for (const auto* pushedFilter : filter.pushedFilters) {
    pushedFilters_.push_back(const_cast<common::ScanSpec*>(pushedFilter));
  }
  PushedFilterGuard pushedFilterGuard{*scanSpec_, pushedFilters_};
  auto projection = scanProjection(rowType_, scanSpec_, scanChannelsBySource_);
  const vx_velox_scan_options scanOptions{
      .struct_size = sizeof(vx_velox_scan_options),
      .abi_version = VX_VELOX_ABI_VERSION,
      .projection = projection.get(),
      .filter = filter.expression.get(),
      .row_range_begin = currentRow_,
      .row_range_end = retainedRange->end,
      .selection =
          {
              .indices = nullptr,
              .length = 0,
              .include = VX_VELOX_SELECTION_ALL,
          },
      .limit = 0,
      .ordered = true,
  };
  vx_error* error{nullptr};
  scan_ = vx_velox_data_source_scan(dataSource_, &scanOptions, &error);
  if (scan_ == nullptr) {
    failVortex("create the Vortex scan", error);
  }
  scanFinished_ = false;
}

void VortexRowReader::initializeMetadataPruning() {
  retainedRanges_.clear();
  retainedRangeIndex_ = 0;
  skippedStrides_ = 0;
  processedStrides_ = 0;
  if (rowRange_.begin == rowRange_.end) {
    return;
  }

  const auto& naturalSplits = file_->naturalSplits();
  std::optional<size_t> firstOwnedSplit;
  size_t ownedSplitCount{0};
  for (size_t i = 0; i < naturalSplits.size(); ++i) {
    const auto& [begin, end] = naturalSplits[i];
    if (end <= rowRange_.begin || begin >= rowRange_.end) {
      continue;
    }
    VELOX_CHECK_GE(begin, rowRange_.begin);
    VELOX_CHECK_LE(end, rowRange_.end);
    if (!firstOwnedSplit.has_value()) {
      firstOwnedSplit = i;
    }
    ++ownedSplitCount;
  }
  if (!firstOwnedSplit.has_value()) {
    retainedRanges_.push_back(rowRange_);
    return;
  }
  for (size_t i = 0; i < ownedSplitCount; ++i) {
    const auto& [begin, end] = naturalSplits[firstOwnedSplit.value() + i];
    VELOX_CHECK_GT(end, rowRange_.begin);
    VELOX_CHECK_LT(begin, rowRange_.end);
  }

  std::vector<uint64_t> excluded(bits::nwords(ownedSplitCount), 0);
  if (metadataFilter_ != nullptr) {
    auto conversions = convertVortexMetadataFilters(*scanSpec_, *rowType_);
    std::vector<std::pair<
        const velox::common::MetadataFilter::LeafNode*,
        std::vector<uint64_t>>>
        leafResults;
    leafResults.reserve(conversions.size());
    for (const auto& conversion : conversions) {
      const auto decisions = file_->pruneNaturalSplits(
          conversion.expression.get(),
          firstOwnedSplit.value(),
          ownedSplitCount);
      std::vector<uint64_t> result(bits::nwords(ownedSplitCount), 0);
      for (size_t i = 0; i < decisions.size(); ++i) {
        VELOX_USER_CHECK_LE(
            decisions[i],
            1,
            "Vortex returned an invalid prune decision: {}",
            decisions[i]);
        if (decisions[i] != 0) {
          bits::setBit(result.data(), i);
        }
      }
      leafResults.emplace_back(conversion.leaf, std::move(result));
    }
    metadataFilter_->eval(leafResults, excluded);
  }

  for (size_t relativeSplit = 0; relativeSplit < ownedSplitCount;
       ++relativeSplit) {
    const auto& [begin, end] =
        naturalSplits[firstOwnedSplit.value() + relativeSplit];
    if (bits::isBitSet(excluded.data(), relativeSplit)) {
      ++skippedStrides_;
      continue;
    }
    ++processedStrides_;
    if (!retainedRanges_.empty() && retainedRanges_.back().end == begin) {
      retainedRanges_.back().end = end;
    } else {
      retainedRanges_.push_back({begin, end});
    }
  }
}

const VortexRowRange* VortexRowReader::activeRetainedRange() const {
  if (retainedRangeIndex_ >= retainedRanges_.size()) {
    return nullptr;
  }
  const auto& range = retainedRanges_[retainedRangeIndex_];
  if (currentRow_ < range.begin || currentRow_ >= range.end) {
    return nullptr;
  }
  return &range;
}

void VortexRowReader::advanceRetainedRange() {
  while (retainedRangeIndex_ < retainedRanges_.size() &&
         currentRow_ >= retainedRanges_[retainedRangeIndex_].end) {
    ++retainedRangeIndex_;
  }
}

void VortexRowReader::closeScan() {
  pendingBatch_.reset();
  pendingRowIndices_.clear();
  pendingOffset_ = 0;
  if (partition_ != nullptr) {
    vx_velox_partition_free(partition_);
    partition_ = nullptr;
  }
  if (scan_ != nullptr) {
    vx_velox_scan_free(scan_);
    scan_ = nullptr;
  }
  scanFinished_ = true;
}

uint64_t VortexRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const common::Mutation* mutation) {
  if (exhausted_) {
    return 0;
  }
  VELOX_USER_CHECK_GT(size, 0, "A Vortex read size must be greater than zero");

  const auto rowsToRead = rowsForNextRead(size);
  const auto firstRow = currentRow_;
  const auto* retainedRange = activeRetainedRange();
  auto input = readInputBatch(rowsToRead);
  currentRow_ += rowsToRead;
  auto projected = projectBatch(input, mutation, firstRow, rowsToRead);
  result = std::move(projected.output);
  addRowNumber(result, projected.selectedRows, input.rowIndices);
  if (retainedRange != nullptr && currentRow_ == retainedRange->end) {
    verifyScanEnded();
    closeScan();
    advanceRetainedRange();
  }
  if (currentRow_ == rowRange_.end) {
    if (scan_ != nullptr) {
      verifyScanEnded();
      closeScan();
    }
    exhausted_ = true;
  }
  return rowsToRead;
}

int64_t VortexRowReader::nextRowNumber() {
  return exhausted_ ? kAtEnd : static_cast<int64_t>(currentRow_);
}

int64_t VortexRowReader::nextReadSize(uint64_t size) {
  if (exhausted_) {
    return kAtEnd;
  }
  VELOX_USER_CHECK_GT(size, 0, "A Vortex read size must be greater than zero");
  return rowsForNextRead(size);
}

void VortexRowReader::updateRuntimeStats(common::RuntimeStats& stats) const {
  stats.skippedStrides += skippedStrides_;
  stats.processedStrides += processedStrides_;
}

void VortexRowReader::resetFilterCaches() {
  scanSpec_->resetCachedValues(false);
  if (!exhausted_) {
    startScan();
  }
}

std::optional<size_t> VortexRowReader::estimatedRowSize() const {
  return fixedWidthProjectedRowSize(*requestedType_, *scanSpec_);
}

VortexRowReader::InputBatch VortexRowReader::readInputBatch(
    vector_size_t size) {
  VELOX_CHECK_GT(size, 0);
  InputBatch result;
  if (!pendingBatch_.has_value()) {
    return result;
  }
  VELOX_CHECK_LT(pendingOffset_, pendingBatch_->size());
  VELOX_CHECK_EQ(pendingBatch_->size(), pendingRowIndices_.size());
  const auto windowEnd = currentRow_ + size;
  VELOX_CHECK_GE(pendingRowIndices_[pendingOffset_], currentRow_);
  if (pendingRowIndices_[pendingOffset_] >= windowEnd) {
    return result;
  }

  const auto begin = pendingRowIndices_.begin() + pendingOffset_;
  const auto end = std::lower_bound(begin, pendingRowIndices_.end(), windowEnd);
  const auto endOffset = static_cast<size_t>(end - pendingRowIndices_.begin());
  result.values = pendingBatch_->slice(pendingOffset_, endOffset);
  result.rowIndices.assign(begin, end);
  pendingOffset_ = endOffset;
  if (pendingOffset_ == pendingBatch_->size()) {
    pendingBatch_.reset();
    pendingRowIndices_.clear();
    pendingOffset_ = 0;
  }
  return result;
}

common::RowReader::ProjectColumnsResult VortexRowReader::projectBatch(
    const InputBatch& batch,
    const common::Mutation* mutation,
    uint64_t firstRow,
    vector_size_t rowsToRead) const {
  PushedFilterGuard pushedFilterGuard{*scanSpec_, pushedFilters_};
  struct DeferredField {
    column_index_t channel;
    std::string fieldName;
    VortexArray field;
    TypePtr sourceType;
    TypePtr targetType;
  };

  std::vector<VectorPtr> children;
  children.reserve(requestedType_->size());
  std::vector<DeferredField> deferredFields;
  const auto batchSize = static_cast<vector_size_t>(batch.rowIndices.size());
  for (column_index_t i = 0; i < requestedType_->size(); ++i) {
    auto* childSpec = scanSpec_->childByName(requestedType_->nameOf(i));
    const auto sourceChannel =
        sourceChannelFor(rowType_, childSpec, requestedType_->nameOf(i));
    if (!sourceChannel.has_value()) {
      children.push_back(
          BaseVector::createNullConstant(
              requestedType_->childAt(i), batchSize, pool_));
      continue;
    }
    if (childSpec != nullptr && childSpec->isConstant()) {
      children.push_back(nullptr);
      continue;
    }
    const bool needsEagerValues = childSpec != nullptr &&
        (childSpec->hasFilter() || childSpec->deltaUpdate() != nullptr ||
         childSpec->hasTransform());
    const bool needsFileValues = childSpec != nullptr &&
        childSpec->readFromFile() &&
        (needsEagerValues || childSpec->projectOut());
    if (!needsFileValues) {
      children.push_back(nullptr);
      continue;
    }
    if (!batch.values.has_value()) {
      children.push_back(nullptr);
      continue;
    }
    VELOX_USER_CHECK(
        scanChannelsBySource_[sourceChannel.value()].has_value(),
        "The Vortex scan omitted a required field: {}",
        requestedType_->nameOf(i));
    auto field = batch.values->field(
        file_->session(), scanChannelsBySource_[sourceChannel.value()].value());
    if (!needsEagerValues && childSpec != nullptr && childSpec->projectOut()) {
      deferredFields.push_back(
          DeferredField{
              childSpec->channel(),
              requestedType_->nameOf(i),
              std::move(field),
              rowType_->childAt(sourceChannel.value()),
              requestedType_->childAt(i),
          });
      children.push_back(nullptr);
      continue;
    }
    if (!needsEagerValues) {
      children.push_back(nullptr);
      continue;
    }
    auto imported = importVortexVector(
        file_->session(),
        field,
        rowType_->childAt(sourceChannel.value()),
        {},
        *pool_);
    imported = applyNestedConstants(std::move(imported), childSpec);
    children.push_back(preserveParentNullsForFiltering(
        adaptVortexVectorType(
            imported, requestedType_->childAt(i), mapRowFieldsByPosition_),
        childSpec));
  }
  auto input = std::make_shared<RowVector>(
      pool_, requestedType_, nullptr, batchSize, std::move(children));

  std::vector<uint64_t> passed(bits::nwords(rowsToRead), ~uint64_t{0});
  if (mutation != nullptr) {
    if (mutation->deletedRows != nullptr) {
      bits::andWithNegatedBits(
          passed.data(), mutation->deletedRows, 0, rowsToRead);
    }
    if (mutation->randomSkip != nullptr) {
      bits::forEachSetBit(passed.data(), 0, rowsToRead, [&](auto row) {
        if (!mutation->randomSkip->testOne()) {
          bits::clearBit(passed.data(), row);
        }
      });
    }
  }

  std::vector<uint64_t> compactDeleted(bits::nwords(batchSize), 0);
  std::vector<vector_size_t> sourceRows;
  const bool hasDeltaUpdate = std::ranges::any_of(
      scanSpec_->children(),
      [](const auto& child) { return child->deltaUpdate() != nullptr; });
  if (hasDeltaUpdate) {
    sourceRows.reserve(batchSize);
  }
  for (vector_size_t row = 0; row < batchSize; ++row) {
    VELOX_USER_CHECK_GE(
        batch.rowIndices[row],
        firstRow,
        "A Vortex row index precedes the current source window: {}",
        batch.rowIndices[row]);
    const auto relativeRow = batch.rowIndices[row] - firstRow;
    VELOX_USER_CHECK_LT(
        relativeRow,
        rowsToRead,
        "A Vortex row index exceeds the current source window: {}",
        batch.rowIndices[row]);
    if (!bits::isBitSet(passed.data(), relativeRow)) {
      bits::setBit(compactDeleted.data(), row);
    }
    if (hasDeltaUpdate) {
      const auto splitRow = batch.rowIndices[row] - rowRange_.begin;
      VELOX_USER_CHECK_LE(
          splitRow,
          static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()),
          "A Vortex delta row exceeds the Velox row limit: {}",
          splitRow);
      sourceRows.push_back(static_cast<vector_size_t>(splitRow));
    }
  }
  const common::Mutation compactMutation{
      .deletedRows = compactDeleted.empty() ? nullptr : compactDeleted.data(),
      .randomSkip = nullptr,
  };
  auto projected = common::RowReader::projectColumnsWithSelection(
      input,
      *scanSpec_,
      mutation == nullptr ? nullptr : &compactMutation,
      sourceRows);
  auto* output = projected.output->asChecked<RowVector>();
  for (auto& deferred : deferredFields) {
    auto& child = output->childAt(deferred.channel);
    if (child != nullptr) {
      VELOX_CHECK(
          child->type()->equivalent(*deferred.targetType),
          "Deferred Vortex projections disagree on a channel type: {}",
          deferred.fieldName);
      continue;
    }
    if (output->size() == 0) {
      child = BaseVector::create(deferred.targetType, 0, pool_);
      continue;
    }
    child = std::make_shared<LazyVector>(
        pool_,
        deferred.targetType,
        output->size(),
        std::make_unique<VortexLazyLoader>(
            file_,
            std::move(deferred.field),
            deferred.sourceType,
            deferred.targetType,
            mapRowFieldsByPosition_,
            scanSpec_,
            std::move(deferred.fieldName),
            projected.selectedRows,
            *pool_));
  }
  return projected;
}

void VortexRowReader::addRowNumber(
    VectorPtr& result,
    const BufferPtr& selectedRows,
    const std::vector<uint64_t>& rowIndices) const {
  if (!rowNumberColumnInfo_.has_value()) {
    return;
  }
  auto* row = result->asChecked<RowVector>();
  const auto insertPosition = rowNumberColumnInfo_->insertPosition;
  VELOX_USER_CHECK_LE(
      insertPosition,
      row->childrenSize(),
      "The Vortex row-number position exceeds the output field count");
  auto rowNumbers =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), result->size(), pool_);
  auto* rawRowNumbers = rowNumbers->mutableRawValues();
  const auto* rawSelectedRows =
      selectedRows ? selectedRows->as<vector_size_t>() : nullptr;
  for (vector_size_t i = 0; i < result->size(); ++i) {
    const auto sourceRow = rawSelectedRows == nullptr ? i : rawSelectedRows[i];
    VELOX_CHECK_LT(sourceRow, rowIndices.size());
    rawRowNumbers[i] = static_cast<int64_t>(rowIndices[sourceRow]);
  }

  auto names = row->type()->asRow().names();
  auto types = row->type()->asRow().children();
  auto children = row->children();
  names.insert(names.begin() + insertPosition, rowNumberColumnInfo_->name);
  types.insert(types.begin() + insertPosition, BIGINT());
  children.insert(children.begin() + insertPosition, std::move(rowNumbers));
  result = std::make_shared<RowVector>(
      pool_,
      ROW(std::move(names), std::move(types)),
      result->nulls(),
      result->size(),
      std::move(children));
}

std::optional<VortexArray> VortexRowReader::nextVortexBatch() {
  if (scanFinished_) {
    return std::nullopt;
  }
  VELOX_CHECK_NOT_NULL(scan_);
  while (true) {
    vx_error* error{nullptr};
    if (partition_ == nullptr) {
      partition_ = vx_velox_scan_next_partition(scan_, &error);
      if (partition_ == nullptr) {
        if (error != nullptr) {
          failVortex("read a Vortex scan partition", error);
        }
        scanFinished_ = true;
        return std::nullopt;
      }
    }

    const auto* array = vx_velox_partition_next(partition_, &error);
    if (array == nullptr) {
      if (error != nullptr) {
        failVortex("read a Vortex scan array", error);
      }
      vx_velox_partition_free(partition_);
      partition_ = nullptr;
      continue;
    }

    auto batch = VortexArray::fromOwned(array);
    const auto invalidCount =
        vx_velox_array_invalid_count(file_->session(), batch.get(), &error);
    if (error != nullptr) {
      failVortex("read Vortex top-level validity", error);
    }
    VELOX_USER_CHECK_EQ(
        invalidCount, 0, "A Vortex scan must not produce null top-level rows");
    if (batch.size() == 0) {
      continue;
    }
    return batch;
  }
}

void VortexRowReader::preparePendingBatch() {
  if (pendingBatch_.has_value() || scanFinished_) {
    return;
  }
  pendingBatch_ = nextVortexBatch();
  pendingOffset_ = 0;
  pendingRowIndices_.clear();
  if (!pendingBatch_.has_value()) {
    return;
  }

  const auto* retainedRange = activeRetainedRange();
  VELOX_CHECK_NOT_NULL(retainedRange);

  auto rowIndexField = pendingBatch_->field(file_->session(), 0);
  pendingRowIndices_ = readVortexRowIndices(file_->session(), rowIndexField);
  VELOX_USER_CHECK_EQ(
      pendingRowIndices_.size(),
      pendingBatch_->size(),
      "Vortex row-index count does not match its batch size");
  for (size_t i = 0; i < pendingRowIndices_.size(); ++i) {
    VELOX_USER_CHECK_GE(
        pendingRowIndices_[i],
        currentRow_,
        "A Vortex scan returned an already consumed row: {}",
        pendingRowIndices_[i]);
    VELOX_USER_CHECK_LT(
        pendingRowIndices_[i],
        retainedRange->end,
        "A Vortex scan returned a row outside its assigned range: {}",
        pendingRowIndices_[i]);
    if (i != 0) {
      VELOX_USER_CHECK_LT(
          pendingRowIndices_[i - 1],
          pendingRowIndices_[i],
          "Vortex row indexes must be strictly increasing: {}",
          pendingRowIndices_[i]);
    }
  }
}

vector_size_t VortexRowReader::rowsForNextRead(uint64_t size) {
  advanceRetainedRange();
  auto* retainedRange = activeRetainedRange();
  if (retainedRange == nullptr) {
    const auto nextRetainedRow = retainedRangeIndex_ < retainedRanges_.size()
        ? retainedRanges_[retainedRangeIndex_].begin
        : rowRange_.end;
    VELOX_CHECK_GT(nextRetainedRow, currentRow_);
    return static_cast<vector_size_t>(std::min<uint64_t>(
        std::min<uint64_t>(size, nextRetainedRow - currentRow_),
        std::numeric_limits<vector_size_t>::max()));
  }
  if (scan_ == nullptr) {
    startScan();
  }
  preparePendingBatch();
  const auto maxRows = static_cast<vector_size_t>(std::min<uint64_t>(
      std::min<uint64_t>(size, retainedRange->end - currentRow_),
      std::numeric_limits<vector_size_t>::max()));
  if (!pendingBatch_.has_value()) {
    return maxRows;
  }

  VELOX_CHECK_LT(pendingOffset_, pendingRowIndices_.size());
  const auto windowEnd = currentRow_ + maxRows;
  const auto begin = pendingRowIndices_.begin() + pendingOffset_;
  if (*begin >= windowEnd) {
    return maxRows;
  }
  const auto end = std::lower_bound(begin, pendingRowIndices_.end(), windowEnd);
  VELOX_CHECK(begin != end);
  return static_cast<vector_size_t>(*(end - 1) + 1 - currentRow_);
}

void VortexRowReader::verifyScanEnded() {
  VELOX_USER_CHECK(
      !pendingBatch_.has_value() || pendingOffset_ == pendingBatch_->size(),
      "The Vortex scan produced rows beyond its assigned row range: {}, {}",
      rowRange_.begin,
      rowRange_.end);
  pendingBatch_.reset();
  pendingRowIndices_.clear();
  pendingOffset_ = 0;
  VELOX_USER_CHECK(
      !nextVortexBatch().has_value(),
      "The Vortex scan produced rows beyond its assigned row range: {}, {}",
      rowRange_.begin,
      rowRange_.end);
}

} // namespace facebook::velox::dwio::vortex
