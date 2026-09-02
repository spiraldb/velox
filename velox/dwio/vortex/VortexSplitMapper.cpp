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

#include "velox/dwio/vortex/VortexSplitMapper.h"

#include <algorithm>
#include <cstddef>
#include <limits>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::vortex {
namespace {

void validateNaturalRowRanges(
    uint64_t numFileRows,
    const std::vector<VortexRowRange>& naturalRowRanges) {
  if (numFileRows == 0) {
    VELOX_USER_CHECK(
        naturalRowRanges.empty(),
        "Natural row ranges must be empty for a zero-row Vortex file.");
    return;
  }

  VELOX_USER_CHECK(
      !naturalRowRanges.empty(),
      "A non-empty Vortex file must have natural row ranges.");

  uint64_t expectedBegin{0};
  for (const auto& naturalRowRange : naturalRowRanges) {
    VELOX_USER_CHECK_LT(
        naturalRowRange.begin,
        naturalRowRange.end,
        "Natural row ranges must be non-empty: {}, {}",
        naturalRowRange.begin,
        naturalRowRange.end);
    VELOX_USER_CHECK_EQ(
        naturalRowRange.begin,
        expectedBegin,
        "Natural row ranges must be ordered and contiguous: {}, {}",
        naturalRowRange.begin,
        expectedBegin);
    VELOX_USER_CHECK_LE(
        naturalRowRange.end,
        numFileRows,
        "Natural row range exceeds the file row count: {}, {}",
        naturalRowRange.end,
        numFileRows);
    expectedBegin = naturalRowRange.end;
  }

  VELOX_USER_CHECK_EQ(
      expectedBegin,
      numFileRows,
      "Natural row ranges must cover the file row count: {}, {}",
      expectedBegin,
      numFileRows);
}

uint64_t splitAssignmentByte(
    size_t naturalRowRangeIndex,
    const VortexRowRange& naturalRowRange,
    uint64_t numFileRows,
    uint64_t fileByteSize) {
  if (naturalRowRangeIndex == 0) {
    return 0;
  }

  const auto midpointRow =
      naturalRowRange.begin + (naturalRowRange.end - naturalRowRange.begin) / 2;
  const auto midpointByte =
      (static_cast<__uint128_t>(midpointRow) * fileByteSize) / numFileRows;
  return static_cast<uint64_t>(midpointByte);
}

} // namespace

std::optional<VortexRowRange> VortexSplitMapper::map(
    uint64_t numFileRows,
    uint64_t fileByteSize,
    const std::vector<VortexRowRange>& naturalRowRanges,
    uint64_t byteOffset,
    uint64_t byteLength) {
  validateNaturalRowRanges(numFileRows, naturalRowRanges);
  if (numFileRows == 0 || byteLength == 0) {
    return std::nullopt;
  }

  VELOX_USER_CHECK_LE(
      byteLength,
      std::numeric_limits<uint64_t>::max() - byteOffset,
      "Vortex byte range exceeds the supported address range: {}, {}",
      byteOffset,
      byteLength);
  const auto byteRangeEnd = byteOffset + byteLength;

  std::vector<uint64_t> assignmentBytes;
  assignmentBytes.reserve(naturalRowRanges.size());
  for (size_t i = 0; i < naturalRowRanges.size(); ++i) {
    assignmentBytes.push_back(
        splitAssignmentByte(i, naturalRowRanges[i], numFileRows, fileByteSize));
  }

  const auto firstOwnedAssignment = std::lower_bound(
      assignmentBytes.begin(), assignmentBytes.end(), byteOffset);
  const auto afterLastOwnedAssignment = std::lower_bound(
      assignmentBytes.begin(), assignmentBytes.end(), byteRangeEnd);
  if (firstOwnedAssignment == afterLastOwnedAssignment) {
    return std::nullopt;
  }

  const auto firstOwnedRowRangeIndex =
      static_cast<size_t>(firstOwnedAssignment - assignmentBytes.begin());
  const auto afterLastOwnedRowRangeIndex =
      static_cast<size_t>(afterLastOwnedAssignment - assignmentBytes.begin());
  return VortexRowRange{
      naturalRowRanges[firstOwnedRowRangeIndex].begin,
      naturalRowRanges[afterLastOwnedRowRangeIndex - 1].end,
  };
}

} // namespace facebook::velox::dwio::vortex
