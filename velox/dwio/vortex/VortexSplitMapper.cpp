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
    const std::vector<VortexNaturalSplit>& naturalSplits) {
  if (naturalSplits.empty()) {
    return;
  }
  VELOX_USER_CHECK_EQ(
      naturalSplits.front().assignmentByte,
      0,
      "The first natural split assignment must be byte zero: {}",
      naturalSplits.front().assignmentByte);

  uint64_t expectedBegin{0};
  uint64_t previousAssignmentByte{0};
  for (const auto& naturalSplit : naturalSplits) {
    const auto& naturalRowRange = naturalSplit.rows;
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
    VELOX_USER_CHECK_GE(
        naturalSplit.assignmentByte,
        previousAssignmentByte,
        "Natural split assignment bytes must be ordered: {}, {}",
        naturalSplit.assignmentByte,
        previousAssignmentByte);
    expectedBegin = naturalRowRange.end;
    previousAssignmentByte = naturalSplit.assignmentByte;
  }
}

} // namespace

std::optional<VortexRowRange> VortexSplitMapper::map(
    const std::vector<VortexNaturalSplit>& naturalSplits,
    uint64_t byteOffset,
    uint64_t byteLength) {
  validateNaturalRowRanges(naturalSplits);
  if (naturalSplits.empty() || byteLength == 0) {
    return std::nullopt;
  }

  VELOX_USER_CHECK_LE(
      byteLength,
      std::numeric_limits<uint64_t>::max() - byteOffset,
      "Vortex byte range exceeds the supported address range: {}, {}",
      byteOffset,
      byteLength);
  const auto byteRangeEnd = byteOffset + byteLength;

  const auto firstOwnedAssignment = std::lower_bound(
      naturalSplits.begin(),
      naturalSplits.end(),
      byteOffset,
      [](const VortexNaturalSplit& split, uint64_t offset) {
        return split.assignmentByte < offset;
      });
  const auto afterLastOwnedAssignment = std::lower_bound(
      naturalSplits.begin(),
      naturalSplits.end(),
      byteRangeEnd,
      [](const VortexNaturalSplit& split, uint64_t offset) {
        return split.assignmentByte < offset;
      });
  if (firstOwnedAssignment == afterLastOwnedAssignment) {
    return std::nullopt;
  }

  const auto firstOwnedRowRangeIndex =
      static_cast<size_t>(firstOwnedAssignment - naturalSplits.begin());
  const auto afterLastOwnedRowRangeIndex =
      static_cast<size_t>(afterLastOwnedAssignment - naturalSplits.begin());
  return VortexRowRange{
      naturalSplits[firstOwnedRowRangeIndex].rows.begin,
      naturalSplits[afterLastOwnedRowRangeIndex - 1].rows.end,
  };
}

} // namespace facebook::velox::dwio::vortex
