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

#include <cstdint>
#include <optional>
#include <vector>

namespace facebook::velox::dwio::vortex {

/// Identifies a half-open range of absolute file rows.
struct VortexRowRange {
  /// Specifies the first row in the range.
  uint64_t begin{0};

  /// Specifies the first row after the range.
  uint64_t end{0};
};

/// Identifies a natural row range and its external byte-range assignment.
struct VortexNaturalSplit {
  /// Specifies the rows in the split.
  VortexRowRange rows;

  /// Specifies the file byte that assigns the split to an external range.
  uint64_t assignmentByte{0};
};

/// Assigns complete Vortex natural row ranges to Velox byte ranges.
class VortexSplitMapper final {
 public:
  /// Returns the contiguous natural row ranges owned by the half-open byte
  /// range [byteOffset, byteOffset + byteLength). Returns no range when the
  /// byte range owns no natural split. Throws a user error when the natural
  /// splits are not ordered or contiguous.
  static std::optional<VortexRowRange> map(
      const std::vector<VortexNaturalSplit>& naturalSplits,
      uint64_t byteOffset,
      uint64_t byteLength);
};

} // namespace facebook::velox::dwio::vortex
