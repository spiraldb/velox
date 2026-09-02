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
#include <memory>
#include <utility>
#include <vector>

#include "velox/dwio/common/BufferedInput.h"
#include "velox/type/Type.h"

struct vx_data_source;
struct vx_expression;
struct vx_session;
struct vx_velox_source;

namespace facebook::velox::dwio::vortex {

/// Owns an open Vortex file and its Velox-backed input callbacks.
class VortexFile {
 public:
  /// Opens a Vortex file through the supplied buffered input.
  VortexFile(
      std::unique_ptr<common::BufferedInput> input,
      memory::MemoryPool& pool);

  ~VortexFile();

  VortexFile(const VortexFile&) = delete;
  VortexFile& operator=(const VortexFile&) = delete;

  /// Returns the number of rows in the file.
  uint64_t rowCount() const;

  /// Returns the logical row type in the file.
  const RowTypePtr& rowType() const;

  /// Returns the file size in bytes.
  uint64_t fileSize() const;

  /// Returns the natural row ranges reported by Vortex.
  const std::vector<std::pair<uint64_t, uint64_t>>& naturalSplits() const;

  /// Returns the Vortex session used by this file.
  const vx_session* session() const;

  /// Creates an owned Vortex data source for a scan.
  const vx_data_source* createDataSource() const;

  /// Returns safe exclusion decisions for a natural split range.
  std::vector<uint8_t> pruneNaturalSplits(
      const vx_expression* expression,
      size_t firstSplit,
      size_t splitCount) const;

 private:
  // Owns the Vortex execution session for this file.
  vx_session* session_{nullptr};

  // Owns the open Vortex source and its callback input.
  vx_velox_source* source_{nullptr};

  // Stores the exact number of rows from the file footer.
  uint64_t rowCount_{0};

  // Stores the logical Velox type from the Vortex schema.
  RowTypePtr rowType_;

  // Stores the physical file size used for byte-split assignment.
  uint64_t fileSize_{0};

  // Stores the natural row ranges from the Vortex layout.
  std::vector<std::pair<uint64_t, uint64_t>> naturalSplits_;
};

} // namespace facebook::velox::dwio::vortex
