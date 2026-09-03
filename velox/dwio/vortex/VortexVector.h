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
#include <vector>

#include "velox/dwio/vortex/VortexArray.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/vector/LazyVector.h"

struct vx_velox_session;

namespace facebook::velox::dwio::vortex {

/// Owns the callback context that charges Arrow payload memory to Velox.
class VortexArrowMemory {
 public:
  /// Creates callbacks that report retained bytes to the supplied pool.
  explicit VortexArrowMemory(memory::MemoryPool& pool);

  ~VortexArrowMemory();

  VortexArrowMemory(const VortexArrowMemory&) = delete;
  VortexArrowMemory& operator=(const VortexArrowMemory&) = delete;

  /// Returns the callback table for one Vortex Arrow export.
  const vx_velox_arrow_memory_callbacks& callbacks() const;

 private:
  struct Context;

  static void retainContext(void* context) noexcept;
  static void releaseContext(void* context) noexcept;
  static int32_t reportAllocation(void* context, size_t bytes) noexcept;
  static void reportFree(void* context, size_t bytes) noexcept;
  static const char* lastError(void* context) noexcept;

  // Holds the initial context reference until the export call returns.
  Context* context_;

  // Stores the stable adapter callback table.
  vx_velox_arrow_memory_callbacks callbacks_;
};

/// Defers one prepared Vortex array until Velox requests materialized values.
class VortexExportCursor {
 public:
  /// Retains one array for later repeated native exports.
  VortexExportCursor(const vx_velox_session* session, const VortexArray& array);

  ~VortexExportCursor();

  VortexExportCursor(const VortexExportCursor&) = delete;
  VortexExportCursor& operator=(const VortexExportCursor&) = delete;

  /// Imports one contiguous window and applies an optional local selection.
  VectorPtr import(
      size_t offset,
      size_t length,
      const TypePtr& targetType,
      RowSet sourceRows,
      memory::MemoryPool& pool);

 private:
  struct State;
  std::unique_ptr<State> state_;
};

/// Retains absolute source-row positions without an intermediate copy.
class VortexRowPositions {
 public:
  /// Creates contiguous positions that start at firstRow.
  VortexRowPositions(uint64_t firstRow, size_t size);

  /// Creates a position view over one retained U64 buffer.
  VortexRowPositions(BufferPtr values, size_t size);

  /// Returns the number of source-row positions.
  size_t size() const;

  /// Returns the source-row position at index.
  uint64_t at(size_t index) const;

  /// Returns the first index whose position is at least value.
  size_t lowerBound(size_t begin, uint64_t value) const;

  /// Returns true when positions form one contiguous range.
  bool isContiguous() const;

  /// Verifies range bounds and debug-checks strict ordering.
  void validateRange(uint64_t begin, uint64_t end) const;

 private:
  // Retains explicit immutable U64 positions when rows are filtered.
  BufferPtr values_;

  // Stores the first position when rows form one contiguous range.
  uint64_t firstRow_{0};

  // Stores the number of U64 positions in the buffer.
  size_t size_;
};

/// Returns true when the semantic visitor can load this Velox type natively.
bool supportsNativeVortexType(const TypePtr& type);

/// Retains a non-null U64 Vortex array as absolute row positions.
VortexRowPositions readVortexRowIndices(
    const vx_velox_session* session,
    const VortexArray& array,
    memory::MemoryPool& pool);

/// Imports selected source rows into a compact Velox vector.
VectorPtr importVortexVector(
    const vx_velox_session* session,
    const VortexArray& array,
    const TypePtr& targetType,
    RowSet sourceRows,
    memory::MemoryPool& pool);

/// Sends selected source rows directly to a Velox value hook.
void loadVortexValueHook(
    const vx_velox_session* session,
    const VortexArray& array,
    const TypePtr& targetType,
    RowSet sourceRows,
    RowSet hookRows,
    ValueHook& hook,
    memory::MemoryPool& pool);

/// Applies supported DWIO schema widening to an imported vector.
VectorPtr adaptVortexVectorType(
    const VectorPtr& input,
    const TypePtr& targetType,
    bool mapRowFieldsByPosition = false);

} // namespace facebook::velox::dwio::vortex
