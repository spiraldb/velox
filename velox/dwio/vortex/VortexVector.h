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

struct vx_session;

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

/// Returns true when the semantic visitor can load this Velox type natively.
bool supportsNativeVortexType(const TypePtr& type);

/// Copies a non-null U64 Vortex array into absolute row indexes.
std::vector<uint64_t> readVortexRowIndices(
    const vx_session* session,
    const VortexArray& array);

/// Imports selected source rows into a compact Velox vector.
VectorPtr importVortexVector(
    const vx_session* session,
    const VortexArray& array,
    const TypePtr& targetType,
    RowSet sourceRows,
    memory::MemoryPool& pool);

/// Sends selected source rows directly to a Velox value hook.
void loadVortexValueHook(
    const vx_session* session,
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
