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

struct vx_array;
struct vx_session;

namespace facebook::velox::dwio::vortex {

/// Retains one immutable Vortex array handle across engine operations.
class VortexArray {
 public:
  /// Takes ownership of an array handle returned through the Vortex C API.
  static VortexArray fromOwned(const vx_array* array);

  /// Returns the number of logical values in the array.
  size_t size() const;

  /// Returns an owned field array from a struct array.
  VortexArray field(const vx_session* session, size_t index) const;

  /// Returns an owned half-open slice of this array.
  VortexArray slice(size_t begin, size_t end) const;

  /// Returns the retained C handle.
  const vx_array* get() const;

  /// Returns true when this wrapper contains no array.
  explicit operator bool() const;

 private:
  explicit VortexArray(std::shared_ptr<const vx_array> array);

  // Retains the immutable array through the Vortex reference count.
  std::shared_ptr<const vx_array> array_;
};

} // namespace facebook::velox::dwio::vortex
