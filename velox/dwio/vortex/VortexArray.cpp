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

#include "velox/dwio/vortex/VortexArray.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/vortex/VortexFfi.h"

namespace facebook::velox::dwio::vortex {
namespace {

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

} // namespace

VortexArray VortexArray::fromOwned(const vx_array* array) {
  VELOX_USER_CHECK_NOT_NULL(array, "Vortex array must not be null");
  return VortexArray{
      std::shared_ptr<const vx_array>{array, vx_velox_array_free}};
}

VortexArray::VortexArray(std::shared_ptr<const vx_array> array)
    : array_{std::move(array)} {}

size_t VortexArray::size() const {
  VELOX_CHECK_NOT_NULL(array_);
  return vx_velox_array_len(array_.get());
}

VortexArray VortexArray::field(const vx_session* session, size_t index) const {
  VELOX_CHECK_NOT_NULL(session);
  VELOX_CHECK_NOT_NULL(array_);
  vx_error* error{nullptr};
  const auto* field =
      vx_velox_array_get_field(session, array_.get(), index, &error);
  if (field == nullptr) {
    failVortex("read a Vortex struct field", error);
  }
  return fromOwned(field);
}

VortexArray VortexArray::slice(size_t begin, size_t end) const {
  VELOX_CHECK_NOT_NULL(array_);
  vx_error* error{nullptr};
  const auto* sliced = vx_velox_array_slice(array_.get(), begin, end, &error);
  if (sliced == nullptr) {
    failVortex("slice a Vortex array", error);
  }
  return fromOwned(sliced);
}

const vx_array* VortexArray::get() const {
  return array_.get();
}

VortexArray::operator bool() const {
  return array_ != nullptr;
}

} // namespace facebook::velox::dwio::vortex
