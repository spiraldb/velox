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

#include "velox/dwio/vortex/VortexType.h"

#include <string>
#include <string_view>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::dwio::vortex {
namespace {

std::string errorMessage(const vx_error* error) {
  if (error == nullptr) {
    return "Vortex returned an unspecified error";
  }
  const auto message = vx_velox_error_message(error);
  if (message.ptr == nullptr) {
    return "Vortex returned an unspecified error";
  }
  return std::string{message.ptr, message.len};
}

[[noreturn]] void failVortex(std::string_view operation, vx_error* error) {
  const auto errorText = errorMessage(error);
  vx_velox_error_free(error);
  VELOX_USER_FAIL("Failed to {}: {}", operation, errorText);
}

class ArrowSchemaOwner {
 public:
  ~ArrowSchemaOwner() {
    if (schema_.release != nullptr) {
      schema_.release(&schema_);
    }
  }

  ArrowSchema* get() {
    return &schema_;
  }

  const ArrowSchema& schema() const {
    return schema_;
  }

 private:
  ArrowSchema schema_{};
};

} // namespace

TypePtr typeFromVortexSource(const vx_velox_source* source) {
  VELOX_USER_CHECK_NOT_NULL(source, "Vortex source must not be null");

  ArrowSchemaOwner schema;
  vx_error* error{nullptr};
  if (vx_velox_source_export_schema(source, schema.get(), &error) != 0) {
    failVortex("read the Vortex schema", error);
  }
  if (error != nullptr) {
    vx_velox_error_free(error);
  }
  return importFromArrow(schema.schema());
}

} // namespace facebook::velox::dwio::vortex
