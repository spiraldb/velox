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

#include "velox/dwio/vortex/tests/VortexTestFile.h"

#include <memory>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempFilePath.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/vector/arrow/Bridge.h"
#include "vortex_velox_test.h"

namespace facebook::velox::test {
namespace {

std::string errorMessage(const vx_velox_error* error) {
  if (error == nullptr) {
    return "Vortex returned an unspecified error";
  }
  const auto message = vx_velox_error_message(error);
  return std::string{message.ptr, message.len};
}

void checkVortexError(vx_velox_error*& error) {
  if (error == nullptr) {
    return;
  }
  const auto errorText = errorMessage(error);
  vx_velox_error_free(error);
  error = nullptr;
  VELOX_FAIL("Vortex test operation failed: {}", errorText);
}

struct WriterReleaser {
  void operator()(vx_velox_test_writer* writer) const {
    if (writer != nullptr) {
      vx_velox_test_writer_abort(writer);
    }
  }
};

} // namespace

void writeVortexFile(
    const std::string& path,
    const std::vector<RowVectorPtr>& batches,
    memory::MemoryPool* pool,
    const ArrowOptions& arrowOptions) {
  VELOX_CHECK_NOT_NULL(pool);
  VELOX_CHECK(!batches.empty());
  vx_velox_error* error{nullptr};
  const vx_velox_view pathView{path.data(), path.size()};
  std::unique_ptr<vx_velox_test_writer, WriterReleaser> writer{
      vx_velox_test_writer_new(pathView, &error)};
  checkVortexError(error);
  VELOX_CHECK_NOT_NULL(writer);
  for (const auto& batch : batches) {
    ArrowArray arrowArray{};
    ArrowSchema arrowSchema{};
    exportToArrow(batch, arrowArray, pool, arrowOptions);
    exportToArrow(batch, arrowSchema, arrowOptions);
    vx_velox_test_writer_push(writer.get(), &arrowArray, &arrowSchema, &error);
    checkVortexError(error);
  }

  auto* rawWriter = writer.release();
  VELOX_CHECK_NOT_NULL(rawWriter);
  vx_velox_test_writer_close(rawWriter, &error);
  checkVortexError(error);
}

std::string writeVortexBytes(
    const std::vector<RowVectorPtr>& batches,
    memory::MemoryPool* pool) {
  const auto file = common::testutil::TempFilePath::create();
  writeVortexFile(file->getPath(), batches, pool);
  std::shared_ptr<ReadFile> readFile =
      std::make_shared<LocalReadFile>(file->getPath());
  return readFile->pread(0, readFile->size());
}

} // namespace facebook::velox::test
