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

namespace facebook::velox::test {
namespace {

std::string errorMessage(const vx_error* error) {
  if (error == nullptr) {
    return "Vortex returned an unspecified error";
  }
  const auto message = vx_error_message(error);
  return std::string{message.ptr, message.len};
}

void checkVortexError(vx_error*& error) {
  if (error == nullptr) {
    return;
  }
  const auto errorText = errorMessage(error);
  vx_error_free(error);
  error = nullptr;
  VELOX_FAIL("Vortex test operation failed: {}", errorText);
}

struct SinkReleaser {
  void operator()(vx_array_sink* sink) const {
    if (sink != nullptr) {
      vx_array_sink_abort(sink);
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
  std::unique_ptr<vx_session, decltype(&vx_session_free)> session{
      vx_session_new(), vx_session_free};
  VELOX_CHECK_NOT_NULL(session);

  vx_error* error{nullptr};
  const vx_view pathView{path.data(), path.size()};
  std::unique_ptr<vx_array_sink, SinkReleaser> sink;
  for (const auto& batch : batches) {
    ArrowArray arrowArray{};
    ArrowSchema arrowSchema{};
    exportToArrow(batch, arrowArray, pool, arrowOptions);
    exportToArrow(batch, arrowSchema, arrowOptions);
    std::unique_ptr<const vx_array, decltype(&vx_array_free)> array{
        vx_array_from_arrow(
            session.get(), &arrowArray, &arrowSchema, false, &error),
        vx_array_free};
    checkVortexError(error);
    VELOX_CHECK_NOT_NULL(array);

    if (sink == nullptr) {
      std::unique_ptr<const vx_dtype, decltype(&vx_dtype_free)> dtype{
          vx_array_dtype(array.get()), vx_dtype_free};
      VELOX_CHECK_NOT_NULL(dtype);
      sink.reset(vx_array_sink_open_file(
          session.get(), pathView, dtype.get(), &error));
      checkVortexError(error);
      VELOX_CHECK_NOT_NULL(sink);
    }
    vx_array_sink_push(sink.get(), array.get(), &error);
    checkVortexError(error);
  }

  auto* rawSink = sink.release();
  VELOX_CHECK_NOT_NULL(rawSink);
  vx_array_sink_close(rawSink, &error);
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
