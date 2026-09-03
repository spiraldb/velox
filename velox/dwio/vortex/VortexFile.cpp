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

#include "velox/dwio/vortex/VortexFile.h"

#include <algorithm>
#include <array>
#include <exception>
#include <limits>
#include <mutex>
#include <string>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/dwio/vortex/VortexType.h"

namespace facebook::velox::dwio::vortex {
namespace {

constexpr size_t kReadConcurrencyHint{4};

struct ReadCallbackErrorState {
  const void* context{nullptr};
  std::array<char, 1'024> error{};
};

thread_local ReadCallbackErrorState readCallbackErrorState;

void setReadCallbackError(
    const void* context,
    std::string_view message) noexcept {
  readCallbackErrorState.context = context;
  const auto length =
      std::min(message.size(), readCallbackErrorState.error.size() - 1);
  std::copy_n(message.data(), length, readCallbackErrorState.error.data());
  readCallbackErrorState.error[length] = '\0';
}

uint32_t readAlignment(size_t requestedAlignment) {
  VELOX_CHECK_LE(
      requestedAlignment,
      std::numeric_limits<uint32_t>::max(),
      "Vortex read alignment exceeds the supported range: {}",
      requestedAlignment);
  const auto alignment = std::max<size_t>(
      requestedAlignment, memory::MemoryAllocator::kMinAlignment);
  VELOX_CHECK(
      alignment != 0 && (alignment & (alignment - 1)) == 0,
      "Vortex read alignment must be a power of two: {}",
      requestedAlignment);
  return static_cast<uint32_t>(alignment);
}

int64_t readSize(size_t size) {
  VELOX_CHECK_LE(
      size,
      std::numeric_limits<int64_t>::max(),
      "Vortex read size exceeds the supported range: {}",
      size);
  return static_cast<int64_t>(size);
}

class AlignedReadBuffer {
 public:
  AlignedReadBuffer(
      size_t size,
      size_t requestedAlignment,
      memory::MemoryPool& pool)
      : size_{readSize(size)},
        alignment_{readAlignment(requestedAlignment)},
        pool_{pool},
        data_{size == 0 ? nullptr : pool_.allocateAligned(size_, alignment_)} {}

  ~AlignedReadBuffer() noexcept {
    try {
      if (data_ != nullptr) {
        pool_.freeAligned(data_, size_, alignment_);
      }
    } catch (...) {
    }
  }

  char* data() const {
    return static_cast<char*>(data_);
  }

 private:
  int64_t size_;
  uint32_t alignment_;
  memory::MemoryPool& pool_;
  void* data_;
};

class BufferedInputContext {
 public:
  BufferedInputContext(
      std::unique_ptr<common::BufferedInput> input,
      memory::MemoryPool& pool)
      : input_{std::move(input)}, pool_{pool} {
    VELOX_CHECK_NOT_NULL(input_);
  }

  int32_t size(uint64_t* sizeOut) noexcept {
    try {
      VELOX_CHECK_NOT_NULL(sizeOut);
      std::lock_guard<std::mutex> lock{mutex_};
      *sizeOut = input_->getInputStream()->getLength();
      return 0;
    } catch (const std::exception& error) {
      setReadCallbackError(this, error.what());
      return 1;
    } catch (...) {
      setReadCallbackError(
          this, "Unknown error from the Velox input size callback");
      return 1;
    }
  }

  int32_t readRanges(
      const vx_velox_read_request* requests,
      size_t requestCount,
      vx_velox_buffer* outputs) noexcept {
    try {
      VELOX_CHECK(
          requestCount == 0 || requests != nullptr,
          "Vortex read requests must not be null");
      VELOX_CHECK(
          requestCount == 0 || outputs != nullptr,
          "Vortex read outputs must not be null");

      std::lock_guard<std::mutex> lock{mutex_};
      // A load invalidates streams backed by the previous load. Reset first so
      // every stream in this batch belongs to the same load.
      input_->reset();
      std::vector<std::unique_ptr<common::SeekableInputStream>> streams;
      streams.reserve(requestCount);
      for (size_t i = 0; i < requestCount; ++i) {
        VELOX_CHECK_GE(
            requests[i].struct_size,
            sizeof(vx_velox_read_request),
            "Vortex read request structure is too small: {}",
            requests[i].struct_size);
        streams.push_back(input_->enqueue(
            velox::common::Region{requests[i].offset, requests[i].length}));
      }
      input_->load(common::LogType::FILE);

      std::vector<std::unique_ptr<AlignedReadBuffer>> owners;
      owners.reserve(requestCount);
      for (size_t i = 0; i < requestCount; ++i) {
        owners.push_back(
            std::make_unique<AlignedReadBuffer>(
                requests[i].length, requests[i].alignment, pool_));
        if (requests[i].length != 0) {
          streams[i]->readFully(owners.back()->data(), requests[i].length);
        }
      }
      for (size_t i = 0; i < requestCount; ++i) {
        outputs[i] = vx_velox_buffer{
            .struct_size = sizeof(vx_velox_buffer),
            .data = reinterpret_cast<const uint8_t*>(owners[i]->data()),
            .length = requests[i].length,
            .owner = owners[i].release(),
            .release =
                [](void* rawOwner) noexcept {
                  try {
                    delete static_cast<AlignedReadBuffer*>(rawOwner);
                  } catch (...) {
                  }
                },
        };
      }
      return 0;
    } catch (const std::exception& error) {
      setReadCallbackError(this, error.what());
      return 1;
    } catch (...) {
      setReadCallbackError(
          this, "Unknown error from the Velox range read callback");
      return 1;
    }
  }

 private:
  std::unique_ptr<common::BufferedInput> input_;
  memory::MemoryPool& pool_;
  std::mutex mutex_;
};

int32_t readSize(void* context, uint64_t* sizeOut) noexcept {
  try {
    VELOX_CHECK_NOT_NULL(context);
    return static_cast<BufferedInputContext*>(context)->size(sizeOut);
  } catch (const std::exception& error) {
    setReadCallbackError(context, error.what());
    return 1;
  } catch (...) {
    setReadCallbackError(context, "Unknown Velox input size callback error");
    return 1;
  }
}

int32_t readRanges(
    void* context,
    const vx_velox_read_request* requests,
    size_t requestCount,
    vx_velox_buffer* outputs) noexcept {
  try {
    VELOX_CHECK_NOT_NULL(context);
    return static_cast<BufferedInputContext*>(context)->readRanges(
        requests, requestCount, outputs);
  } catch (const std::exception& error) {
    setReadCallbackError(context, error.what());
    return 1;
  } catch (...) {
    setReadCallbackError(context, "Unknown Velox range read callback error");
    return 1;
  }
}

const char* lastError(void* context) noexcept {
  if (readCallbackErrorState.context != context) {
    return "";
  }
  return readCallbackErrorState.error.data();
}

void releaseContext(void* context) noexcept {
  try {
    delete static_cast<BufferedInputContext*>(context);
  } catch (...) {
  }
}

int32_t isCancelled(void* context) noexcept {
  try {
    VELOX_CHECK_NOT_NULL(context);
    return 0;
  } catch (const std::exception& error) {
    setReadCallbackError(context, error.what());
    return 1;
  } catch (...) {
    setReadCallbackError(context, "Unknown Velox cancellation callback error");
    return 1;
  }
}

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

const vx_session* defaultSession() {
  static const std::unique_ptr<vx_session, decltype(&vx_velox_session_free)>
      session{vx_velox_session_new(), vx_velox_session_free};
  VELOX_CHECK_NOT_NULL(session);
  return session.get();
}

} // namespace

VortexFile::VortexFile(
    std::unique_ptr<common::BufferedInput> input,
    memory::MemoryPool& pool) {
  VELOX_USER_CHECK_EQ(
      vx_velox_abi_version(),
      VX_VELOX_ABI_VERSION,
      "Unsupported Vortex Velox ABI version: {}",
      vx_velox_abi_version());
  constexpr uint64_t kRequiredCapabilities = VX_VELOX_CAPABILITY_BATCH_READ |
      VX_VELOX_CAPABILITY_CALLBACK_SOURCE | VX_VELOX_CAPABILITY_NATURAL_SPLITS |
      VX_VELOX_CAPABILITY_ARROW_SCHEMA | VX_VELOX_CAPABILITY_PRIMITIVE_VISITOR |
      VX_VELOX_CAPABILITY_ARRAY_ARROW_EXPORT |
      VX_VELOX_CAPABILITY_ROW_INDEX_PROJECTION |
      VX_VELOX_CAPABILITY_NATURAL_SPLIT_PRUNING |
      VX_VELOX_CAPABILITY_READ_CANCELLATION |
      VX_VELOX_CAPABILITY_EXPORT_CURSOR | VX_VELOX_CAPABILITY_PLAIN_PROJECTION |
      VX_VELOX_CAPABILITY_VARBIN_VISITOR |
      VX_VELOX_CAPABILITY_DICTIONARY_VISITOR |
      VX_VELOX_CAPABILITY_CONSTANT_VISITOR | VX_VELOX_CAPABILITY_BOOL_VISITOR |
      VX_VELOX_CAPABILITY_DATE_VISITOR | VX_VELOX_CAPABILITY_DECIMAL_VISITOR |
      VX_VELOX_CAPABILITY_STRUCT_VISITOR | VX_VELOX_CAPABILITY_LIST_VISITOR |
      VX_VELOX_CAPABILITY_MAP_VISITOR;
  VELOX_USER_CHECK_EQ(
      vx_velox_capabilities() & kRequiredCapabilities,
      kRequiredCapabilities,
      "The Vortex Velox adapter lacks required capabilities: {}",
      kRequiredCapabilities & ~vx_velox_capabilities());

  std::unique_ptr<vx_session, decltype(&vx_velox_session_free)> session{
      vx_velox_session_clone(defaultSession()), vx_velox_session_free};
  VELOX_CHECK_NOT_NULL(session);

  auto context = std::make_unique<BufferedInputContext>(std::move(input), pool);
  const vx_velox_read_at_callbacks callbacks{
      .struct_size = sizeof(vx_velox_read_at_callbacks),
      .abi_version = VX_VELOX_ABI_VERSION,
      .context = context.get(),
      .size = readSize,
      .read_ranges = readRanges,
      .last_error = lastError,
      .release_context = releaseContext,
      .is_cancelled = isCancelled,
      .concurrency = kReadConcurrencyHint,
  };

  vx_error* error = nullptr;
  std::unique_ptr<vx_velox_read_at, decltype(&vx_velox_read_at_free)> reader{
      vx_velox_read_at_new(&callbacks, &error), vx_velox_read_at_free};
  if (reader == nullptr) {
    failVortex("create a Vortex reader", error);
  }
  context.release();

  std::unique_ptr<vx_velox_source, decltype(&vx_velox_source_free)> source{
      vx_velox_source_new(session.get(), reader.get(), &error),
      vx_velox_source_free};
  if (source == nullptr) {
    failVortex("open the Vortex file", error);
  }
  reader.reset();

  rowCount_ = vx_velox_source_row_count(source.get());
  auto type = typeFromVortexSource(source.get());
  VELOX_USER_CHECK(type->isRow(), "A Vortex file must contain a row schema");
  rowType_ = std::dynamic_pointer_cast<const RowType>(std::move(type));
  fileSize_ = vx_velox_source_file_size(source.get());
  const auto splitCount = vx_velox_source_natural_split_count(source.get());
  naturalSplits_.reserve(splitCount);
  for (size_t i = 0; i < splitCount; ++i) {
    vx_velox_natural_split split{
        .struct_size = sizeof(vx_velox_natural_split),
    };
    if (vx_velox_source_natural_split_at(source.get(), i, &split, &error) !=
        0) {
      failVortex("read Vortex split metadata", error);
    }
    naturalSplits_.emplace_back(split.row_begin, split.row_end);
  }
  session_ = session.release();
  source_ = source.release();
}

VortexFile::~VortexFile() {
  if (source_ != nullptr) {
    vx_velox_source_free(source_);
  }
  if (session_ != nullptr) {
    vx_velox_session_free(session_);
  }
}

uint64_t VortexFile::rowCount() const {
  return rowCount_;
}

const RowTypePtr& VortexFile::rowType() const {
  return rowType_;
}

uint64_t VortexFile::fileSize() const {
  return fileSize_;
}

const std::vector<std::pair<uint64_t, uint64_t>>& VortexFile::naturalSplits()
    const {
  return naturalSplits_;
}

const vx_session* VortexFile::session() const {
  return session_;
}

const vx_data_source* VortexFile::createDataSource() const {
  vx_error* error = nullptr;
  auto* dataSource = vx_velox_source_data_source(source_, &error);
  if (dataSource == nullptr) {
    failVortex("create a Vortex data source", error);
  }
  return dataSource;
}

std::vector<uint8_t> VortexFile::pruneNaturalSplits(
    const vx_expression* expression,
    size_t firstSplit,
    size_t splitCount) const {
  std::vector<uint8_t> decisions(splitCount);
  vx_error* error{nullptr};
  const auto status = vx_velox_source_prune_natural_splits(
      source_,
      expression,
      firstSplit,
      splitCount,
      decisions.empty() ? nullptr : decisions.data(),
      &error);
  if (status != 0) {
    failVortex("prune Vortex natural splits", error);
  }
  VELOX_CHECK_NULL(error);
  return decisions;
}

} // namespace facebook::velox::dwio::vortex
