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

#include "velox/dwio/vortex/VortexVector.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <limits>
#include <mutex>
#include <span>
#include <type_traits>
#include <unordered_map>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::dwio::vortex {
namespace {

static_assert(sizeof(vx_velox_binary_view) == sizeof(StringView));
static_assert(StringView::kInlineSize == 12);

[[noreturn]] void failVortex(std::string_view operation, vx_velox_error* error);

class VortexBufferLifetime {
 public:
  VortexBufferLifetime(
      const vx_velox_buffer_owner& owner,
      memory::MemoryPool& pool,
      bool memoryPrecharged = false)
      : owner_{owner},
        retainedBytes_{owner.retained_bytes},
        pool_{pool},
        memoryPrecharged_{memoryPrecharged} {
    VELOX_USER_CHECK_GE(
        owner_.struct_size,
        sizeof(vx_velox_buffer_owner),
        "Vortex buffer owner structure is too small: {}",
        owner_.struct_size);
    VELOX_USER_CHECK_NOT_NULL(
        owner_.retain, "Vortex buffer owner lacks a retain callback");
    VELOX_USER_CHECK_NOT_NULL(
        owner_.release, "Vortex buffer owner lacks a release callback");
    VELOX_USER_CHECK_NOT_NULL(
        owner_.owner, "Vortex buffer owner pointer must not be null");
    VELOX_USER_CHECK_LE(
        retainedBytes_,
        std::numeric_limits<int64_t>::max(),
        "Vortex retained buffer size exceeds the Velox memory limit: {}",
        retainedBytes_);
    if (retainedBytes_ != 0 && !memoryPrecharged_) {
      pool_.reportExternalAllocation(static_cast<int64_t>(retainedBytes_));
    }
    owner_.retain(owner_.owner);
  }

  ~VortexBufferLifetime() {
    if (retainedBytes_ != 0 && !memoryPrecharged_) {
      pool_.reportExternalFree(static_cast<int64_t>(retainedBytes_));
    }
    owner_.release(owner_.owner);
  }

  bool matches(const vx_velox_buffer_owner& owner) const {
    return owner_.owner == owner.owner && owner_.retain == owner.retain &&
        owner_.release == owner.release &&
        owner_.retained_bytes == owner.retained_bytes;
  }

 private:
  vx_velox_buffer_owner owner_;
  size_t retainedBytes_;
  memory::MemoryPool& pool_;
  bool memoryPrecharged_;
};

struct SharedBufferReleaser {
  void addRef() const {}
  void release() const {}

  std::shared_ptr<VortexBufferLifetime> lifetime;
};

struct PrimitiveCapture {
  const vx_velox_session* session;
  TypePtr targetType;
  memory::MemoryPool* pool;
  ValueHook* hook{nullptr};
  RowSet hookRows{};
  std::shared_ptr<VortexBufferLifetime>* sharedLifetime{nullptr};
  std::unordered_map<const vx_velox_export_cursor*, VectorPtr>* cursorCache{
      nullptr};
  bool memoryPrecharged{false};
  VectorPtr result;
  std::array<char, 1'024> error{};
};

int32_t capturePrimitive(
    void* rawContext,
    const vx_velox_primitive_view* view) noexcept;
int32_t captureVarBin(
    void* rawContext,
    const vx_velox_varbin_view* view) noexcept;
int32_t captureBool(void* rawContext, const vx_velox_bool_view* view) noexcept;
int32_t captureDictionary(
    void* rawContext,
    const vx_velox_dictionary_view* view) noexcept;
int32_t captureConstant(
    void* rawContext,
    const vx_velox_constant_view* view) noexcept;
int32_t captureStruct(
    void* rawContext,
    const vx_velox_struct_view* view) noexcept;
int32_t captureList(void* rawContext, const vx_velox_list_view* view) noexcept;
int32_t captureMap(void* rawContext, const vx_velox_map_view* view) noexcept;
const char* primitiveError(void* rawContext);

vx_velox_visitor makeVisitor(PrimitiveCapture& capture) {
  return vx_velox_visitor{
      .struct_size = sizeof(vx_velox_visitor),
      .abi_version = VX_VELOX_ABI_VERSION,
      .context = &capture,
      .visit_primitive = capturePrimitive,
      .last_error = primitiveError,
      .visit_varbin = captureVarBin,
      .visit_dictionary = captureDictionary,
      .visit_constant = captureConstant,
      .visit_bool = captureBool,
      .visit_struct = captureStruct,
      .visit_list = captureList,
      .visit_map = captureMap,
  };
}

struct RowIndexCapture {
  memory::MemoryPool* pool;
  std::optional<VortexRowPositions> result;
  std::array<char, 1'024> error{};
};

size_t checkedRowPositionBytes(size_t size) {
  VELOX_USER_CHECK_LE(
      size,
      std::numeric_limits<size_t>::max() / sizeof(uint64_t),
      "Vortex row-index count exceeds the supported byte size: {}",
      size);
  return size * sizeof(uint64_t);
}

size_t checkedBitmapBytes(size_t bitOffset, size_t length) {
  VELOX_USER_CHECK_LE(
      bitOffset,
      std::numeric_limits<size_t>::max() - length,
      "Vortex bitmap range overflows: {}",
      bitOffset);
  const auto endBit = bitOffset + length;
  VELOX_USER_CHECK_LE(
      endBit,
      std::numeric_limits<size_t>::max() - 7,
      "Vortex bitmap byte count overflows: {}",
      endBit);
  return bits::nbytes(endBit);
}

struct CallbackErrorState {
  const void* context{nullptr};
  std::array<char, 1'024> error{};
};

thread_local CallbackErrorState callbackErrorState;

void setCallbackError(const void* context, std::string_view message) noexcept {
  callbackErrorState.context = context;
  const auto length =
      std::min(message.size(), callbackErrorState.error.size() - 1);
  std::copy_n(message.data(), length, callbackErrorState.error.data());
  callbackErrorState.error[length] = '\0';
}

size_t primitiveWidth(vx_velox_primitive_type primitiveType) {
  switch (primitiveType) {
    case VX_VELOX_PRIMITIVE_U8:
    case VX_VELOX_PRIMITIVE_I8:
      return 1;
    case VX_VELOX_PRIMITIVE_U16:
    case VX_VELOX_PRIMITIVE_I16:
    case VX_VELOX_PRIMITIVE_F16:
      return 2;
    case VX_VELOX_PRIMITIVE_U32:
    case VX_VELOX_PRIMITIVE_I32:
    case VX_VELOX_PRIMITIVE_F32:
      return 4;
    case VX_VELOX_PRIMITIVE_U64:
    case VX_VELOX_PRIMITIVE_I64:
    case VX_VELOX_PRIMITIVE_F64:
      return 8;
    case VX_VELOX_PRIMITIVE_I128:
      return 16;
  }
  VELOX_UNREACHABLE();
}

vx_velox_primitive_type primitiveType(const TypePtr& type) {
  if (type->isShortDecimal()) {
    return VX_VELOX_PRIMITIVE_I64;
  }
  if (type->isLongDecimal()) {
    return VX_VELOX_PRIMITIVE_I128;
  }
  switch (type->kind()) {
    case TypeKind::TINYINT:
      return VX_VELOX_PRIMITIVE_I8;
    case TypeKind::SMALLINT:
      return VX_VELOX_PRIMITIVE_I16;
    case TypeKind::INTEGER:
      return VX_VELOX_PRIMITIVE_I32;
    case TypeKind::BIGINT:
      return VX_VELOX_PRIMITIVE_I64;
    case TypeKind::REAL:
      return VX_VELOX_PRIMITIVE_F32;
    case TypeKind::DOUBLE:
      return VX_VELOX_PRIMITIVE_F64;
    default:
      VELOX_USER_FAIL(
          "Vortex native import does not support type: {}", type->toString());
  }
}

template <typename View>
bool isNullAt(const View& view, vector_size_t row) {
  switch (view.validity_kind) {
    case VX_VELOX_VALIDITY_NON_NULLABLE:
    case VX_VELOX_VALIDITY_ALL_VALID:
      return false;
    case VX_VELOX_VALIDITY_ALL_INVALID:
      return true;
    case VX_VELOX_VALIDITY_BITMAP:
      return !bits::isBitSet(view.validity, view.validity_bit_offset + row);
  }
  VELOX_UNREACHABLE();
}

template <typename View>
void validateValidity(const View& view, vector_size_t length) {
  switch (view.validity_kind) {
    case VX_VELOX_VALIDITY_NON_NULLABLE:
    case VX_VELOX_VALIDITY_ALL_VALID:
    case VX_VELOX_VALIDITY_ALL_INVALID:
      return;
    case VX_VELOX_VALIDITY_BITMAP:
      break;
    default:
      VELOX_USER_FAIL(
          "Vortex validity kind is invalid: {}", view.validity_kind);
  }
  VELOX_USER_CHECK_NOT_NULL(
      view.validity, "Vortex validity bitmap must not be null");
  VELOX_USER_CHECK_GE(
      view.validity_alignment,
      alignof(uint64_t),
      "Vortex validity bitmap alignment is too small: {}",
      view.validity_alignment);
  VELOX_USER_CHECK_EQ(
      reinterpret_cast<uintptr_t>(view.validity) % alignof(uint64_t),
      0,
      "Vortex validity bitmap pointer is not aligned");
  VELOX_USER_CHECK_GE(
      view.validity_length,
      checkedBitmapBytes(view.validity_bit_offset, length),
      "Vortex validity bitmap is too small: {}",
      view.validity_length);
}

template <typename View>
BufferPtr importNulls(
    const View& view,
    const std::shared_ptr<VortexBufferLifetime>& lifetime,
    vector_size_t length,
    memory::MemoryPool& pool) {
  validateValidity(view, length);
  switch (view.validity_kind) {
    case VX_VELOX_VALIDITY_NON_NULLABLE:
    case VX_VELOX_VALIDITY_ALL_VALID:
      return nullptr;
    case VX_VELOX_VALIDITY_ALL_INVALID:
      return allocateNulls(length, &pool, bits::kNull);
    case VX_VELOX_VALIDITY_BITMAP:
      break;
    default:
      VELOX_UNREACHABLE();
  }
  if (view.validity_bit_offset == 0 &&
      view.validity_length >= bits::nwords(length) * sizeof(uint64_t) &&
      view.validity_alignment >= alignof(uint64_t) &&
      reinterpret_cast<uintptr_t>(view.validity) % alignof(uint64_t) == 0) {
    return BufferView<SharedBufferReleaser>::create(
        view.validity, bits::nbytes(length), SharedBufferReleaser{lifetime});
  }
  auto nulls = allocateNulls(length, &pool, bits::kNull);
  if (length != 0) {
    bits::copyBits(
        reinterpret_cast<const uint64_t*>(view.validity),
        view.validity_bit_offset,
        nulls->asMutable<uint64_t>(),
        0,
        length);
  }
  return nulls;
}

std::shared_ptr<VortexBufferLifetime> captureLifetime(
    PrimitiveCapture& capture,
    const vx_velox_buffer_owner& owner) {
  if (capture.hook != nullptr) {
    return nullptr;
  }
  if (capture.sharedLifetime == nullptr) {
    return std::make_shared<VortexBufferLifetime>(
        owner, *capture.pool, capture.memoryPrecharged);
  }
  if (*capture.sharedLifetime == nullptr) {
    *capture.sharedLifetime = std::make_shared<VortexBufferLifetime>(
        owner, *capture.pool, capture.memoryPrecharged);
  } else {
    VELOX_USER_CHECK(
        (*capture.sharedLifetime)->matches(owner),
        "Vortex export cursor changed its retained buffer owner");
  }
  return *capture.sharedLifetime;
}

template <typename T>
void captureTyped(
    PrimitiveCapture& capture,
    const vx_velox_primitive_view& view,
    const std::shared_ptr<VortexBufferLifetime>& lifetime) {
  const auto length = static_cast<vector_size_t>(view.length);
  const auto* values = reinterpret_cast<const T*>(view.values);
  if (capture.hook != nullptr) {
    if (capture.hookRows.data() != nullptr) {
      VELOX_CHECK_EQ(capture.hookRows.size(), length);
    }
    for (vector_size_t selectedRow = 0; selectedRow < length; ++selectedRow) {
      const auto hookRow = capture.hookRows.data() == nullptr
          ? selectedRow
          : capture.hookRows[selectedRow];
      if (isNullAt(view, selectedRow)) {
        if (capture.hook->acceptsNulls()) {
          capture.hook->addNull(hookRow);
        }
      } else {
        capture.hook->addValueTyped(hookRow, values[selectedRow]);
      }
    }
    return;
  }

  BufferPtr valueBuffer;
  if (length == 0) {
    valueBuffer = AlignedBuffer::allocate<T>(0, capture.pool);
  } else {
    VELOX_USER_CHECK_GE(
        view.values_alignment,
        alignof(T),
        "Vortex primitive value alignment is too small: {}",
        view.values_alignment);
    VELOX_USER_CHECK_EQ(
        reinterpret_cast<uintptr_t>(view.values) % alignof(T),
        0,
        "Vortex primitive value pointer is not aligned for its type");
    valueBuffer = BufferView<SharedBufferReleaser>::create(
        view.values, sizeof(T) * view.length, SharedBufferReleaser{lifetime});
  }
  auto nulls = importNulls(view, lifetime, length, *capture.pool);
  capture.result = std::make_shared<FlatVector<T>>(
      capture.pool,
      capture.targetType,
      std::move(nulls),
      length,
      std::move(valueBuffer),
      std::vector<BufferPtr>{});
}

int32_t capturePrimitive(
    void* rawContext,
    const vx_velox_primitive_view* view) noexcept {
  auto* capture = static_cast<PrimitiveCapture*>(rawContext);
  try {
    VELOX_USER_CHECK_NOT_NULL(view, "Vortex primitive view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_primitive_view),
        "Vortex primitive view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex primitive view exceeds the Velox vector limit: {}",
        view->length);
    VELOX_USER_CHECK_EQ(
        view->primitive_type,
        primitiveType(capture->targetType),
        "Vortex primitive type does not match the requested Velox type");
    if (capture->targetType->isDecimal()) {
      const auto [precision, scale] =
          getDecimalPrecisionScale(*capture->targetType);
      VELOX_USER_CHECK_EQ(
          view->decimal_precision,
          precision,
          "Vortex decimal precision does not match the requested Velox type");
      VELOX_USER_CHECK_EQ(
          view->decimal_scale,
          scale,
          "Vortex decimal scale does not match the requested Velox type");
    } else {
      VELOX_USER_CHECK_EQ(
          view->decimal_precision,
          0,
          "Vortex non-decimal primitive reported decimal precision: {}",
          view->decimal_precision);
      VELOX_USER_CHECK_EQ(
          view->decimal_scale,
          0,
          "Vortex non-decimal primitive reported decimal scale: {}",
          view->decimal_scale);
    }
    const auto valueBytes = primitiveWidth(view->primitive_type) * view->length;
    VELOX_USER_CHECK_GE(
        view->values_length,
        valueBytes,
        "Vortex primitive value buffer is too small: {}",
        view->values_length);
    VELOX_USER_CHECK(
        valueBytes == 0 || view->values != nullptr,
        "Vortex primitive value buffer must not be null");
    VELOX_USER_CHECK(
        valueBytes == 0 || view->values_alignment >= valueBytes / view->length,
        "Vortex primitive value alignment is too small: {}",
        view->values_alignment);
    VELOX_USER_CHECK(
        valueBytes == 0 ||
            reinterpret_cast<uintptr_t>(view->values) %
                    (valueBytes / view->length) ==
                0,
        "Vortex primitive value pointer is not aligned for its type");
    validateValidity(*view, static_cast<vector_size_t>(view->length));
    auto lifetime = captureLifetime(*capture, view->buffers);
    switch (view->primitive_type) {
      case VX_VELOX_PRIMITIVE_I8:
        captureTyped<int8_t>(*capture, *view, lifetime);
        break;
      case VX_VELOX_PRIMITIVE_I16:
        captureTyped<int16_t>(*capture, *view, lifetime);
        break;
      case VX_VELOX_PRIMITIVE_I32:
        captureTyped<int32_t>(*capture, *view, lifetime);
        break;
      case VX_VELOX_PRIMITIVE_I64:
        captureTyped<int64_t>(*capture, *view, lifetime);
        break;
      case VX_VELOX_PRIMITIVE_F32:
        captureTyped<float>(*capture, *view, lifetime);
        break;
      case VX_VELOX_PRIMITIVE_F64:
        captureTyped<double>(*capture, *view, lifetime);
        break;
      case VX_VELOX_PRIMITIVE_I128:
        captureTyped<int128_t>(*capture, *view, lifetime);
        break;
      default:
        VELOX_USER_FAIL("Unsupported Vortex primitive type");
    }
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{
        "Unknown Vortex primitive callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

const char* primitiveError(void* rawContext) {
  return static_cast<PrimitiveCapture*>(rawContext)->error.data();
}

int32_t captureVarBin(
    void* rawContext,
    const vx_velox_varbin_view* view) noexcept {
  auto* capture = static_cast<PrimitiveCapture*>(rawContext);
  try {
    VELOX_USER_CHECK_NOT_NULL(
        view, "Vortex variable-width view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_varbin_view),
        "Vortex variable-width view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex variable-width view exceeds the Velox vector limit: {}",
        view->length);
    VELOX_USER_CHECK(
        capture->targetType->kind() == TypeKind::VARCHAR ||
            capture->targetType->kind() == TypeKind::VARBINARY,
        "Vortex variable-width data requires VARCHAR or VARBINARY output");
    const auto expectedKind = capture->targetType->kind() == TypeKind::VARCHAR
        ? VX_VELOX_VARBIN_UTF8
        : VX_VELOX_VARBIN_BINARY;
    VELOX_USER_CHECK_EQ(
        view->kind,
        expectedKind,
        "Vortex variable-width type does not match the requested Velox type");
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<size_t>::max() / sizeof(vx_velox_binary_view),
        "Vortex variable-width view byte count overflows: {}",
        view->length);
    const auto requiredViewBytes = view->length * sizeof(vx_velox_binary_view);
    VELOX_USER_CHECK_GE(
        view->views_length,
        requiredViewBytes,
        "Vortex variable-width view buffer is too small: {}",
        view->views_length);
    VELOX_USER_CHECK(
        requiredViewBytes == 0 || view->views != nullptr,
        "Vortex variable-width view buffer must not be null");
    VELOX_USER_CHECK(
        requiredViewBytes == 0 ||
            view->views_alignment >= alignof(vx_velox_binary_view),
        "Vortex variable-width view alignment is too small: {}",
        view->views_alignment);
    VELOX_USER_CHECK(
        requiredViewBytes == 0 ||
            reinterpret_cast<uintptr_t>(view->views) %
                    alignof(vx_velox_binary_view) ==
                0,
        "Vortex variable-width view pointer is not aligned");
    VELOX_USER_CHECK(
        view->data_buffer_count == 0 || view->data_buffers != nullptr,
        "Vortex string buffer descriptors must not be null");
    validateValidity(*view, static_cast<vector_size_t>(view->length));

    auto lifetime = captureLifetime(*capture, view->buffers);
    const auto outlinedData = [&](const vx_velox_binary_view& binaryView) {
      const auto size = binaryView.length;
      uint32_t bufferIndex;
      uint32_t offset;
      std::memcpy(&bufferIndex, binaryView.data + 4, sizeof(bufferIndex));
      std::memcpy(&offset, binaryView.data + 8, sizeof(offset));
      VELOX_USER_CHECK_LT(
          bufferIndex,
          view->data_buffer_count,
          "Vortex string buffer index is out of range: {}",
          bufferIndex);
      const auto& buffer = view->data_buffers[bufferIndex];
      VELOX_USER_CHECK(
          buffer.length == 0 || buffer.data != nullptr,
          "Vortex string payload buffer must not be null");
      VELOX_USER_CHECK_LE(
          offset,
          buffer.length,
          "Vortex string offset is out of range: {}",
          offset);
      VELOX_USER_CHECK_LE(
          size,
          buffer.length - offset,
          "Vortex string length is out of range: {}",
          size);
      return reinterpret_cast<const char*>(buffer.data + offset);
    };
    const auto valueAt = [&](vector_size_t row) {
      const auto& binaryView = view->views[row];
      const auto size = binaryView.length;
      VELOX_USER_CHECK_LE(
          size,
          std::numeric_limits<int32_t>::max(),
          "Vortex string length exceeds the Velox StringView limit: {}",
          size);
      const auto* data = size <= StringView::kInlineSize
          ? reinterpret_cast<const char*>(binaryView.data)
          : outlinedData(binaryView);
      return StringView{data, static_cast<int32_t>(size)};
    };

    const auto length = static_cast<vector_size_t>(view->length);
    if (capture->hook != nullptr) {
      if (capture->hookRows.data() != nullptr) {
        VELOX_CHECK_EQ(capture->hookRows.size(), length);
      }
      for (vector_size_t selectedRow = 0; selectedRow < length; ++selectedRow) {
        const auto hookRow = capture->hookRows.data() == nullptr
            ? selectedRow
            : capture->hookRows[selectedRow];
        if (isNullAt(*view, selectedRow)) {
          if (capture->hook->acceptsNulls()) {
            capture->hook->addNull(hookRow);
          }
        } else {
          capture->hook->addValueTyped(hookRow, valueAt(selectedRow));
        }
      }
      return 0;
    }

    auto values = AlignedBuffer::allocate<StringView>(length, capture->pool);
    auto* rawValues = values->asMutable<uint64_t>();
    for (vector_size_t row = 0; row < length; ++row) {
      if (isNullAt(*view, row)) {
        rawValues[2 * row] = 0;
        rawValues[2 * row + 1] = 0;
        continue;
      }
      const auto& binaryView = view->views[row];
      const auto size = binaryView.length;
      VELOX_USER_CHECK_LE(
          size,
          std::numeric_limits<int32_t>::max(),
          "Vortex string length exceeds the Velox StringView limit: {}",
          size);
      std::memcpy(rawValues + 2 * row, &binaryView, sizeof(uint64_t));
      if (size <= StringView::kInlineSize) {
        std::memcpy(
            rawValues + 2 * row + 1,
            reinterpret_cast<const uint8_t*>(&binaryView) + sizeof(uint64_t),
            sizeof(uint64_t));
      } else {
        rawValues[2 * row + 1] =
            reinterpret_cast<uint64_t>(outlinedData(binaryView));
      }
    }
    std::vector<BufferPtr> stringBuffers;
    stringBuffers.reserve(view->data_buffer_count);
    for (size_t index = 0; index < view->data_buffer_count; ++index) {
      const auto& buffer = view->data_buffers[index];
      VELOX_USER_CHECK(
          buffer.length == 0 || buffer.data != nullptr,
          "Vortex string payload buffer must not be null");
      if (buffer.length != 0) {
        stringBuffers.push_back(
            BufferView<SharedBufferReleaser>::create(
                buffer.data, buffer.length, SharedBufferReleaser{lifetime}));
      }
    }
    auto nulls = importNulls(*view, lifetime, length, *capture->pool);
    capture->result = std::make_shared<FlatVector<StringView>>(
        capture->pool,
        capture->targetType,
        std::move(nulls),
        length,
        std::move(values),
        std::move(stringBuffers));
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{
        "Unknown Vortex variable-width callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

int32_t captureBool(void* rawContext, const vx_velox_bool_view* view) noexcept {
  auto* capture = static_cast<PrimitiveCapture*>(rawContext);
  try {
    VELOX_USER_CHECK_NOT_NULL(view, "Vortex Boolean view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_bool_view),
        "Vortex Boolean view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_EQ(
        static_cast<int32_t>(capture->targetType->kind()),
        static_cast<int32_t>(TypeKind::BOOLEAN),
        "Vortex Boolean data does not match the requested Velox type");
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex Boolean view exceeds the Velox vector limit: {}",
        view->length);
    const auto length = static_cast<vector_size_t>(view->length);
    VELOX_USER_CHECK_GE(
        view->values_length,
        checkedBitmapBytes(view->values_bit_offset, length),
        "Vortex Boolean value buffer is too small: {}",
        view->values_length);
    VELOX_USER_CHECK(
        length == 0 || view->values != nullptr,
        "Vortex Boolean value buffer must not be null");
    if (length != 0) {
      VELOX_USER_CHECK_GE(
          view->values_alignment,
          alignof(uint64_t),
          "Vortex Boolean value alignment is too small: {}",
          view->values_alignment);
      VELOX_USER_CHECK_EQ(
          reinterpret_cast<uintptr_t>(view->values) % alignof(uint64_t),
          0,
          "Vortex Boolean value pointer is not aligned");
    }
    validateValidity(*view, length);
    auto lifetime = captureLifetime(*capture, view->buffers);

    if (capture->hook != nullptr) {
      if (capture->hookRows.data() != nullptr) {
        VELOX_CHECK_EQ(capture->hookRows.size(), length);
      }
      for (vector_size_t selectedRow = 0; selectedRow < length; ++selectedRow) {
        const auto hookRow = capture->hookRows.data() == nullptr
            ? selectedRow
            : capture->hookRows[selectedRow];
        if (isNullAt(*view, selectedRow)) {
          if (capture->hook->acceptsNulls()) {
            capture->hook->addNull(hookRow);
          }
        } else {
          capture->hook->addValueTyped(
              hookRow,
              bits::isBitSet(
                  view->values, view->values_bit_offset + selectedRow));
        }
      }
      return 0;
    }

    BufferPtr values;
    if (length == 0) {
      values = AlignedBuffer::allocate<bool>(0, capture->pool);
    } else if (
        view->values_bit_offset == 0 &&
        view->values_length >= bits::nwords(length) * sizeof(uint64_t) &&
        view->values_alignment >= alignof(uint64_t) &&
        reinterpret_cast<uintptr_t>(view->values) % alignof(uint64_t) == 0) {
      values = BufferView<SharedBufferReleaser>::create(
          view->values, bits::nbytes(length), SharedBufferReleaser{lifetime});
    } else {
      values = AlignedBuffer::allocate<bool>(length, capture->pool, false);
      bits::copyBits(
          reinterpret_cast<const uint64_t*>(view->values),
          view->values_bit_offset,
          values->asMutable<uint64_t>(),
          0,
          length);
    }
    auto nulls = importNulls(*view, lifetime, length, *capture->pool);
    capture->result = std::make_shared<FlatVector<bool>>(
        capture->pool,
        capture->targetType,
        std::move(nulls),
        length,
        std::move(values),
        std::vector<BufferPtr>{});
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{"Unknown Vortex Boolean callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

VectorPtr importPreparedCursor(
    PrimitiveCapture& parent,
    const vx_velox_export_cursor* cursor,
    size_t offset,
    size_t length,
    const TypePtr& targetType) {
  VELOX_USER_CHECK_NOT_NULL(cursor, "Vortex child cursor must not be null");
  VELOX_USER_CHECK_NOT_NULL(
      parent.session, "Vortex child cursor requires a session");
  VELOX_USER_CHECK_LE(
      length,
      std::numeric_limits<vector_size_t>::max(),
      "Vortex child cursor exceeds the Velox vector limit: {}",
      length);
  PrimitiveCapture child{
      .session = parent.session,
      .targetType = targetType,
      .pool = parent.pool,
      .cursorCache = parent.cursorCache,
      .memoryPrecharged = parent.memoryPrecharged,
  };
  const auto visitor = makeVisitor(child);
  vx_velox_error* error{nullptr};
  if (vx_velox_export_cursor_visit(cursor, offset, length, &visitor, &error) !=
      0) {
    failVortex("visit a prepared Vortex child array", error);
  }
  VELOX_CHECK_NOT_NULL(child.result);
  return child.result;
}

VectorPtr importCachedCursor(
    PrimitiveCapture& capture,
    const vx_velox_export_cursor* cursor,
    size_t length,
    const TypePtr& targetType) {
  if (capture.cursorCache != nullptr) {
    const auto cached = capture.cursorCache->find(cursor);
    if (cached == capture.cursorCache->end()) {
      auto result =
          importPreparedCursor(capture, cursor, 0, length, targetType);
      capture.cursorCache->emplace(cursor, result);
      return result;
    }
    VELOX_USER_CHECK_EQ(
        cached->second->size(), length, "Vortex cached child changed its size");
    VELOX_USER_CHECK(
        cached->second->type()->equivalent(*targetType),
        "Vortex cached child changed its type");
    return cached->second;
  }
  return importPreparedCursor(capture, cursor, 0, length, targetType);
}

BufferPtr copyDictionaryNulls(
    const vx_velox_primitive_view& codes,
    vector_size_t length,
    memory::MemoryPool& pool) {
  switch (codes.validity_kind) {
    case VX_VELOX_VALIDITY_NON_NULLABLE:
    case VX_VELOX_VALIDITY_ALL_VALID:
      return nullptr;
    case VX_VELOX_VALIDITY_ALL_INVALID:
      return allocateNulls(length, &pool, bits::kNull);
    case VX_VELOX_VALIDITY_BITMAP:
      break;
  }
  validateValidity(codes, length);
  auto nulls = allocateNulls(length, &pool, bits::kNull);
  if (length != 0) {
    bits::copyBits(
        reinterpret_cast<const uint64_t*>(codes.validity),
        codes.validity_bit_offset,
        nulls->asMutable<uint64_t>(),
        0,
        length);
  }
  return nulls;
}

template <typename Code>
void copyDictionaryIndices(
    const vx_velox_primitive_view& codes,
    vector_size_t length,
    vector_size_t baseSize,
    vector_size_t* indices) {
  const auto* values = reinterpret_cast<const Code*>(codes.values);
  for (vector_size_t row = 0; row < length; ++row) {
    if (isNullAt(codes, row)) {
      indices[row] = 0;
      continue;
    }
    const auto code = values[row];
    if constexpr (std::is_signed_v<Code>) {
      VELOX_USER_CHECK_GE(
          code, 0, "Vortex dictionary code must not be negative: {}", code);
    }
    using UnsignedCode = std::make_unsigned_t<Code>;
    const auto unsignedCode = static_cast<UnsignedCode>(code);
    VELOX_USER_CHECK_LT(
        unsignedCode,
        static_cast<uint64_t>(baseSize),
        "Vortex dictionary code exceeds the value count: {}",
        unsignedCode);
    indices[row] = static_cast<vector_size_t>(unsignedCode);
  }
}

void fillDictionaryIndices(
    const vx_velox_primitive_view& codes,
    vector_size_t length,
    vector_size_t baseSize,
    vector_size_t* indices) {
  VELOX_USER_CHECK_GE(
      codes.struct_size,
      sizeof(vx_velox_primitive_view),
      "Vortex dictionary code view structure is too small: {}",
      codes.struct_size);
  VELOX_USER_CHECK_EQ(
      codes.length,
      length,
      "Vortex dictionary code count does not match the output size");
  const auto width = primitiveWidth(codes.primitive_type);
  VELOX_USER_CHECK_LE(
      codes.length,
      std::numeric_limits<size_t>::max() / width,
      "Vortex dictionary code byte count overflows: {}",
      codes.length);
  VELOX_USER_CHECK_GE(
      codes.values_length,
      width * codes.length,
      "Vortex dictionary code buffer is too small: {}",
      codes.values_length);
  VELOX_USER_CHECK(
      codes.values == nullptr ||
          reinterpret_cast<uintptr_t>(codes.values) % width == 0,
      "Vortex dictionary code buffer is not aligned to {} bytes",
      width);
  VELOX_USER_CHECK(
      length == 0 || codes.values != nullptr,
      "Vortex dictionary code buffer must not be null");
  VELOX_USER_CHECK(
      length == 0 || codes.values_alignment >= width,
      "Vortex dictionary code alignment is too small: {}",
      codes.values_alignment);
  validateValidity(codes, length);
  switch (codes.primitive_type) {
    case VX_VELOX_PRIMITIVE_U8:
      copyDictionaryIndices<uint8_t>(codes, length, baseSize, indices);
      return;
    case VX_VELOX_PRIMITIVE_U16:
      copyDictionaryIndices<uint16_t>(codes, length, baseSize, indices);
      return;
    case VX_VELOX_PRIMITIVE_U32:
      copyDictionaryIndices<uint32_t>(codes, length, baseSize, indices);
      return;
    case VX_VELOX_PRIMITIVE_U64:
      copyDictionaryIndices<uint64_t>(codes, length, baseSize, indices);
      return;
    case VX_VELOX_PRIMITIVE_I8:
      copyDictionaryIndices<int8_t>(codes, length, baseSize, indices);
      return;
    case VX_VELOX_PRIMITIVE_I16:
      copyDictionaryIndices<int16_t>(codes, length, baseSize, indices);
      return;
    case VX_VELOX_PRIMITIVE_I32:
      copyDictionaryIndices<int32_t>(codes, length, baseSize, indices);
      return;
    case VX_VELOX_PRIMITIVE_I64:
      copyDictionaryIndices<int64_t>(codes, length, baseSize, indices);
      return;
    default:
      VELOX_USER_FAIL("Vortex dictionary codes must use an integer type");
  }
}

int32_t captureDictionary(
    void* rawContext,
    const vx_velox_dictionary_view* view) noexcept {
  auto* capture = static_cast<PrimitiveCapture*>(rawContext);
  try {
    VELOX_USER_CHECK(
        capture->hook == nullptr,
        "Vortex dictionary callbacks do not support value hooks");
    VELOX_USER_CHECK_NOT_NULL(view, "Vortex dictionary view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_dictionary_view),
        "Vortex dictionary view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex dictionary view exceeds the Velox vector limit: {}",
        view->length);
    VELOX_USER_CHECK_LE(
        view->values_length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex dictionary values exceed the Velox vector limit: {}",
        view->values_length);
    auto base = importCachedCursor(
        *capture, view->values, view->values_length, capture->targetType);
    const auto length = static_cast<vector_size_t>(view->length);
    auto indices = allocateIndices(length, capture->pool);
    fillDictionaryIndices(
        view->codes, length, base->size(), indices->asMutable<vector_size_t>());
    auto nulls = copyDictionaryNulls(view->codes, length, *capture->pool);
    capture->result = BaseVector::wrapInDictionary(
        std::move(nulls), std::move(indices), length, std::move(base));
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{
        "Unknown Vortex dictionary callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

int32_t captureConstant(
    void* rawContext,
    const vx_velox_constant_view* view) noexcept {
  auto* capture = static_cast<PrimitiveCapture*>(rawContext);
  try {
    VELOX_USER_CHECK(
        capture->hook == nullptr,
        "Vortex constant callbacks do not support value hooks");
    VELOX_USER_CHECK_NOT_NULL(view, "Vortex constant view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_constant_view),
        "Vortex constant view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex constant view exceeds the Velox vector limit: {}",
        view->length);
    auto value =
        importCachedCursor(*capture, view->value, 1, capture->targetType);
    capture->result =
        BaseVector::wrapInConstant(view->length, 0, std::move(value));
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{"Unknown Vortex constant callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

int32_t captureStruct(
    void* rawContext,
    const vx_velox_struct_view* view) noexcept {
  auto* capture = static_cast<PrimitiveCapture*>(rawContext);
  try {
    VELOX_USER_CHECK(
        capture->hook == nullptr,
        "Vortex struct callbacks do not support value hooks");
    VELOX_USER_CHECK_NOT_NULL(view, "Vortex struct view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_struct_view),
        "Vortex struct view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_EQ(
        capture->targetType->kind(),
        TypeKind::ROW,
        "Vortex struct data does not match the requested Velox type");
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex struct view exceeds the Velox vector limit: {}",
        view->length);
    VELOX_USER_CHECK_EQ(
        view->field_count,
        capture->targetType->size(),
        "Vortex struct field count does not match the requested Velox type");
    VELOX_USER_CHECK(
        view->field_count == 0 || view->fields != nullptr,
        "Vortex struct field cursors must not be null");
    const auto length = static_cast<vector_size_t>(view->length);
    validateValidity(*view, length);
    auto lifetime = captureLifetime(*capture, view->buffers);
    auto nulls = importNulls(*view, lifetime, length, *capture->pool);
    std::vector<VectorPtr> children;
    children.reserve(view->field_count);
    for (size_t field = 0; field < view->field_count; ++field) {
      children.push_back(importPreparedCursor(
          *capture,
          view->fields[field],
          view->offset,
          view->length,
          capture->targetType->childAt(field)));
    }
    capture->result = std::make_shared<RowVector>(
        capture->pool,
        capture->targetType,
        std::move(nulls),
        length,
        std::move(children));
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{"Unknown Vortex struct callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

struct ImportedCollectionMetadata {
  vector_size_t length;
  BufferPtr nulls;
  BufferPtr offsets;
  BufferPtr sizes;
};

template <typename View>
ImportedCollectionMetadata importCollectionMetadata(
    PrimitiveCapture& capture,
    const View& view,
    size_t entriesLength) {
  VELOX_USER_CHECK_LE(
      entriesLength,
      std::numeric_limits<vector_size_t>::max(),
      "Vortex collection entries exceed the Velox vector limit: {}",
      entriesLength);
  const auto length = static_cast<vector_size_t>(view.length);
  VELOX_USER_CHECK(
      length == 0 || view.offsets != nullptr,
      "Vortex collection offsets must not be null");
  VELOX_USER_CHECK(
      length == 0 || view.sizes != nullptr,
      "Vortex collection sizes must not be null");
  VELOX_USER_CHECK(
      length == 0 || view.offsets_alignment >= alignof(vector_size_t),
      "Vortex collection offset alignment is too small: {}",
      view.offsets_alignment);
  VELOX_USER_CHECK(
      length == 0 || view.sizes_alignment >= alignof(vector_size_t),
      "Vortex collection size alignment is too small: {}",
      view.sizes_alignment);
  VELOX_USER_CHECK(
      length == 0 ||
          reinterpret_cast<uintptr_t>(view.offsets) % alignof(vector_size_t) ==
              0,
      "Vortex collection offset pointer is not aligned");
  VELOX_USER_CHECK(
      length == 0 ||
          reinterpret_cast<uintptr_t>(view.sizes) % alignof(vector_size_t) == 0,
      "Vortex collection size pointer is not aligned");
  validateValidity(view, length);
  auto lifetime = captureLifetime(capture, view.buffers);
  auto nulls = importNulls(view, lifetime, length, *capture.pool);
  auto offsets = length == 0
      ? AlignedBuffer::allocate<vector_size_t>(0, capture.pool)
      : BufferView<SharedBufferReleaser>::create(
            reinterpret_cast<const uint8_t*>(view.offsets),
            length * sizeof(vector_size_t),
            SharedBufferReleaser{lifetime});
  auto sizes = length == 0
      ? AlignedBuffer::allocate<vector_size_t>(0, capture.pool)
      : BufferView<SharedBufferReleaser>::create(
            reinterpret_cast<const uint8_t*>(view.sizes),
            length * sizeof(vector_size_t),
            SharedBufferReleaser{lifetime});
  const auto checkedEntriesLength = static_cast<vector_size_t>(entriesLength);
  for (vector_size_t row = 0; row < length; ++row) {
    VELOX_USER_CHECK_GE(
        view.offsets[row],
        0,
        "Vortex collection offset must not be negative: {}",
        view.offsets[row]);
    VELOX_USER_CHECK_GE(
        view.sizes[row],
        0,
        "Vortex collection size must not be negative: {}",
        view.sizes[row]);
    VELOX_USER_CHECK_LE(
        view.offsets[row],
        checkedEntriesLength,
        "Vortex collection offset exceeds the entry count: {}",
        view.offsets[row]);
    VELOX_USER_CHECK_LE(
        view.sizes[row],
        checkedEntriesLength - view.offsets[row],
        "Vortex collection size exceeds the remaining entry count: {}",
        view.sizes[row]);
  }
  return {length, std::move(nulls), std::move(offsets), std::move(sizes)};
}

int32_t captureList(void* rawContext, const vx_velox_list_view* view) noexcept {
  auto* capture = static_cast<PrimitiveCapture*>(rawContext);
  try {
    VELOX_USER_CHECK(
        capture->hook == nullptr,
        "Vortex list callbacks do not support value hooks");
    VELOX_USER_CHECK_NOT_NULL(view, "Vortex list view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_list_view),
        "Vortex list view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_EQ(
        capture->targetType->kind(),
        TypeKind::ARRAY,
        "Vortex list data does not match the requested Velox type");
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex list view exceeds the Velox vector limit: {}",
        view->length);
    VELOX_USER_CHECK_NOT_NULL(
        view->elements, "Vortex list element cursor must not be null");
    auto metadata =
        importCollectionMetadata(*capture, *view, view->elements_length);
    auto elements = importCachedCursor(
        *capture,
        view->elements,
        view->elements_length,
        capture->targetType->childAt(0));
    capture->result = std::make_shared<ArrayVector>(
        capture->pool,
        capture->targetType,
        std::move(metadata.nulls),
        metadata.length,
        std::move(metadata.offsets),
        std::move(metadata.sizes),
        std::move(elements));
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{"Unknown Vortex list callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

int32_t captureMap(void* rawContext, const vx_velox_map_view* view) noexcept {
  auto* capture = static_cast<PrimitiveCapture*>(rawContext);
  try {
    VELOX_USER_CHECK(
        capture->hook == nullptr,
        "Vortex map callbacks do not support value hooks");
    VELOX_USER_CHECK_NOT_NULL(view, "Vortex map view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_map_view),
        "Vortex map view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_EQ(
        capture->targetType->kind(),
        TypeKind::MAP,
        "Vortex map data does not match the requested Velox type");
    VELOX_USER_CHECK_LE(
        view->length,
        std::numeric_limits<vector_size_t>::max(),
        "Vortex map view exceeds the Velox vector limit: {}",
        view->length);
    VELOX_USER_CHECK_NOT_NULL(
        view->keys, "Vortex map key cursor must not be null");
    VELOX_USER_CHECK_NOT_NULL(
        view->values, "Vortex map value cursor must not be null");
    auto metadata =
        importCollectionMetadata(*capture, *view, view->entries_length);
    auto keys = importCachedCursor(
        *capture,
        view->keys,
        view->entries_length,
        capture->targetType->childAt(0));
    auto values = importCachedCursor(
        *capture,
        view->values,
        view->entries_length,
        capture->targetType->childAt(1));
    capture->result = std::make_shared<MapVector>(
        capture->pool,
        capture->targetType,
        std::move(metadata.nulls),
        metadata.length,
        std::move(metadata.offsets),
        std::move(metadata.sizes),
        std::move(keys),
        std::move(values),
        std::nullopt,
        view->keys_sorted);
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{"Unknown Vortex map callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

int32_t captureRowIndices(
    void* rawContext,
    const vx_velox_primitive_view* view) noexcept {
  auto* capture = static_cast<RowIndexCapture*>(rawContext);
  try {
    VELOX_USER_CHECK_NOT_NULL(view, "Vortex row-index view must not be null");
    VELOX_USER_CHECK_GE(
        view->struct_size,
        sizeof(vx_velox_primitive_view),
        "Vortex row-index view structure is too small: {}",
        view->struct_size);
    VELOX_USER_CHECK_EQ(
        view->primitive_type,
        VX_VELOX_PRIMITIVE_U64,
        "Vortex row indexes must use U64 values");
    VELOX_USER_CHECK(
        view->validity_kind == VX_VELOX_VALIDITY_NON_NULLABLE ||
            view->validity_kind == VX_VELOX_VALIDITY_ALL_VALID,
        "Vortex row indexes must not contain nulls");
    VELOX_USER_CHECK_LE(
        view->length,
        static_cast<size_t>(std::numeric_limits<vector_size_t>::max()),
        "Vortex row-index count exceeds the Velox vector limit: {}",
        view->length);
    const auto valuesLength = checkedRowPositionBytes(view->length);
    VELOX_USER_CHECK_GE(
        view->values_length,
        valuesLength,
        "Vortex row-index value buffer is too small: {}",
        view->values_length);
    VELOX_USER_CHECK(
        view->length == 0 || view->values != nullptr,
        "Vortex row-index values must not be null");
    VELOX_USER_CHECK(
        view->length == 0 || view->values_alignment >= alignof(uint64_t),
        "Vortex row-index alignment is too small: {}",
        view->values_alignment);
    VELOX_USER_CHECK(
        view->length == 0 ||
            reinterpret_cast<uintptr_t>(view->values) % alignof(uint64_t) == 0,
        "Vortex row-index values are not aligned");
    auto lifetime =
        std::make_shared<VortexBufferLifetime>(view->buffers, *capture->pool);
    auto values = view->length == 0
        ? AlignedBuffer::allocate<uint64_t>(0, capture->pool)
        : BufferView<SharedBufferReleaser>::create(
              view->values,
              valuesLength,
              SharedBufferReleaser{std::move(lifetime)});
    capture->result.emplace(std::move(values), view->length);
    return 0;
  } catch (const std::exception& error) {
    const auto message = std::string_view{error.what()};
    const auto length = std::min(message.size(), capture->error.size() - 1);
    std::copy_n(message.data(), length, capture->error.data());
    capture->error[length] = '\0';
    return 1;
  } catch (...) {
    constexpr std::string_view kError{
        "Unknown Vortex row-index callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

const char* rowIndexError(void* rawContext) {
  return static_cast<RowIndexCapture*>(rawContext)->error.data();
}

std::string errorMessage(const vx_velox_error* error) {
  if (error == nullptr) {
    return "Vortex returned an unspecified error";
  }
  const auto message = vx_velox_error_message(error);
  return std::string{message.ptr, message.len};
}

[[noreturn]] void failVortex(
    std::string_view operation,
    vx_velox_error* error) {
  const auto errorText = errorMessage(error);
  vx_velox_error_free(error);
  VELOX_USER_FAIL("Failed to {}: {}", operation, errorText);
}

std::vector<uint64_t> visitorRows(RowSet rows, size_t arraySize) {
  if (rows.data() == nullptr) {
    return {};
  }
  if (rows.size() == arraySize) {
    bool identity{true};
    for (vector_size_t row = 0; row < rows.size(); ++row) {
      if (rows[row] != row) {
        identity = false;
        break;
      }
    }
    if (identity) {
      return {};
    }
  }
  return std::vector<uint64_t>(rows.begin(), rows.end());
}

VectorPtr importArrow(
    const vx_velox_session* session,
    const VortexArray& array,
    memory::MemoryPool& pool) {
  ArrowSchema schema{};
  ArrowArray arrowArray{};
  VortexArrowMemory memory{pool};
  vx_velox_error* error{nullptr};
  const auto status = vx_velox_array_export_arrow(
      session, array.get(), &memory.callbacks(), &schema, &arrowArray, &error);
  if (status != 0) {
    failVortex("export a Vortex array to Arrow", error);
  }
  return importFromArrowAsOwner(schema, arrowArray, &pool);
}

template <typename To>
VectorPtr castNumericVector(const VectorPtr& input, const TypePtr& targetType) {
  DecodedVector decoded{*input};
  auto output = BaseVector::create<FlatVector<To>>(
      targetType, input->size(), input->pool());
  for (vector_size_t row = 0; row < input->size(); ++row) {
    if (decoded.isNullAt(row)) {
      output->setNull(row, true);
      continue;
    }
    switch (input->typeKind()) {
      case TypeKind::TINYINT:
        output->set(row, static_cast<To>(decoded.valueAt<int8_t>(row)));
        break;
      case TypeKind::SMALLINT:
        output->set(row, static_cast<To>(decoded.valueAt<int16_t>(row)));
        break;
      case TypeKind::INTEGER:
        output->set(row, static_cast<To>(decoded.valueAt<int32_t>(row)));
        break;
      case TypeKind::BIGINT:
        output->set(row, static_cast<To>(decoded.valueAt<int64_t>(row)));
        break;
      case TypeKind::REAL:
        output->set(row, static_cast<To>(decoded.valueAt<float>(row)));
        break;
      default:
        VELOX_USER_FAIL(
            "Unsupported Vortex schema conversion: {} to {}",
            input->type()->toString(),
            targetType->toString());
    }
  }
  return output;
}

bool isNumericWidening(TypeKind source, TypeKind target) {
  switch (source) {
    case TypeKind::TINYINT:
      return target == TypeKind::SMALLINT || target == TypeKind::INTEGER ||
          target == TypeKind::BIGINT || target == TypeKind::REAL ||
          target == TypeKind::DOUBLE;
    case TypeKind::SMALLINT:
      return target == TypeKind::INTEGER || target == TypeKind::BIGINT ||
          target == TypeKind::REAL || target == TypeKind::DOUBLE;
    case TypeKind::INTEGER:
      return target == TypeKind::BIGINT || target == TypeKind::REAL ||
          target == TypeKind::DOUBLE;
    case TypeKind::BIGINT:
      return target == TypeKind::REAL || target == TypeKind::DOUBLE;
    case TypeKind::REAL:
      return target == TypeKind::DOUBLE;
    default:
      return false;
  }
}

VectorPtr materializeWrappedVector(const VectorPtr& input) {
  auto materialized =
      BaseVector::create(input->type(), input->size(), input->pool());
  SelectivityVector rows{input->size()};
  materialized->copy(input.get(), rows, nullptr);
  return materialized;
}

VectorPtr adaptRowVector(
    const VectorPtr& input,
    const TypePtr& targetType,
    bool mapRowFieldsByPosition) {
  auto source = input;
  if (input->encoding() != VectorEncoding::Simple::ROW) {
    source = materializeWrappedVector(input);
  }
  const auto* sourceVector = source->asChecked<RowVector>();
  const auto& sourceType = source->type()->asRow();
  const auto& targetRowType = targetType->asRow();
  std::vector<VectorPtr> children;
  children.reserve(targetRowType.size());
  for (column_index_t i = 0; i < targetRowType.size(); ++i) {
    const auto sourceIndex = mapRowFieldsByPosition && i < sourceType.size()
        ? std::optional<column_index_t>{i}
        : sourceType.getChildIdxIfExists(targetRowType.nameOf(i));
    if (!sourceIndex.has_value()) {
      children.push_back(
          BaseVector::createNullConstant(
              targetRowType.childAt(i), source->size(), source->pool()));
      continue;
    }
    children.push_back(adaptVortexVectorType(
        sourceVector->childAt(sourceIndex.value()),
        targetRowType.childAt(i),
        mapRowFieldsByPosition));
  }
  return std::make_shared<RowVector>(
      source->pool(),
      targetType,
      source->nulls(),
      source->size(),
      std::move(children),
      source->getNullCount());
}

VectorPtr adaptArrayVector(
    const VectorPtr& input,
    const TypePtr& targetType,
    bool mapRowFieldsByPosition) {
  auto source = input;
  if (input->encoding() != VectorEncoding::Simple::ARRAY) {
    source = materializeWrappedVector(input);
  }
  const auto* sourceVector = source->asChecked<ArrayVector>();
  return std::make_shared<ArrayVector>(
      source->pool(),
      targetType,
      source->nulls(),
      source->size(),
      sourceVector->offsets(),
      sourceVector->sizes(),
      adaptVortexVectorType(
          sourceVector->elements(),
          targetType->childAt(0),
          mapRowFieldsByPosition),
      source->getNullCount());
}

VectorPtr adaptMapVector(
    const VectorPtr& input,
    const TypePtr& targetType,
    bool mapRowFieldsByPosition) {
  auto source = input;
  if (input->encoding() != VectorEncoding::Simple::MAP) {
    source = materializeWrappedVector(input);
  }
  const auto* sourceVector = source->asChecked<MapVector>();
  const bool preserveSortedKeys = sourceVector->hasSortedKeys() &&
      source->type()->childAt(0)->equivalent(*targetType->childAt(0));
  return std::make_shared<MapVector>(
      source->pool(),
      targetType,
      source->nulls(),
      source->size(),
      sourceVector->offsets(),
      sourceVector->sizes(),
      adaptVortexVectorType(
          sourceVector->mapKeys(),
          targetType->childAt(0),
          mapRowFieldsByPosition),
      adaptVortexVectorType(
          sourceVector->mapValues(),
          targetType->childAt(1),
          mapRowFieldsByPosition),
      source->getNullCount(),
      preserveSortedKeys);
}

} // namespace

struct VortexArrowMemory::Context {
  explicit Context(memory::MemoryPool& memoryPool)
      : pool{memoryPool.shared_from_this()} {}

  std::atomic<uint32_t> references{1};
  std::shared_ptr<memory::MemoryPool> pool;
};

VortexArrowMemory::VortexArrowMemory(memory::MemoryPool& pool)
    : context_{new Context{pool}},
      callbacks_{
          .struct_size = sizeof(vx_velox_arrow_memory_callbacks),
          .abi_version = VX_VELOX_ABI_VERSION,
          .context = context_,
          .retain_context = retainContext,
          .release_context = releaseContext,
          .report_allocation = reportAllocation,
          .report_free = reportFree,
          .last_error = lastError,
      } {}

VortexArrowMemory::~VortexArrowMemory() {
  releaseContext(context_);
}

const vx_velox_arrow_memory_callbacks& VortexArrowMemory::callbacks() const {
  return callbacks_;
}

void VortexArrowMemory::retainContext(void* rawContext) noexcept {
  auto* context = static_cast<Context*>(rawContext);
  context->references.fetch_add(1, std::memory_order_relaxed);
}

void VortexArrowMemory::releaseContext(void* rawContext) noexcept {
  auto* context = static_cast<Context*>(rawContext);
  if (context->references.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    delete context;
  }
}

int32_t VortexArrowMemory::reportAllocation(
    void* rawContext,
    size_t bytes) noexcept {
  auto* context = static_cast<Context*>(rawContext);
  try {
    VELOX_CHECK_GT(bytes, 0);
    VELOX_CHECK_LE(bytes, std::numeric_limits<int64_t>::max());
    context->pool->reportExternalAllocation(static_cast<int64_t>(bytes));
    return 0;
  } catch (const std::exception& error) {
    setCallbackError(context, error.what());
    return 1;
  } catch (...) {
    setCallbackError(context, "Unknown Velox memory allocation error");
    return 1;
  }
}

void VortexArrowMemory::reportFree(void* rawContext, size_t bytes) noexcept {
  auto* context = static_cast<Context*>(rawContext);
  try {
    if (bytes == 0) {
      return;
    }
    VELOX_CHECK_LE(bytes, std::numeric_limits<int64_t>::max());
    context->pool->reportExternalFree(static_cast<int64_t>(bytes));
  } catch (const std::exception& error) {
    setCallbackError(context, error.what());
  } catch (...) {
    setCallbackError(context, "Unknown Velox memory free error");
  }
}

const char* VortexArrowMemory::lastError(void* rawContext) noexcept {
  if (callbackErrorState.context != rawContext) {
    return "";
  }
  return callbackErrorState.error.data();
}

struct VortexExportCursor::State {
  State(const vx_velox_session* session, VortexArray array)
      : session{session},
        array{std::move(array)},
        cursor{nullptr, vx_velox_export_cursor_free} {}

  vx_velox_export_cursor* ensureCursor(memory::MemoryPool& memoryPool) {
    if (cursor != nullptr) {
      VELOX_CHECK(
          pool == &memoryPool,
          "A Vortex export cursor changed its memory pool");
      return cursor.get();
    }
    pool = &memoryPool;
    memory = std::make_unique<VortexArrowMemory>(memoryPool);
    vx_velox_error* error{nullptr};
    auto* exportCursor = vx_velox_export_cursor_new(
        session, array.get(), &memory->callbacks(), &error);
    if (exportCursor == nullptr) {
      failVortex("prepare a Vortex export cursor", error);
    }
    cursor.reset(exportCursor);
    return cursor.get();
  }

  const vx_velox_session* session;
  VortexArray array;
  memory::MemoryPool* pool{nullptr};
  std::unique_ptr<VortexArrowMemory> memory;
  std::
      unique_ptr<vx_velox_export_cursor, decltype(&vx_velox_export_cursor_free)>
          cursor;
  std::shared_ptr<VortexBufferLifetime> lifetime;
  std::unordered_map<const vx_velox_export_cursor*, VectorPtr> cursorCache;
  mutable std::mutex mutex;
};

VortexExportCursor::VortexExportCursor(
    const vx_velox_session* session,
    const VortexArray& array)
    : state_{std::make_unique<State>(session, array)} {}

VortexExportCursor::~VortexExportCursor() = default;

VectorPtr VortexExportCursor::import(
    size_t offset,
    size_t length,
    const TypePtr& targetType,
    RowSet sourceRows,
    memory::MemoryPool& pool) {
  VELOX_CHECK(supportsNativeVortexType(targetType));
  if (sourceRows.data() != nullptr && sourceRows.empty()) {
    return BaseVector::create(targetType, 0, &pool);
  }
  VELOX_USER_CHECK_LE(
      length,
      std::numeric_limits<size_t>::max() - offset,
      "A Vortex export range exceeds the supported size: {}, {}",
      offset,
      length);
  VectorPtr result;
  {
    std::lock_guard lock{state_->mutex};
    PrimitiveCapture capture{
        .session = state_->session,
        .targetType = targetType,
        .pool = &pool,
        .sharedLifetime = &state_->lifetime,
        .cursorCache = &state_->cursorCache,
        .memoryPrecharged = true,
    };
    const auto visitor = makeVisitor(capture);
    vx_velox_error* error{nullptr};
    if (vx_velox_export_cursor_visit(
            state_->ensureCursor(pool), offset, length, &visitor, &error) !=
        0) {
      failVortex("visit a prepared Vortex array", error);
    }
    VELOX_CHECK_NOT_NULL(capture.result);
    result = std::move(capture.result);
  }
  if (sourceRows.data() == nullptr) {
    return result;
  }
  bool identity = sourceRows.size() == length;
  for (vector_size_t row = 0; row < sourceRows.size(); ++row) {
    VELOX_USER_CHECK_LT(
        sourceRows[row],
        length,
        "A Vortex export selection exceeds its window: {}",
        sourceRows[row]);
    identity = identity && sourceRows[row] == row;
  }
  if (identity) {
    return result;
  }
  auto indices = allocateIndices(sourceRows.size(), &pool);
  std::copy(
      sourceRows.begin(),
      sourceRows.end(),
      indices->asMutable<vector_size_t>());
  return BaseVector::wrapInDictionary(
      nullptr, std::move(indices), sourceRows.size(), std::move(result));
}

VortexRowPositions::VortexRowPositions(uint64_t firstRow, size_t size)
    : firstRow_{firstRow}, size_{size} {
  VELOX_CHECK_LE(
      size_, static_cast<size_t>(std::numeric_limits<vector_size_t>::max()));
  VELOX_CHECK_LE(
      size_,
      std::numeric_limits<uint64_t>::max() - firstRow_,
      "A contiguous Vortex row range exceeds the supported row count: {}",
      size_);
}

VortexRowPositions::VortexRowPositions(BufferPtr values, size_t size)
    : values_{std::move(values)}, size_{size} {
  VELOX_CHECK_NOT_NULL(values_);
  VELOX_CHECK_LE(
      size_, static_cast<size_t>(std::numeric_limits<vector_size_t>::max()));
  VELOX_CHECK_GE(values_->size(), checkedRowPositionBytes(size_));
}

size_t VortexRowPositions::size() const {
  return size_;
}

uint64_t VortexRowPositions::at(size_t index) const {
  VELOX_CHECK_LT(index, size_);
  return values_ == nullptr ? firstRow_ + index
                            : values_->as<uint64_t>()[index];
}

size_t VortexRowPositions::lowerBound(size_t begin, uint64_t value) const {
  VELOX_CHECK_LE(begin, size_);
  if (values_ == nullptr) {
    if (value <= firstRow_) {
      return begin;
    }
    const auto offset = std::min<uint64_t>(value - firstRow_, size_);
    return std::max(begin, static_cast<size_t>(offset));
  }
  const auto positions = std::span{values_->as<uint64_t>(), size_};
  return std::lower_bound(positions.begin() + begin, positions.end(), value) -
      positions.begin();
}

bool VortexRowPositions::isContiguous() const {
  return values_ == nullptr;
}

void VortexRowPositions::validateRange(uint64_t begin, uint64_t end) const {
  VELOX_CHECK_GT(size_, 0);
  const auto first = at(0);
  const auto last = at(size_ - 1);
  VELOX_CHECK_GE(
      first,
      begin,
      "A Vortex scan returned an already consumed row: {}",
      first);
  VELOX_CHECK_LT(
      last,
      end,
      "A Vortex scan returned a row outside its assigned range: {}",
      last);
#ifndef NDEBUG
  if (values_ != nullptr) {
    const auto* positions = values_->as<uint64_t>();
    for (size_t i = 1; i < size_; ++i) {
      VELOX_DCHECK_LT(
          positions[i - 1],
          positions[i],
          "Vortex row indexes must be strictly increasing: {}",
          positions[i]);
    }
  }
#endif
}

bool supportsNativeVortexType(const TypePtr& type) {
  if (type->kind() == TypeKind::ROW || type->kind() == TypeKind::ARRAY ||
      type->kind() == TypeKind::MAP) {
    for (size_t child = 0; child < type->size(); ++child) {
      if (!supportsNativeVortexType(type->childAt(child))) {
        return false;
      }
    }
    return true;
  }
  return !type->isTime() &&
      (type->kind() == TypeKind::BOOLEAN || type->kind() == TypeKind::TINYINT ||
       type->kind() == TypeKind::SMALLINT ||
       type->kind() == TypeKind::INTEGER || type->kind() == TypeKind::BIGINT ||
       type->kind() == TypeKind::REAL || type->kind() == TypeKind::DOUBLE ||
       type->kind() == TypeKind::VARCHAR ||
       type->kind() == TypeKind::VARBINARY || type->isDecimal());
}

VortexRowPositions readVortexRowIndices(
    const vx_velox_session* session,
    const VortexArray& array,
    memory::MemoryPool& pool) {
  RowIndexCapture capture{.pool = &pool};
  const vx_velox_visit_request request{
      .struct_size = sizeof(vx_velox_visit_request),
      .rows = nullptr,
      .row_count = 0,
  };
  const vx_velox_visitor visitor{
      .struct_size = sizeof(vx_velox_visitor),
      .abi_version = VX_VELOX_ABI_VERSION,
      .context = &capture,
      .visit_primitive = captureRowIndices,
      .last_error = rowIndexError,
      .visit_varbin = nullptr,
      .visit_dictionary = nullptr,
      .visit_constant = nullptr,
      .visit_bool = nullptr,
      .visit_struct = nullptr,
      .visit_list = nullptr,
      .visit_map = nullptr,
  };
  vx_velox_error* error{nullptr};
  if (vx_velox_array_visit(session, array.get(), &request, &visitor, &error) !=
      0) {
    failVortex("read Vortex row indexes", error);
  }
  VELOX_CHECK(capture.result.has_value());
  return std::move(capture.result.value());
}

VectorPtr importVortexVector(
    const vx_velox_session* session,
    const VortexArray& array,
    const TypePtr& targetType,
    RowSet sourceRows,
    memory::MemoryPool& pool) {
  const bool allRows = sourceRows.data() == nullptr;
  if (!allRows && sourceRows.empty()) {
    return BaseVector::create(targetType, 0, &pool);
  }
  if (!supportsNativeVortexType(targetType)) {
    auto imported = importArrow(session, array, pool);
    if (!allRows) {
      auto indices = allocateIndices(sourceRows.size(), &pool);
      std::copy(
          sourceRows.begin(),
          sourceRows.end(),
          indices->asMutable<vector_size_t>());
      imported = BaseVector::wrapInDictionary(
          nullptr, std::move(indices), sourceRows.size(), std::move(imported));
    }
    return adaptVortexVectorType(imported, targetType);
  }

  const auto rows = visitorRows(sourceRows, array.size());
  PrimitiveCapture capture{
      .session = session,
      .targetType = targetType,
      .pool = &pool,
  };
  const vx_velox_visit_request request{
      .struct_size = sizeof(vx_velox_visit_request),
      .rows = rows.empty() ? nullptr : rows.data(),
      .row_count = rows.size(),
  };
  const auto visitor = makeVisitor(capture);
  vx_velox_error* error{nullptr};
  if (vx_velox_array_visit(session, array.get(), &request, &visitor, &error) !=
      0) {
    failVortex("visit a Vortex primitive array", error);
  }
  VELOX_CHECK_NOT_NULL(capture.result);
  return capture.result;
}

void loadVortexValueHook(
    const vx_velox_session* session,
    const VortexArray& array,
    const TypePtr& targetType,
    RowSet sourceRows,
    RowSet hookRows,
    ValueHook& hook,
    memory::MemoryPool& pool) {
  VELOX_CHECK(supportsNativeVortexType(targetType));
  const auto selectedSize =
      sourceRows.data() == nullptr ? array.size() : sourceRows.size();
  if (hookRows.data() != nullptr) {
    VELOX_CHECK_EQ(hookRows.size(), selectedSize);
  }
  const auto rows = visitorRows(sourceRows, array.size());
  PrimitiveCapture capture{
      .session = session,
      .targetType = targetType,
      .pool = &pool,
      .hook = &hook,
      .hookRows = hookRows,
  };
  const vx_velox_visit_request request{
      .struct_size = sizeof(vx_velox_visit_request),
      .rows = rows.empty() ? nullptr : rows.data(),
      .row_count = rows.size(),
  };
  const auto visitor = makeVisitor(capture);
  vx_velox_error* error{nullptr};
  if (vx_velox_array_visit(session, array.get(), &request, &visitor, &error) !=
      0) {
    failVortex("load a Vortex value hook", error);
  }
}

VectorPtr adaptVortexVectorType(
    const VectorPtr& input,
    const TypePtr& targetType,
    bool mapRowFieldsByPosition) {
  if (input->type()->equivalent(*targetType)) {
    return input;
  }
  if (input->typeKind() == TypeKind::UNKNOWN) {
    return BaseVector::createNullConstant(
        targetType, input->size(), input->pool());
  }
  switch (targetType->kind()) {
    case TypeKind::ROW: {
      VELOX_USER_CHECK_EQ(
          static_cast<int32_t>(input->typeKind()),
          static_cast<int32_t>(TypeKind::ROW),
          "Unsupported Vortex schema conversion: {} to {}",
          input->type()->toString(),
          targetType->toString());
      return adaptRowVector(input, targetType, mapRowFieldsByPosition);
    }
    case TypeKind::ARRAY: {
      VELOX_USER_CHECK_EQ(
          static_cast<int32_t>(input->typeKind()),
          static_cast<int32_t>(TypeKind::ARRAY),
          "Unsupported Vortex schema conversion: {} to {}",
          input->type()->toString(),
          targetType->toString());
      return adaptArrayVector(input, targetType, mapRowFieldsByPosition);
    }
    case TypeKind::MAP: {
      VELOX_USER_CHECK_EQ(
          static_cast<int32_t>(input->typeKind()),
          static_cast<int32_t>(TypeKind::MAP),
          "Unsupported Vortex schema conversion: {} to {}",
          input->type()->toString(),
          targetType->toString());
      return adaptMapVector(input, targetType, mapRowFieldsByPosition);
    }
    default:
      break;
  }
  VELOX_USER_CHECK(
      !input->type()->isDate() && !input->type()->isTime() &&
          !input->type()->isDecimal() && !targetType->isDate() &&
          !targetType->isTime() && !targetType->isDecimal() &&
          isNumericWidening(input->typeKind(), targetType->kind()),
      "Unsupported Vortex schema conversion: {} to {}",
      input->type()->toString(),
      targetType->toString());
  switch (targetType->kind()) {
    case TypeKind::SMALLINT:
      return castNumericVector<int16_t>(input, targetType);
    case TypeKind::INTEGER:
      return castNumericVector<int32_t>(input, targetType);
    case TypeKind::BIGINT:
      return castNumericVector<int64_t>(input, targetType);
    case TypeKind::REAL:
      return castNumericVector<float>(input, targetType);
    case TypeKind::DOUBLE:
      return castNumericVector<double>(input, targetType);
    default:
      VELOX_UNREACHABLE();
  }
}

} // namespace facebook::velox::dwio::vortex
