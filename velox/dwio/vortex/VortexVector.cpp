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
#include <limits>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::dwio::vortex {
namespace {

class VortexBufferLifetime {
 public:
  VortexBufferLifetime(
      const vx_velox_buffer_owner& owner,
      memory::MemoryPool& pool)
      : owner_{owner}, retainedBytes_{owner.retained_bytes}, pool_{pool} {
    VELOX_USER_CHECK_GE(
        owner_.struct_size,
        sizeof(vx_velox_buffer_owner),
        "Vortex buffer owner structure is too small: {}",
        owner_.struct_size);
    VELOX_USER_CHECK_NOT_NULL(
        owner_.retain, "Vortex buffer owner lacks a retain callback");
    VELOX_USER_CHECK_NOT_NULL(
        owner_.release, "Vortex buffer owner lacks a release callback");
    VELOX_USER_CHECK_LE(
        retainedBytes_,
        std::numeric_limits<int64_t>::max(),
        "Vortex retained buffer size exceeds the Velox memory limit: {}",
        retainedBytes_);
    if (retainedBytes_ != 0) {
      pool_.reportExternalAllocation(static_cast<int64_t>(retainedBytes_));
    }
    owner_.retain(owner_.owner);
  }

  ~VortexBufferLifetime() {
    if (retainedBytes_ != 0) {
      pool_.reportExternalFree(static_cast<int64_t>(retainedBytes_));
    }
    owner_.release(owner_.owner);
  }

 private:
  vx_velox_buffer_owner owner_;
  size_t retainedBytes_;
  memory::MemoryPool& pool_;
};

struct SharedBufferReleaser {
  void addRef() const {}
  void release() const {}

  std::shared_ptr<VortexBufferLifetime> lifetime;
};

struct PrimitiveCapture {
  TypePtr targetType;
  memory::MemoryPool* pool;
  ValueHook* hook{nullptr};
  RowSet hookRows{};
  VectorPtr result;
  std::array<char, 1'024> error{};
};

struct RowIndexCapture {
  std::vector<uint64_t> rows;
  std::array<char, 1'024> error{};
};

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
  }
  VELOX_UNREACHABLE();
}

vx_velox_primitive_type primitiveType(const TypePtr& type) {
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

bool isNullAt(const vx_velox_primitive_view& view, vector_size_t row) {
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

void validateValidity(
    const vx_velox_primitive_view& view,
    vector_size_t length) {
  if (view.validity_kind != VX_VELOX_VALIDITY_BITMAP) {
    return;
  }
  VELOX_USER_CHECK_NOT_NULL(
      view.validity, "Vortex validity bitmap must not be null");
  VELOX_USER_CHECK_GE(
      view.validity_length,
      bits::nbytes(view.validity_bit_offset + length),
      "Vortex validity bitmap is too small: {}",
      view.validity_length);
}

BufferPtr importNulls(
    const vx_velox_primitive_view& view,
    const std::shared_ptr<VortexBufferLifetime>& lifetime,
    vector_size_t length,
    memory::MemoryPool& pool) {
  switch (view.validity_kind) {
    case VX_VELOX_VALIDITY_NON_NULLABLE:
    case VX_VELOX_VALIDITY_ALL_VALID:
      return nullptr;
    case VX_VELOX_VALIDITY_ALL_INVALID:
      return allocateNulls(length, &pool, bits::kNull);
    case VX_VELOX_VALIDITY_BITMAP:
      break;
  }
  validateValidity(view, length);
  if (view.validity_bit_offset == 0 &&
      view.validity_alignment >= alignof(uint64_t) &&
      reinterpret_cast<uintptr_t>(view.validity) % alignof(uint64_t) == 0) {
    return BufferView<SharedBufferReleaser>::create(
        view.validity, bits::nbytes(length), SharedBufferReleaser{lifetime});
  }
  auto nulls = allocateNulls(length, &pool, bits::kNull);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  for (vector_size_t row = 0; row < length; ++row) {
    if (!isNullAt(view, row)) {
      bits::setBit(rawNulls, row);
    }
  }
  return nulls;
}

template <typename T>
void captureTyped(
    PrimitiveCapture& capture,
    const vx_velox_primitive_view& view,
    const std::shared_ptr<VortexBufferLifetime>& lifetime) {
  const auto length = static_cast<vector_size_t>(view.length);
  const auto* values = reinterpret_cast<const T*>(view.values);
  if (capture.hook != nullptr) {
    for (vector_size_t row = 0; row < length; ++row) {
      const auto hookRow =
          capture.hookRows.data() == nullptr ? row : capture.hookRows[row];
      if (isNullAt(view, row)) {
        if (capture.hook->acceptsNulls()) {
          capture.hook->addNull(hookRow);
        }
      } else {
        capture.hook->addValueTyped(hookRow, values[row]);
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
    std::shared_ptr<VortexBufferLifetime> lifetime;
    if (capture->hook == nullptr) {
      lifetime =
          std::make_shared<VortexBufferLifetime>(view->buffers, *capture->pool);
    }
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
    VELOX_USER_CHECK_GE(
        view->values_length,
        sizeof(uint64_t) * view->length,
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
    if (view->length == 0) {
      capture->rows.clear();
    } else {
      const auto* values = reinterpret_cast<const uint64_t*>(view->values);
      capture->rows.assign(values, values + view->length);
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
        "Unknown Vortex row-index callback error"};
    std::copy(kError.begin(), kError.end(), capture->error.begin());
    capture->error[kError.size()] = '\0';
    return 1;
  }
}

const char* rowIndexError(void* rawContext) {
  return static_cast<RowIndexCapture*>(rawContext)->error.data();
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
    const vx_session* session,
    const VortexArray& array,
    memory::MemoryPool& pool) {
  ArrowSchema schema{};
  ArrowArray arrowArray{};
  VortexArrowMemory memory{pool};
  vx_error* error{nullptr};
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

bool supportsNativeVortexType(const TypePtr& type) {
  return !type->isDate() && !type->isTime() && !type->isDecimal() &&
      (type->kind() == TypeKind::TINYINT ||
       type->kind() == TypeKind::SMALLINT ||
       type->kind() == TypeKind::INTEGER || type->kind() == TypeKind::BIGINT ||
       type->kind() == TypeKind::REAL || type->kind() == TypeKind::DOUBLE);
}

std::vector<uint64_t> readVortexRowIndices(
    const vx_session* session,
    const VortexArray& array) {
  RowIndexCapture capture;
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
  };
  vx_error* error{nullptr};
  if (vx_velox_array_visit(session, array.get(), &request, &visitor, &error) !=
      0) {
    failVortex("read Vortex row indexes", error);
  }
  return capture.rows;
}

VectorPtr importVortexVector(
    const vx_session* session,
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
      .targetType = targetType,
      .pool = &pool,
  };
  const vx_velox_visit_request request{
      .struct_size = sizeof(vx_velox_visit_request),
      .rows = rows.empty() ? nullptr : rows.data(),
      .row_count = rows.size(),
  };
  const vx_velox_visitor visitor{
      .struct_size = sizeof(vx_velox_visitor),
      .abi_version = VX_VELOX_ABI_VERSION,
      .context = &capture,
      .visit_primitive = capturePrimitive,
      .last_error = primitiveError,
  };
  vx_error* error{nullptr};
  if (vx_velox_array_visit(session, array.get(), &request, &visitor, &error) !=
      0) {
    failVortex("visit a Vortex primitive array", error);
  }
  VELOX_CHECK_NOT_NULL(capture.result);
  return capture.result;
}

void loadVortexValueHook(
    const vx_session* session,
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
  const vx_velox_visitor visitor{
      .struct_size = sizeof(vx_velox_visitor),
      .abi_version = VX_VELOX_ABI_VERSION,
      .context = &capture,
      .visit_primitive = capturePrimitive,
      .last_error = primitiveError,
  };
  vx_error* error{nullptr};
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
