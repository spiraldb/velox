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

#include "velox/dwio/vortex/VortexVectorVisitor.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwio::vortex {
namespace {

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
    auto imported = detail::importVortexArrow(session, array, pool);
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
  return detail::importVortexNative(
      session, array, targetType, sourceRows, pool);
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
  detail::loadVortexNativeValueHook(
      session, array, targetType, sourceRows, hookRows, hook, pool);
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
