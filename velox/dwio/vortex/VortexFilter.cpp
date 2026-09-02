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

#include "velox/dwio/vortex/VortexFilter.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

namespace facebook::velox::dwio::vortex {
namespace {

using ScalarPtr = std::unique_ptr<vx_scalar, decltype(&vx_velox_scalar_free)>;
using DTypePtr =
    std::unique_ptr<const vx_dtype, decltype(&vx_velox_dtype_free)>;

VortexExpressionPtr expression(vx_expression* value) {
  return VortexExpressionPtr{value};
}

[[noreturn]] void failLiteral(std::string_view operation, vx_error* error) {
  std::string detail{"Vortex returned an unspecified error"};
  if (error != nullptr) {
    const auto view = vx_velox_error_message(error);
    if (view.ptr != nullptr) {
      detail.assign(view.ptr, view.len);
    }
    vx_velox_error_free(error);
  }
  VELOX_FAIL("Failed to {}: {}", operation, detail);
}

VortexExpressionPtr literal(const vx_scalar* scalar) {
  vx_error* error{nullptr};
  auto result = expression(vx_velox_expression_literal(scalar, &error));
  if (error != nullptr) {
    failLiteral("create a Vortex literal expression", error);
  }
  VELOX_CHECK_NOT_NULL(result);
  return result;
}

VortexExpressionPtr booleanLiteral(bool value) {
  ScalarPtr scalar{
      vx_velox_scalar_new_bool(value, false), vx_velox_scalar_free};
  return literal(scalar.get());
}

ScalarPtr integerScalar(const Type& type, int64_t value) {
  ScalarPtr scalar{nullptr, vx_velox_scalar_free};
  switch (type.kind()) {
    case TypeKind::TINYINT:
      scalar.reset(vx_velox_scalar_new_i8(static_cast<int8_t>(value), false));
      break;
    case TypeKind::SMALLINT:
      scalar.reset(vx_velox_scalar_new_i16(static_cast<int16_t>(value), false));
      break;
    case TypeKind::INTEGER:
      scalar.reset(vx_velox_scalar_new_i32(static_cast<int32_t>(value), false));
      break;
    case TypeKind::BIGINT:
      scalar.reset(vx_velox_scalar_new_i64(value, false));
      break;
    default:
      return ScalarPtr{nullptr, vx_velox_scalar_free};
  }

  return scalar;
}

VortexExpressionPtr scalarLiteral(const Type& type, int64_t value) {
  auto scalar = integerScalar(type, value);
  if (scalar == nullptr) {
    return nullptr;
  }
  return literal(scalar.get());
}

VortexExpressionPtr scalarLiteral(const Type& type, double value) {
  ScalarPtr scalar{nullptr, vx_velox_scalar_free};
  switch (type.kind()) {
    case TypeKind::REAL:
      scalar.reset(vx_velox_scalar_new_f32(static_cast<float>(value), false));
      break;
    case TypeKind::DOUBLE:
      scalar.reset(vx_velox_scalar_new_f64(value, false));
      break;
    default:
      return nullptr;
  }

  return literal(scalar.get());
}

VortexExpressionPtr scalarLiteral(const Type& type, std::string_view value) {
  vx_error* error{nullptr};
  ScalarPtr scalar{nullptr, vx_velox_scalar_free};
  const vx_view view{value.data(), value.size()};
  if (type.kind() == TypeKind::VARCHAR) {
    scalar.reset(vx_velox_scalar_new_utf8(view, false, &error));
  } else if (type.kind() == TypeKind::VARBINARY) {
    scalar.reset(vx_velox_scalar_new_binary(
        reinterpret_cast<const uint8_t*>(value.data()),
        value.size(),
        false,
        &error));
  } else {
    return nullptr;
  }
  if (scalar == nullptr) {
    if (error != nullptr) {
      vx_velox_error_free(error);
    }
    return nullptr;
  }
  VELOX_CHECK_NULL(error);
  return literal(scalar.get());
}

VortexExpressionPtr binary(
    vx_velox_binary_operator operation,
    const vx_expression* left,
    const vx_expression* right) {
  vx_error* error{nullptr};
  auto result =
      expression(vx_velox_expression_binary(operation, left, right, &error));
  if (error != nullptr) {
    failLiteral("create a Vortex binary expression", error);
  }
  VELOX_CHECK_NOT_NULL(result);
  return result;
}

VortexExpressionPtr conjunction(std::vector<VortexExpressionPtr> children) {
  if (std::ranges::any_of(
          children, [](const auto& child) { return child == nullptr; })) {
    return nullptr;
  }
  if (children.empty()) {
    return booleanLiteral(true);
  }
  if (children.size() == 1) {
    return std::move(children.front());
  }
  std::vector<const vx_expression*> expressions;
  expressions.reserve(children.size());
  for (const auto& child : children) {
    expressions.push_back(child.get());
  }
  return expression(
      vx_velox_expression_and(expressions.data(), expressions.size()));
}

VortexExpressionPtr disjunction(std::vector<VortexExpressionPtr> children) {
  if (std::ranges::any_of(
          children, [](const auto& child) { return child == nullptr; })) {
    return nullptr;
  }
  if (children.empty()) {
    return booleanLiteral(false);
  }
  if (children.size() == 1) {
    return std::move(children.front());
  }
  std::vector<const vx_expression*> expressions;
  expressions.reserve(children.size());
  for (const auto& child : children) {
    expressions.push_back(child.get());
  }
  return expression(
      vx_velox_expression_or(expressions.data(), expressions.size()));
}

VortexExpressionPtr negate(VortexExpressionPtr child) {
  if (child == nullptr) {
    return nullptr;
  }
  return expression(vx_velox_expression_not(child.get()));
}

VortexExpressionPtr isNull(const vx_expression* column) {
  return expression(vx_velox_expression_is_null(column));
}

VortexExpressionPtr isNotNull(const vx_expression* column) {
  return negate(isNull(column));
}

VortexExpressionPtr compare(
    vx_velox_binary_operator operation,
    const vx_expression* column,
    VortexExpressionPtr literal) {
  if (literal == nullptr) {
    return nullptr;
  }
  return binary(operation, column, literal.get());
}

VortexExpressionPtr applyNullSemantics(
    const velox::common::Filter& filter,
    const vx_expression* column,
    VortexExpressionPtr nonNullPredicate) {
  if (nonNullPredicate == nullptr) {
    return nullptr;
  }

  std::vector<VortexExpressionPtr> nonNullChildren;
  nonNullChildren.push_back(isNotNull(column));
  nonNullChildren.push_back(std::move(nonNullPredicate));
  auto result = conjunction(std::move(nonNullChildren));
  if (!filter.nullAllowed()) {
    return result;
  }

  std::vector<VortexExpressionPtr> nullableChildren;
  nullableChildren.push_back(std::move(result));
  nullableChildren.push_back(isNull(column));
  return disjunction(std::move(nullableChildren));
}

std::optional<std::pair<int64_t, int64_t>> integerBounds(const Type& type) {
  switch (type.kind()) {
    case TypeKind::TINYINT:
      return std::pair<int64_t, int64_t>{
          std::numeric_limits<int8_t>::min(),
          std::numeric_limits<int8_t>::max()};
    case TypeKind::SMALLINT:
      return std::pair<int64_t, int64_t>{
          std::numeric_limits<int16_t>::min(),
          std::numeric_limits<int16_t>::max()};
    case TypeKind::INTEGER:
      return std::pair<int64_t, int64_t>{
          std::numeric_limits<int32_t>::min(),
          std::numeric_limits<int32_t>::max()};
    case TypeKind::BIGINT:
      return std::pair<int64_t, int64_t>{
          std::numeric_limits<int64_t>::min(),
          std::numeric_limits<int64_t>::max()};
    default:
      return std::nullopt;
  }
}

std::optional<vx_velox_ptype> integerPType(const Type& type) {
  switch (type.kind()) {
    case TypeKind::TINYINT:
      return VX_VELOX_PTYPE_I8;
    case TypeKind::SMALLINT:
      return VX_VELOX_PTYPE_I16;
    case TypeKind::INTEGER:
      return VX_VELOX_PTYPE_I32;
    case TypeKind::BIGINT:
      return VX_VELOX_PTYPE_I64;
    default:
      return std::nullopt;
  }
}

VortexExpressionPtr integerListLiteral(
    const Type& type,
    const std::vector<int64_t>& values) {
  const auto ptype = integerPType(type);
  if (!ptype.has_value()) {
    return nullptr;
  }

  vx_error* error{nullptr};
  DTypePtr elementType{
      vx_velox_dtype_new_primitive(ptype.value(), false, &error),
      vx_velox_dtype_free};
  if (error != nullptr) {
    failLiteral("create a Vortex primitive type", error);
  }
  VELOX_CHECK_NOT_NULL(elementType);

  std::vector<ScalarPtr> elements;
  std::vector<const vx_scalar*> elementPointers;
  elements.reserve(values.size());
  elementPointers.reserve(values.size());
  for (const auto value : values) {
    elements.push_back(integerScalar(type, value));
    VELOX_CHECK_NOT_NULL(elements.back());
    elementPointers.push_back(elements.back().get());
  }

  ScalarPtr list{
      vx_velox_scalar_new_list(
          elementType.get(),
          elementPointers.data(),
          elementPointers.size(),
          false,
          &error),
      vx_velox_scalar_free};
  if (error != nullptr) {
    failLiteral("create a Vortex integer list", error);
  }
  VELOX_CHECK_NOT_NULL(list);
  return literal(list.get());
}

VortexExpressionPtr integerRange(
    const Type& type,
    const vx_expression* column,
    int64_t lower,
    int64_t upper) {
  const auto bounds = integerBounds(type);
  if (!bounds.has_value()) {
    return nullptr;
  }
  if (upper < bounds->first || lower > bounds->second) {
    return booleanLiteral(false);
  }

  lower = std::max(lower, bounds->first);
  upper = std::min(upper, bounds->second);
  if (lower == upper) {
    return compare(
        VX_VELOX_OPERATOR_EQ,
        column,
        scalarLiteral(type, static_cast<int64_t>(lower)));
  }

  std::vector<VortexExpressionPtr> comparisons;
  if (lower != bounds->first) {
    comparisons.push_back(compare(
        VX_VELOX_OPERATOR_GTE,
        column,
        scalarLiteral(type, static_cast<int64_t>(lower))));
  }
  if (upper != bounds->second) {
    comparisons.push_back(compare(
        VX_VELOX_OPERATOR_LTE,
        column,
        scalarLiteral(type, static_cast<int64_t>(upper))));
  }
  return conjunction(std::move(comparisons));
}

VortexExpressionPtr integerValues(
    const Type& type,
    const vx_expression* column,
    std::vector<int64_t> values) {
  const auto bounds = integerBounds(type);
  if (!bounds.has_value()) {
    return nullptr;
  }
  std::erase_if(values, [&](int64_t value) {
    return value < bounds->first || value > bounds->second;
  });
  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());

  if (values.empty()) {
    return booleanLiteral(false);
  }
  if (values.size() == 1) {
    return compare(
        VX_VELOX_OPERATOR_EQ, column, scalarLiteral(type, values.front()));
  }

  auto list = integerListLiteral(type, values);
  if (list == nullptr) {
    return nullptr;
  }
  return expression(vx_velox_expression_list_contains(list.get(), column));
}

template <typename TRange>
VortexExpressionPtr floatingRange(
    const Type& type,
    const vx_expression* column,
    const TRange& range) {
  const bool typeMatches =
      (range.kind() == velox::common::FilterKind::kFloatRange &&
       type.kind() == TypeKind::REAL) ||
      (range.kind() == velox::common::FilterKind::kDoubleRange &&
       type.kind() == TypeKind::DOUBLE);
  if (!typeMatches) {
    return nullptr;
  }

  const auto serialized = range.serialize();
  const auto lowerUnbounded = serialized["lowerUnbounded"].asBool();
  const auto lowerExclusive = serialized["lowerExclusive"].asBool();
  const auto upperUnbounded = serialized["upperUnbounded"].asBool();
  const auto upperExclusive = serialized["upperExclusive"].asBool();

  const auto negativeInfinity = -std::numeric_limits<double>::infinity();
  const auto positiveInfinity = std::numeric_limits<double>::infinity();
  std::vector<VortexExpressionPtr> nanComparisons;
  nanComparisons.push_back(compare(
      VX_VELOX_OPERATOR_LT, column, scalarLiteral(type, negativeInfinity)));
  nanComparisons.push_back(compare(
      VX_VELOX_OPERATOR_GT, column, scalarLiteral(type, positiveInfinity)));
  auto isNan = disjunction(std::move(nanComparisons));

  std::vector<VortexExpressionPtr> comparisons;
  if (!lowerUnbounded) {
    auto lower = range.lower();
    if (lower == 0) {
      lower = lowerExclusive ? 0.0 : -0.0;
    }
    comparisons.push_back(compare(
        lowerExclusive ? VX_VELOX_OPERATOR_GT : VX_VELOX_OPERATOR_GTE,
        column,
        scalarLiteral(type, lower)));
  }
  if (!upperUnbounded) {
    auto upper = range.upper();
    if (upper == 0) {
      upper = upperExclusive ? -0.0 : 0.0;
    }
    comparisons.push_back(compare(
        upperExclusive ? VX_VELOX_OPERATOR_LT : VX_VELOX_OPERATOR_LTE,
        column,
        scalarLiteral(type, upper)));
  }
  auto result = conjunction(std::move(comparisons));
  if (upperUnbounded) {
    std::vector<VortexExpressionPtr> withNan;
    withNan.push_back(std::move(result));
    withNan.push_back(std::move(isNan));
    return disjunction(std::move(withNan));
  }

  std::vector<VortexExpressionPtr> withoutNan;
  withoutNan.push_back(std::move(result));
  withoutNan.push_back(negate(std::move(isNan)));
  return conjunction(std::move(withoutNan));
}

VortexExpressionPtr bytesRange(
    const Type& type,
    const vx_expression* column,
    const velox::common::BytesRange& range) {
  if (type.kind() != TypeKind::VARCHAR && type.kind() != TypeKind::VARBINARY) {
    return nullptr;
  }
  std::vector<VortexExpressionPtr> comparisons;
  if (!range.isLowerUnbounded()) {
    comparisons.push_back(compare(
        range.isLowerExclusive() ? VX_VELOX_OPERATOR_GT : VX_VELOX_OPERATOR_GTE,
        column,
        scalarLiteral(type, range.lower())));
  }
  if (!range.isUpperUnbounded()) {
    comparisons.push_back(compare(
        range.isUpperExclusive() ? VX_VELOX_OPERATOR_LT : VX_VELOX_OPERATOR_LTE,
        column,
        scalarLiteral(type, range.upper())));
  }
  for (const auto& comparison : comparisons) {
    if (comparison == nullptr) {
      return nullptr;
    }
  }
  return conjunction(std::move(comparisons));
}

VortexExpressionPtr bytesValues(
    const Type& type,
    const vx_expression* column,
    const folly::F14FastSet<std::string>& values) {
  if (type.kind() != TypeKind::VARCHAR && type.kind() != TypeKind::VARBINARY) {
    return nullptr;
  }
  std::vector<std::string_view> sortedValues;
  sortedValues.reserve(values.size());
  for (const auto& value : values) {
    sortedValues.push_back(value);
  }
  std::sort(sortedValues.begin(), sortedValues.end());

  std::vector<VortexExpressionPtr> comparisons;
  comparisons.reserve(sortedValues.size());
  for (const auto value : sortedValues) {
    auto comparison =
        compare(VX_VELOX_OPERATOR_EQ, column, scalarLiteral(type, value));
    if (comparison == nullptr) {
      return nullptr;
    }
    comparisons.push_back(std::move(comparison));
  }
  return disjunction(std::move(comparisons));
}

VortexExpressionPtr convertNonNullFilter(
    const velox::common::Filter& filter,
    const Type& type,
    const vx_expression* column) {
  switch (filter.kind()) {
    case velox::common::FilterKind::kAlwaysFalse:
      return booleanLiteral(false);
    case velox::common::FilterKind::kAlwaysTrue:
      return booleanLiteral(true);
    case velox::common::FilterKind::kBoolValue: {
      const auto value = filter.testBool(true);
      if (type.kind() == TypeKind::BOOLEAN) {
        ScalarPtr scalar{
            vx_velox_scalar_new_bool(value, false), vx_velox_scalar_free};
        return compare(VX_VELOX_OPERATOR_EQ, column, literal(scalar.get()));
      }
      if (!integerBounds(type).has_value()) {
        return nullptr;
      }
      auto equalsZero = compare(
          VX_VELOX_OPERATOR_EQ, column, scalarLiteral(type, int64_t{0}));
      return value ? negate(std::move(equalsZero)) : std::move(equalsZero);
    }
    case velox::common::FilterKind::kBigintRange: {
      const auto& range = *filter.as<velox::common::BigintRange>();
      return integerRange(type, column, range.lower(), range.upper());
    }
    case velox::common::FilterKind::kNegatedBigintRange: {
      const auto& range = *filter.as<velox::common::NegatedBigintRange>();
      return negate(integerRange(type, column, range.lower(), range.upper()));
    }
    case velox::common::FilterKind::kBigintValuesUsingHashTable: {
      const auto& values =
          filter.as<velox::common::BigintValuesUsingHashTable>()->values();
      return integerValues(type, column, values);
    }
    case velox::common::FilterKind::kBigintValuesUsingBitmask: {
      auto values =
          filter.as<velox::common::BigintValuesUsingBitmask>()->values();
      return integerValues(type, column, std::move(values));
    }
    case velox::common::FilterKind::kNegatedBigintValuesUsingHashTable: {
      const auto& values =
          filter.as<velox::common::NegatedBigintValuesUsingHashTable>()
              ->values();
      return negate(integerValues(type, column, values));
    }
    case velox::common::FilterKind::kNegatedBigintValuesUsingBitmask: {
      auto values =
          filter.as<velox::common::NegatedBigintValuesUsingBitmask>()->values();
      return negate(integerValues(type, column, std::move(values)));
    }
    case velox::common::FilterKind::kBigintMultiRange: {
      const auto& ranges =
          filter.as<velox::common::BigintMultiRange>()->ranges();
      std::vector<VortexExpressionPtr> expressions;
      expressions.reserve(ranges.size());
      for (const auto& range : ranges) {
        expressions.push_back(
            integerRange(type, column, range->lower(), range->upper()));
      }
      return disjunction(std::move(expressions));
    }
    case velox::common::FilterKind::kFloatRange:
      return floatingRange(
          type, column, *filter.as<velox::common::FloatRange>());
    case velox::common::FilterKind::kDoubleRange:
      return floatingRange(
          type, column, *filter.as<velox::common::DoubleRange>());
    case velox::common::FilterKind::kBytesRange:
      return bytesRange(type, column, *filter.as<velox::common::BytesRange>());
    case velox::common::FilterKind::kNegatedBytesRange:
      return negate(bytesRange(
          type,
          column,
          *filter.as<velox::common::NegatedBytesRange>()->getNonNegated()));
    case velox::common::FilterKind::kBytesValues:
      return bytesValues(
          type, column, filter.as<velox::common::BytesValues>()->values());
    case velox::common::FilterKind::kNegatedBytesValues:
      return negate(bytesValues(
          type,
          column,
          filter.as<velox::common::NegatedBytesValues>()->values()));
    case velox::common::FilterKind::kMultiRange: {
      if (type.kind() != TypeKind::REAL && type.kind() != TypeKind::DOUBLE &&
          type.kind() != TypeKind::VARCHAR &&
          type.kind() != TypeKind::VARBINARY) {
        return nullptr;
      }
      const auto& filters = filter.as<velox::common::MultiRange>()->filters();
      std::vector<VortexExpressionPtr> expressions;
      expressions.reserve(filters.size());
      for (const auto& child : filters) {
        auto converted = convertNonNullFilter(*child, type, column);
        if (converted == nullptr) {
          return nullptr;
        }
        expressions.push_back(std::move(converted));
      }
      return disjunction(std::move(expressions));
    }
    default:
      return nullptr;
  }
}

VortexExpressionPtr convertFilter(
    const velox::common::Filter& filter,
    const Type& type,
    const vx_expression* column) {
  if (!filter.isDeterministic()) {
    return nullptr;
  }
  if (type.isDate() || type.isTime() || type.isDecimal()) {
    return nullptr;
  }
  if (filter.kind() == velox::common::FilterKind::kIsNull) {
    return isNull(column);
  }
  if (filter.kind() == velox::common::FilterKind::kIsNotNull) {
    return isNotNull(column);
  }
  return applyNullSemantics(
      filter, column, convertNonNullFilter(filter, type, column));
}

void addResidualSubtree(
    const velox::common::ScanSpec& scanSpec,
    VortexFilterConversion& conversion) {
  if (scanSpec.filter() != nullptr) {
    conversion.residualFilters.push_back(&scanSpec);
  }
  for (const auto& child : scanSpec.children()) {
    addResidualSubtree(*child, conversion);
  }
}

void convertScanSpec(
    const velox::common::ScanSpec& scanSpec,
    const Type& type,
    const vx_expression* column,
    uint32_t depth,
    std::vector<VortexExpressionPtr>& expressions,
    VortexFilterConversion& conversion) {
  if (!scanSpec.readFromFile() || scanSpec.deltaUpdate() != nullptr) {
    addResidualSubtree(scanSpec, conversion);
    return;
  }
  if (depth > 1) {
    addResidualSubtree(scanSpec, conversion);
    return;
  }
  if (const auto* filter = scanSpec.filter()) {
    auto converted = convertFilter(*filter, type, column);
    if (converted == nullptr) {
      conversion.residualFilters.push_back(&scanSpec);
    } else {
      expressions.push_back(std::move(converted));
      conversion.pushedFilters.push_back(&scanSpec);
    }
  }

  if (scanSpec.children().empty()) {
    return;
  }
  if (!type.isRow()) {
    for (const auto& child : scanSpec.children()) {
      addResidualSubtree(*child, conversion);
    }
    return;
  }

  const auto& rowType = type.asRow();
  for (const auto& child : scanSpec.children()) {
    std::optional<column_index_t> childIndex;
    if (child->subscript() >= 0 && child->subscript() < rowType.size()) {
      childIndex = static_cast<column_index_t>(child->subscript());
    } else if (child->subscript() < 0) {
      childIndex = rowType.getChildIdxIfExists(child->fieldName());
    }
    if (!childIndex.has_value()) {
      addResidualSubtree(*child, conversion);
      continue;
    }
    const auto& fieldName = rowType.nameOf(childIndex.value());
    const vx_view fieldView{fieldName.data(), fieldName.size()};
    auto childColumn =
        expression(vx_velox_expression_get_item(fieldView, column));
    if (childColumn == nullptr) {
      addResidualSubtree(*child, conversion);
      continue;
    }
    convertScanSpec(
        *child,
        *rowType.childAt(childIndex.value()),
        childColumn.get(),
        depth + 1,
        expressions,
        conversion);
  }
}

} // namespace

void VortexExpressionDeleter::operator()(vx_expression* expression) const {
  if (expression != nullptr) {
    vx_velox_expression_free(expression);
  }
}

VortexFilterConversion convertVortexFilter(
    const velox::common::ScanSpec& scanSpec,
    const RowType& rowType) {
  VortexFilterConversion conversion;
  auto root = expression(vx_velox_expression_root());
  std::vector<VortexExpressionPtr> expressions;
  convertScanSpec(scanSpec, rowType, root.get(), 0, expressions, conversion);
  if (!expressions.empty()) {
    conversion.expression = conjunction(std::move(expressions));
  }
  return conversion;
}

std::vector<VortexMetadataFilterConversion> convertVortexMetadataFilters(
    const velox::common::ScanSpec& scanSpec,
    const RowType& rowType) {
  std::vector<VortexMetadataFilterConversion> conversions;
  auto root = expression(vx_velox_expression_root());
  for (const auto& child : scanSpec.children()) {
    if (!child->readFromFile() || child->deltaUpdate() != nullptr ||
        child->numMetadataFilters() == 0) {
      continue;
    }

    std::optional<column_index_t> sourceChannel;
    if (child->subscript() >= 0 && child->subscript() < rowType.size()) {
      sourceChannel = static_cast<column_index_t>(child->subscript());
    } else if (child->subscript() < 0) {
      sourceChannel = rowType.getChildIdxIfExists(child->fieldName());
    }
    if (!sourceChannel.has_value()) {
      continue;
    }

    const auto& fieldName = rowType.nameOf(sourceChannel.value());
    const vx_view fieldView{fieldName.data(), fieldName.size()};
    auto column =
        expression(vx_velox_expression_get_item(fieldView, root.get()));
    if (column == nullptr) {
      continue;
    }
    for (int i = 0; i < child->numMetadataFilters(); ++i) {
      auto converted = convertFilter(
          *child->metadataFilterAt(i),
          *rowType.childAt(sourceChannel.value()),
          column.get());
      if (converted == nullptr) {
        continue;
      }
      conversions.push_back(
          {child->metadataFilterNodeAt(i), std::move(converted)});
    }
  }
  return conversions;
}

} // namespace facebook::velox::dwio::vortex
