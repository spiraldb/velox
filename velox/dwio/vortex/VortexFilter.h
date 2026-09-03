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
#include <vector>

#include "velox/dwio/common/MetadataFilter.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/vortex/VortexFfi.h"
#include "velox/type/Type.h"

namespace facebook::velox::dwio::vortex {

/// Releases a Vortex expression through the Vortex C API.
struct VortexExpressionDeleter {
  /// Releases expression when it is not null.
  void operator()(vx_velox_expression* expression) const;
};

/// Owns one Vortex expression.
using VortexExpressionPtr =
    std::unique_ptr<vx_velox_expression, VortexExpressionDeleter>;

/// Describes the exact and residual portions of a ScanSpec filter tree.
struct VortexFilterConversion {
  /// Owns the conjunction of all exactly converted filters.
  VortexExpressionPtr expression;

  /// Identifies ScanSpec nodes represented by expression.
  std::vector<const velox::common::ScanSpec*> pushedFilters;

  /// Identifies ScanSpec nodes that Velox must still evaluate.
  std::vector<const velox::common::ScanSpec*> residualFilters;

  /// Returns true when every active filter was converted exactly.
  bool fullyConverted() const {
    return residualFilters.empty();
  }
};

/// Describes one metadata-filter leaf that Vortex can evaluate safely.
struct VortexMetadataFilterConversion {
  /// Identifies the leaf in the Velox metadata expression tree.
  const velox::common::MetadataFilter::LeafNode* leaf;

  /// Owns the equivalent Vortex predicate.
  VortexExpressionPtr expression;
};

/// Converts exact scalar filters from scanSpec into one Vortex predicate.
///
/// The row type describes the physical Vortex schema used to bind literals.
/// Unsupported filters remain listed in residualFilters.
VortexFilterConversion convertVortexFilter(
    const velox::common::ScanSpec& scanSpec,
    const RowType& rowType);

/// Converts top-level metadata-filter leaves into Vortex predicates.
///
/// Unsupported and nested leaves remain absent. MetadataFilter preserves safe
/// Boolean semantics when it combines the available leaf results.
std::vector<VortexMetadataFilterConversion> convertVortexMetadataFilters(
    const velox::common::ScanSpec& scanSpec,
    const RowType& rowType);

} // namespace facebook::velox::dwio::vortex
