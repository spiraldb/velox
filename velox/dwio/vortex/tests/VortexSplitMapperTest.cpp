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

#include "velox/dwio/vortex/VortexSplitMapper.h"

#include <limits>
#include <utility>

#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::dwio::vortex {
namespace {

void expectOwnedRange(
    const std::optional<VortexRowRange>& actual,
    uint64_t expectedBegin,
    uint64_t expectedEnd) {
  ASSERT_TRUE(actual.has_value());
  EXPECT_EQ(actual->begin, expectedBegin);
  EXPECT_EQ(actual->end, expectedEnd);
}

TEST(VortexSplitMapperTest, exactBoundaries) {
  const std::vector<VortexRowRange> naturalRowRanges{
      {0, 2},
      {2, 5},
      {5, 10},
  };

  expectOwnedRange(
      VortexSplitMapper::map(10, 10, naturalRowRanges, 0, 3), 0, 2);
  expectOwnedRange(
      VortexSplitMapper::map(10, 10, naturalRowRanges, 3, 4), 2, 5);
  expectOwnedRange(
      VortexSplitMapper::map(10, 10, naturalRowRanges, 7, 3), 5, 10);
}

TEST(VortexSplitMapperTest, emptyOwners) {
  const std::vector<VortexRowRange> naturalRowRanges{
      {0, 1},
      {1, 2},
      {2, 3},
      {3, 4},
  };

  EXPECT_EQ(
      VortexSplitMapper::map(4, 16, naturalRowRanges, 1, 3), std::nullopt);
  EXPECT_EQ(
      VortexSplitMapper::map(4, 16, naturalRowRanges, 4, 0), std::nullopt);
}

TEST(VortexSplitMapperTest, zeroRowFile) {
  EXPECT_EQ(VortexSplitMapper::map(0, 0, {}, 0, 1), std::nullopt);
  EXPECT_EQ(
      VortexSplitMapper::map(0, std::numeric_limits<uint64_t>::max(), {}, 0, 1),
      std::nullopt);
}

TEST(VortexSplitMapperTest, oneNaturalSplit) {
  const std::vector<VortexRowRange> naturalRowRanges{{0, 100}};

  expectOwnedRange(
      VortexSplitMapper::map(100, 1'000, naturalRowRanges, 0, 1), 0, 100);
  EXPECT_EQ(
      VortexSplitMapper::map(100, 1'000, naturalRowRanges, 1, 999),
      std::nullopt);
}

TEST(VortexSplitMapperTest, adjacentByteTokensTileRows) {
  const std::vector<VortexRowRange> naturalRowRanges{
      {0, 1},
      {1, 4},
      {4, 10},
      {10, 13},
  };

  const std::vector<std::optional<VortexRowRange>> ownedRowRanges{
      VortexSplitMapper::map(13, 16, naturalRowRanges, 0, 4),
      VortexSplitMapper::map(13, 16, naturalRowRanges, 4, 4),
      VortexSplitMapper::map(13, 16, naturalRowRanges, 8, 4),
      VortexSplitMapper::map(13, 16, naturalRowRanges, 12, 4),
  };

  expectOwnedRange(ownedRowRanges[0], 0, 4);
  EXPECT_EQ(ownedRowRanges[1], std::nullopt);
  expectOwnedRange(ownedRowRanges[2], 4, 10);
  expectOwnedRange(ownedRowRanges[3], 10, 13);

  uint64_t nextExpectedRow{0};
  for (const auto& ownedRowRange : ownedRowRanges) {
    if (!ownedRowRange.has_value()) {
      continue;
    }
    EXPECT_EQ(ownedRowRange->begin, nextExpectedRow);
    nextExpectedRow = ownedRowRange->end;
  }
  EXPECT_EQ(nextExpectedRow, 13);
}

TEST(VortexSplitMapperTest, collidingAssignmentBytesRemainTogether) {
  const std::vector<VortexRowRange> naturalRowRanges{
      {0, 1},
      {1, 2},
      {2, 3},
      {3, 4},
  };

  expectOwnedRange(VortexSplitMapper::map(4, 2, naturalRowRanges, 0, 1), 0, 2);
  expectOwnedRange(VortexSplitMapper::map(4, 2, naturalRowRanges, 1, 1), 2, 4);
}

TEST(VortexSplitMapperTest, invalidNaturalRanges) {
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(4, 16, {}, 0, 16), "must have natural row ranges");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(4, 16, {{1, 4}}, 0, 16),
      "must be ordered and contiguous");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(4, 16, {{0, 1}, {2, 4}}, 0, 16),
      "must be ordered and contiguous");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(4, 16, {{0, 3}, {2, 4}}, 0, 16),
      "must be ordered and contiguous");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(4, 16, {{0, 2}, {2, 2}, {2, 4}}, 0, 16),
      "must be non-empty");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(4, 16, {{0, 3}}, 0, 16),
      "must cover the file row count");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(0, 0, {{0, 1}}, 0, 1),
      "must be empty for a zero-row Vortex file");
}

} // namespace
} // namespace facebook::velox::dwio::vortex
