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

VortexNaturalSplit
split(uint64_t begin, uint64_t end, uint64_t assignmentByte) {
  return {{begin, end}, assignmentByte};
}

TEST(VortexSplitMapperTest, exactBoundaries) {
  const std::vector<VortexNaturalSplit> naturalSplits{
      split(0, 2, 0),
      split(2, 5, 3),
      split(5, 10, 7),
  };

  expectOwnedRange(VortexSplitMapper::map(naturalSplits, 0, 3), 0, 2);
  expectOwnedRange(VortexSplitMapper::map(naturalSplits, 3, 4), 2, 5);
  expectOwnedRange(VortexSplitMapper::map(naturalSplits, 7, 3), 5, 10);
}

TEST(VortexSplitMapperTest, emptyOwners) {
  const std::vector<VortexNaturalSplit> naturalSplits{
      split(0, 1, 0),
      split(1, 2, 6),
      split(2, 3, 10),
      split(3, 4, 14),
  };

  EXPECT_EQ(VortexSplitMapper::map(naturalSplits, 1, 3), std::nullopt);
  EXPECT_EQ(VortexSplitMapper::map(naturalSplits, 4, 0), std::nullopt);
}

TEST(VortexSplitMapperTest, zeroRowFile) {
  EXPECT_EQ(VortexSplitMapper::map({}, 0, 1), std::nullopt);
}

TEST(VortexSplitMapperTest, oneNaturalSplit) {
  const std::vector<VortexNaturalSplit> naturalSplits{split(0, 100, 0)};

  expectOwnedRange(VortexSplitMapper::map(naturalSplits, 0, 1), 0, 100);
  EXPECT_EQ(VortexSplitMapper::map(naturalSplits, 1, 999), std::nullopt);
}

TEST(VortexSplitMapperTest, adjacentByteTokensTileRows) {
  const std::vector<VortexNaturalSplit> naturalSplits{
      split(0, 1, 0),
      split(1, 4, 3),
      split(4, 10, 8),
      split(10, 13, 14),
  };

  const std::vector<std::optional<VortexRowRange>> ownedRowRanges{
      VortexSplitMapper::map(naturalSplits, 0, 4),
      VortexSplitMapper::map(naturalSplits, 4, 4),
      VortexSplitMapper::map(naturalSplits, 8, 4),
      VortexSplitMapper::map(naturalSplits, 12, 4),
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
  const std::vector<VortexNaturalSplit> naturalSplits{
      split(0, 1, 0),
      split(1, 2, 0),
      split(2, 3, 1),
      split(3, 4, 1),
  };

  expectOwnedRange(VortexSplitMapper::map(naturalSplits, 0, 1), 0, 2);
  expectOwnedRange(VortexSplitMapper::map(naturalSplits, 1, 1), 2, 4);
}

TEST(VortexSplitMapperTest, invalidNaturalRanges) {
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map({split(1, 4, 0)}, 0, 16),
      "must be ordered and contiguous");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map({split(0, 1, 0), split(2, 4, 8)}, 0, 16),
      "must be ordered and contiguous");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map({split(0, 3, 0), split(2, 4, 8)}, 0, 16),
      "must be ordered and contiguous");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(
          {split(0, 2, 0), split(2, 2, 8), split(2, 4, 12)}, 0, 16),
      "must be non-empty");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map({split(0, 2, 4)}, 0, 16),
      "first natural split assignment must be byte zero");
  VELOX_ASSERT_THROW(
      VortexSplitMapper::map(
          {split(0, 2, 0), split(2, 4, 8), split(4, 6, 7)}, 0, 16),
      "assignment bytes must be ordered");
}

} // namespace
} // namespace facebook::velox::dwio::vortex
