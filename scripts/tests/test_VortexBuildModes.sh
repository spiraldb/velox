#!/usr/bin/env bash

# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 VORTEX_SOURCE_DIR [WORK_DIR]" >&2
  exit 2
fi

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
VELOX_SOURCE_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
VORTEX_SOURCE_DIR=$(cd "$1" && pwd)
WORK_DIR=${2:-/tmp/velox-vortex-build-modes}
TEST_SOURCE_DIR="${VELOX_SOURCE_DIR}/CMake/tests/vortex-build-modes"

cmake -E remove_directory "${WORK_DIR}"
cmake -E make_directory "${WORK_DIR}"

cmake \
  -S "${TEST_SOURCE_DIR}" \
  -B "${WORK_DIR}/local" \
  -DVELOX_SOURCE_DIR="${VELOX_SOURCE_DIR}" \
  -DVELOX_VORTEX_SOURCE_DIR="${VORTEX_SOURCE_DIR}" \
  -DVortex_SOURCE=BUNDLED
cmake --build "${WORK_DIR}/local" --target vortex_build_mode_test
"${WORK_DIR}/local/vortex_build_mode_test"

cmake -E make_directory "${WORK_DIR}/installed/include" "${WORK_DIR}/installed/lib"
cmake -E copy \
  "${VORTEX_SOURCE_DIR}/vortex-velox/cinclude/vortex_velox.h" \
  "${WORK_DIR}/installed/include/vortex_velox.h"

if [[ "$(uname -s)" == "MINGW"* || "$(uname -s)" == "MSYS"* ]]; then
  VORTEX_LIBRARY=vortex_velox.lib
else
  VORTEX_LIBRARY=libvortex_velox.a
fi

cmake -E copy \
  "${WORK_DIR}/local/vortex-cargo/release/${VORTEX_LIBRARY}" \
  "${WORK_DIR}/installed/lib/${VORTEX_LIBRARY}"
cmake \
  -S "${TEST_SOURCE_DIR}" \
  -B "${WORK_DIR}/system" \
  -DVELOX_SOURCE_DIR="${VELOX_SOURCE_DIR}" \
  -DCMAKE_PREFIX_PATH="${WORK_DIR}/installed" \
  -DVortex_SOURCE=SYSTEM
cmake --build "${WORK_DIR}/system" --target vortex_build_mode_test
"${WORK_DIR}/system/vortex_build_mode_test"

cmake \
  -S "${TEST_SOURCE_DIR}" \
  -B "${WORK_DIR}/pinned" \
  -DVELOX_SOURCE_DIR="${VELOX_SOURCE_DIR}" \
  -DVortex_SOURCE=BUNDLED
cmake --build "${WORK_DIR}/pinned" --target vortex_build_mode_test
"${WORK_DIR}/pinned/vortex_build_mode_test"
