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

cmake_minimum_required(VERSION 3.28)
project(VortexBuildModeTest LANGUAGES CXX)

if(NOT VELOX_SOURCE_DIR)
  message(FATAL_ERROR "Set VELOX_SOURCE_DIR to the Velox source checkout.")
endif()

list(
  PREPEND
  CMAKE_MODULE_PATH
  "${VELOX_SOURCE_DIR}/CMake"
  "${VELOX_SOURCE_DIR}/CMake/resolve_dependency_modules"
)
include(ResolveDependency)

velox_resolve_dependency(Vortex REQUIRED)

add_executable(vortex_build_mode_test VortexBuildModeTest.cpp)
target_compile_features(vortex_build_mode_test PRIVATE cxx_std_20)
target_link_libraries(vortex_build_mode_test PRIVATE Vortex::vortex_velox)
