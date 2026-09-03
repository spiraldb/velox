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

include_guard(GLOBAL)

set(
  VELOX_VORTEX_BUILD_VERSION
  "1b34b18895f95fa5deaaef5e10d90415c27adf86"
  CACHE STRING
  "Vortex tag or commit for the bundled source archive."
)
set(VELOX_VORTEX_SOURCE_URL "" CACHE STRING "URL for the pinned Vortex source archive.")
set(
  VELOX_VORTEX_BUILD_SHA256_CHECKSUM
  "75fce40970bee64882945e8904f94d82411c2c3f936be97d0d53e6faa3da3251"
  CACHE STRING
  "SHA-256 checksum for the pinned Vortex source archive."
)

if(VELOX_VORTEX_SOURCE_DIR)
  get_filename_component(VORTEX_SOURCE_DIR "${VELOX_VORTEX_SOURCE_DIR}" ABSOLUTE)
  message(STATUS "Use local Vortex checkout: ${VORTEX_SOURCE_DIR}")
else()
  if(VELOX_VORTEX_BUILD_VERSION AND NOT VELOX_VORTEX_SOURCE_URL)
    set(
      VELOX_VORTEX_SOURCE_URL
      "https://github.com/vortex-data/vortex/archive/${VELOX_VORTEX_BUILD_VERSION}.tar.gz"
    )
  endif()

  if(NOT VELOX_VORTEX_SOURCE_URL AND DEFINED ENV{VELOX_VORTEX_URL})
    set(VELOX_VORTEX_SOURCE_URL "$ENV{VELOX_VORTEX_URL}")
  endif()
  if(NOT VELOX_VORTEX_SOURCE_URL)
    message(
      FATAL_ERROR
      "A bundled Vortex build requires VELOX_VORTEX_BUILD_VERSION, "
      "VELOX_VORTEX_SOURCE_URL, or VELOX_VORTEX_URL."
    )
  endif()

  velox_resolve_dependency_url(VORTEX)
  if(VELOX_VORTEX_BUILD_SHA256_CHECKSUM STREQUAL "SHA256=")
    message(
      FATAL_ERROR
      "A bundled Vortex build requires VELOX_VORTEX_BUILD_SHA256_CHECKSUM "
      "or VELOX_VORTEX_SHA256."
    )
  endif()

  FetchContent_Declare(
    vortex
    URL "${VELOX_VORTEX_SOURCE_URL}"
    URL_HASH "${VELOX_VORTEX_BUILD_SHA256_CHECKSUM}"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    EXCLUDE_FROM_ALL
  )
  FetchContent_MakeAvailable(vortex)
  set(VORTEX_SOURCE_DIR "${vortex_SOURCE_DIR}")
endif()

if(NOT EXISTS "${VORTEX_SOURCE_DIR}/Cargo.toml")
  message(FATAL_ERROR "Vortex source does not contain Cargo.toml: ${VORTEX_SOURCE_DIR}")
endif()
if(NOT EXISTS "${VORTEX_SOURCE_DIR}/Cargo.lock")
  message(FATAL_ERROR "Vortex source does not contain Cargo.lock: ${VORTEX_SOURCE_DIR}")
endif()
if(NOT EXISTS "${VORTEX_SOURCE_DIR}/vortex-velox/Cargo.toml")
  message(FATAL_ERROR "Vortex source does not contain the vortex-velox crate: ${VORTEX_SOURCE_DIR}")
endif()
if(NOT EXISTS "${VORTEX_SOURCE_DIR}/vortex-velox/cinclude/vortex_velox.h")
  message(FATAL_ERROR "Vortex source does not contain vortex_velox.h: ${VORTEX_SOURCE_DIR}")
endif()
find_program(
  VORTEX_CARGO_EXECUTABLE
  NAMES cargo
  HINTS "$ENV{HOME}/.cargo/bin"
  REQUIRED
  DOC "Cargo executable for Vortex"
)
mark_as_advanced(VORTEX_CARGO_EXECUTABLE)

get_filename_component(VORTEX_CARGO_BIN_DIR "${VORTEX_CARGO_EXECUTABLE}" DIRECTORY)
find_program(
  VORTEX_RUSTC_EXECUTABLE
  NAMES rustc
  HINTS "${VORTEX_CARGO_BIN_DIR}" "$ENV{HOME}/.cargo/bin"
  REQUIRED
  DOC "Rust compiler for Vortex"
)
mark_as_advanced(VORTEX_RUSTC_EXECUTABLE)

set(VORTEX_CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/vortex-cargo")
if(WIN32)
  set(VORTEX_VELOX_LIBRARY_NAME "vortex_velox.lib")
else()
  set(VORTEX_VELOX_LIBRARY_NAME "libvortex_velox.a")
endif()

set(
  VORTEX_CARGO_PROFILE
  "$<IF:$<CONFIG:Debug>,dev,$<IF:$<CONFIG:RelWithDebInfo>,release_debug,release>>"
)
set(
  VORTEX_CARGO_PROFILE_DIR
  "$<IF:$<CONFIG:Debug>,debug,$<IF:$<CONFIG:RelWithDebInfo>,release_debug,release>>"
)
set(
  VORTEX_VELOX_LIBRARY
  "${VORTEX_CARGO_TARGET_DIR}/${VORTEX_CARGO_PROFILE_DIR}/${VORTEX_VELOX_LIBRARY_NAME}"
)

set(VORTEX_CARGO_ENV --unset=MAKEFLAGS --unset=MFLAGS "RUSTC=${VORTEX_RUSTC_EXECUTABLE}")
if(APPLE AND CMAKE_OSX_DEPLOYMENT_TARGET)
  list(APPEND VORTEX_CARGO_ENV "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}")
endif()

add_custom_target(
  vortex_velox_cargo_build
  COMMAND
    "${CMAKE_COMMAND}" -E env ${VORTEX_CARGO_ENV} "${VORTEX_CARGO_EXECUTABLE}" build --locked
    --manifest-path "${VORTEX_SOURCE_DIR}/Cargo.toml" --package vortex-velox --target-dir
    "${VORTEX_CARGO_TARGET_DIR}" --profile "${VORTEX_CARGO_PROFILE}"
  BYPRODUCTS "${VORTEX_VELOX_LIBRARY}"
  WORKING_DIRECTORY "${VORTEX_SOURCE_DIR}"
  COMMENT "Build vortex-velox with Cargo"
  USES_TERMINAL
  VERBATIM
)

add_library(vortex_velox_rust STATIC IMPORTED GLOBAL)
add_dependencies(vortex_velox_rust vortex_velox_cargo_build)
set_target_properties(
  vortex_velox_rust
  PROPERTIES
    IMPORTED_LOCATION "${VORTEX_CARGO_TARGET_DIR}/release/${VORTEX_VELOX_LIBRARY_NAME}"
    IMPORTED_LOCATION_DEBUG "${VORTEX_CARGO_TARGET_DIR}/debug/${VORTEX_VELOX_LIBRARY_NAME}"
    IMPORTED_LOCATION_MINSIZEREL "${VORTEX_CARGO_TARGET_DIR}/release/${VORTEX_VELOX_LIBRARY_NAME}"
    IMPORTED_LOCATION_RELEASE "${VORTEX_CARGO_TARGET_DIR}/release/${VORTEX_VELOX_LIBRARY_NAME}"
    IMPORTED_LOCATION_RELWITHDEBINFO
      "${VORTEX_CARGO_TARGET_DIR}/release_debug/${VORTEX_VELOX_LIBRARY_NAME}"
    INTERFACE_INCLUDE_DIRECTORIES "${VORTEX_SOURCE_DIR}/vortex-velox/cinclude"
)
add_library(Vortex::vortex_velox ALIAS vortex_velox_rust)

install(FILES "${VORTEX_VELOX_LIBRARY}" DESTINATION lib COMPONENT velox_libraries)
install(
  FILES "${VORTEX_SOURCE_DIR}/vortex-velox/cinclude/vortex_velox.h"
  DESTINATION include
  COMPONENT velox_headers
)

find_package(Threads REQUIRED)
if(APPLE)
  find_library(VORTEX_COREFOUNDATION_FRAMEWORK CoreFoundation REQUIRED)
  find_library(VORTEX_ICONV_LIBRARY iconv REQUIRED)
  set_property(
    TARGET vortex_velox_rust
    APPEND
    PROPERTY INTERFACE_LINK_LIBRARIES "${VORTEX_COREFOUNDATION_FRAMEWORK};${VORTEX_ICONV_LIBRARY}"
  )
elseif(UNIX)
  set_property(
    TARGET vortex_velox_rust
    APPEND
    PROPERTY INTERFACE_LINK_LIBRARIES "Threads::Threads;${CMAKE_DL_LIBS};m;rt;util"
  )
elseif(WIN32)
  set_property(
    TARGET vortex_velox_rust
    APPEND
    PROPERTY INTERFACE_LINK_LIBRARIES "advapi32;bcrypt;ntdll;userenv;ws2_32"
  )
endif()

set(Vortex_FOUND TRUE)
set(Vortex_INCLUDE_DIRS "${VORTEX_SOURCE_DIR}/vortex-velox/cinclude")
set(Vortex_LIBRARIES Vortex::vortex_velox)
