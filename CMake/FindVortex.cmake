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

include(FindPackageHandleStandardArgs)

find_path(Vortex_INCLUDE_DIR NAMES vortex_velox.h)
find_library(Vortex_LIBRARY NAMES vortex_velox)

find_package_handle_standard_args(Vortex REQUIRED_VARS Vortex_INCLUDE_DIR Vortex_LIBRARY)

mark_as_advanced(Vortex_INCLUDE_DIR Vortex_LIBRARY)

if(Vortex_FOUND AND NOT TARGET Vortex::vortex_velox)
  add_library(Vortex::vortex_velox STATIC IMPORTED)
  set_target_properties(
    Vortex::vortex_velox
    PROPERTIES
      IMPORTED_LOCATION "${Vortex_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${Vortex_INCLUDE_DIR}"
  )

  find_package(Threads REQUIRED)
  if(APPLE)
    find_library(Vortex_COREFOUNDATION_FRAMEWORK CoreFoundation REQUIRED)
    find_library(Vortex_ICONV_LIBRARY iconv REQUIRED)
    set_property(
      TARGET Vortex::vortex_velox
      APPEND
      PROPERTY INTERFACE_LINK_LIBRARIES "${Vortex_COREFOUNDATION_FRAMEWORK};${Vortex_ICONV_LIBRARY}"
    )
  elseif(UNIX)
    set_property(
      TARGET Vortex::vortex_velox
      APPEND
      PROPERTY INTERFACE_LINK_LIBRARIES "Threads::Threads;${CMAKE_DL_LIBS};m;rt;util"
    )
  elseif(WIN32)
    set_property(
      TARGET Vortex::vortex_velox
      APPEND
      PROPERTY INTERFACE_LINK_LIBRARIES "advapi32;bcrypt;ntdll;userenv;ws2_32"
    )
  endif()
endif()

set(Vortex_INCLUDE_DIRS "${Vortex_INCLUDE_DIR}")
set(Vortex_LIBRARIES Vortex::vortex_velox)
