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

#include "velox/dwio/vortex/RegisterVortexReader.h"

#include "velox/dwio/vortex/VortexReader.h"

namespace facebook::velox::dwio::vortex {

std::unique_ptr<common::Reader> VortexReaderFactory::createReader(
    std::unique_ptr<common::BufferedInput> input,
    const common::ReaderOptions& options) {
  return std::make_unique<VortexReader>(std::move(input), options);
}

void registerVortexReaderFactory() {
  common::registerReaderFactory(std::make_shared<VortexReaderFactory>());
}

void unregisterVortexReaderFactory() {
  common::unregisterReaderFactory(common::FileFormat::VORTEX);
}

} // namespace facebook::velox::dwio::vortex
