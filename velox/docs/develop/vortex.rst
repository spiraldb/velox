##################
Vortex file format
##################

Velox reads Vortex files through the DWIO reader interface. The Hive and
Iceberg connectors select the reader through ``FileFormat::VORTEX``.

The integration has two ownership boundaries. Vortex owns its Rust scan API,
file semantics, expression binding, execution, and the versioned adapter ABI.
Velox owns DWIO, filter conversion, lazy vectors, value hooks, mutations, and
memory charges.

Build configuration
===================

Vortex support is optional. Enable it with ``VELOX_ENABLE_VORTEX``.

For local development, set ``VELOX_VORTEX_SOURCE_DIR`` to a Vortex checkout:

.. code-block:: bash

    cmake \
      -DVELOX_ENABLE_VORTEX=ON \
      -DVELOX_VORTEX_SOURCE_DIR=/path/to/vortex \
      -S . -B _build
    cmake --build _build --target velox_dwio_vortex_reader

The checkout must contain ``vortex-velox``. Cargo uses the Rust toolchain from
the Vortex ``rust-toolchain.toml`` file.

A bundled build accepts these cache variables:

* ``VELOX_VORTEX_BUILD_VERSION`` identifies the Vortex release.
* ``VELOX_VORTEX_SOURCE_URL`` overrides the release archive URL.
* ``VELOX_VORTEX_BUILD_SHA256_CHECKSUM`` verifies the source archive.

The release configuration must define a source URL and a SHA-256 checksum.
Velox uses ``cargo build --locked`` for the ``vortex-velox`` package.

Set ``Vortex_SOURCE=SYSTEM`` to use an installed adapter. The installation
must provide ``vortex_velox.h`` and the ``vortex_velox`` static library.

Run the build-mode test against a local Vortex checkout:

.. code-block:: bash

    scripts/tests/test_VortexBuildModes.sh /path/to/vortex

The test builds and runs a small ABI program in local, installed, and pinned
source modes.

Reader registration
===================

Register the Vortex reader before a Hive or Iceberg scan uses Vortex files:

.. code-block:: cpp

    facebook::velox::dwio::vortex::registerVortexReaderFactory();

Unregister the factory after all Vortex readers close:

.. code-block:: cpp

    facebook::velox::dwio::vortex::unregisterVortexReaderFactory();

Read path
=========

The reader maps each Velox byte split to the natural Vortex row splits. Each
natural split belongs to one Velox split, so concurrent scans do not duplicate
rows.

Vortex evaluates supported scalar filters and column projections. Velox keeps
unsupported filters as residual filters. Metadata proves split exclusion only
when Vortex can establish that no row matches.

The first reader does not expose file-level column statistics through the
DWIO reader interface. Vortex still uses natural-split statistics for pruning.

Output columns use ``LazyVector`` by default. Compatible scalar value hooks
consume selected rows without a temporary Velox vector. Native primitive
visits retain Vortex buffers, while the Arrow C Data path covers other types.

The reader supports row-number requests and Iceberg positional deletes. It
also preserves filter-only columns and nested constants during eager filter
evaluation.

Current scope
=============

This first integration provides a CPU reader. It does not provide a Vortex
writer or a direct CUDA path. Those paths require separate ownership and
performance contracts.

Validation includes randomized filters, nested data, metadata exclusion,
dynamic filters, Iceberg deletes, value hooks, and all TPC-H queries.
Vortex tests file compatibility through its versioned compatibility suite.

The optional ``velox_dwio_vortex_scan_benchmark`` compares Vortex and
Parquet scans. It also includes Nimble when ``VELOX_ENABLE_NIMBLE`` is on.
Enable both ``VELOX_ENABLE_BENCHMARKS`` and ``VELOX_ENABLE_PARQUET`` to build
the benchmark.
