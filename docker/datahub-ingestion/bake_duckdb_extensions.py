#!/usr/bin/env python3
"""Pre-install ("bake") the DuckDB extensions used by data-lake profiling.

Reads ``DATAHUB_DUCKDB_EXTENSION_DIRECTORY`` and ``DATAHUB_DUCKDB_BAKE_EXTENSIONS``
from the environment and ``INSTALL``s each extension into that directory. DuckDB
stores extensions under ``<dir>/v<duckdb-version>/<platform>/``, so this must run
with the same duckdb version/platform the image ships (it does — the image build
runs it right after installing the profiling deps, on the target platform).

At runtime the profiler points ``extension_directory`` at this same dir and issues
``INSTALL ... LOAD ...``; a matching baked binary makes ``INSTALL`` a no-network
no-op, so data-lake profiling works offline out of the box. Only a version/platform
mismatch (a hand-modified deployment) triggers a download.

Invoked from the datahub-ingestion Dockerfile (build-time, network available).
"""

import os

import duckdb

ext_dir = os.environ["DATAHUB_DUCKDB_EXTENSION_DIRECTORY"]
extensions = os.environ["DATAHUB_DUCKDB_BAKE_EXTENSIONS"].split()

os.makedirs(ext_dir, exist_ok=True)
conn = duckdb.connect()
# extension_directory does not accept a bind parameter; the value is a
# build-controlled path, so string interpolation is safe here.
conn.execute(f"SET extension_directory='{ext_dir}'")
for ext in extensions:
    conn.execute(f"INSTALL {ext}")
conn.close()

print(f"Baked DuckDB profiling extensions ({' '.join(extensions)}) into {ext_dir}")
