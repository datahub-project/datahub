# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import pathlib

# ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH keeps its original name on purpose:
# renaming it would silently change behaviour for deployments that already set it.
DEFAULT_GMS_PAYLOAD_MAX_LENGTH = 15368520


def string_to_bool(string: str) -> bool:
    return string.lower() == "true"


def get_payload_max_length() -> int:
    val = os.environ.get("ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH")
    return int(val) if val and val.isdigit() else DEFAULT_GMS_PAYLOAD_MAX_LENGTH


def get_bundled_venv_path() -> str:
    return os.environ.get("DATAHUB_BUNDLED_VENV_PATH", "/opt/datahub/venvs")


def get_dependency_resolution_enabled() -> bool:
    return string_to_bool(
        os.environ.get("INGESTION_DEPENDENCY_RESOLUTION_ENABLED", "true")
    )


def get_print_subprocess_logs() -> bool:
    return string_to_bool(
        os.environ.get("DATAHUB_EXECUTOR_PRINT_SUBPROCESS_LOGS", "true")
    )


def get_venv_cache_enabled() -> bool:
    """Whether named venvs are reused from a node-local cache.

    Defaults on: the speedup is the point, and a cache that ships off gets no
    soak. The switch exists because this changes behaviour for ingestion in
    customer deployments, and an operator who suspects the cache must be able
    to take it out of the picture without shipping new code.
    """
    return string_to_bool(os.environ.get("DATAHUB_VENV_CACHE_ENABLED", "true"))


def get_venv_cache_path(exec_out_dir: str) -> str:
    """Root for reusable venvs: a SIBLING of the per-execution directories.

    Deliberately not inside `exec_out_dir`. That is the directory
    finalize_task_output removes when a task ends, which is the very reason
    venvs are rebuilt every run. Both callers construct it as
    `{config.tmp_dir}/{exec_id}`, so its parent is the executor's configured
    tmp_dir -- the same volume, which is what keeps uv's hardlinking working.
    """
    override = os.environ.get("DATAHUB_VENV_CACHE_PATH")
    if override:
        return override
    return str(pathlib.Path(exec_out_dir).parent / "_venv_cache")


def get_venv_cache_max_bytes() -> int:
    """Eviction budget. Bytes, because venv sizes vary hugely by connector and
    a count limit would not bound disk at all."""
    raw = os.environ.get("DATAHUB_VENV_CACHE_MAX_GB", "20")
    try:
        return int(float(raw) * 1024**3)
    except ValueError:
        return 20 * 1024**3
