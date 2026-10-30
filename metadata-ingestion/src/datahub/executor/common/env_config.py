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
from typing import Optional

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


EXTRA_PIP_REQUIREMENTS_ENABLED_ENV = "INGESTION_EXTRA_PIP_REQUIREMENTS_ENABLED"
EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV = "INGESTION_EXTRA_PIP_REQUIREMENTS_ALLOWLIST"


def get_extra_pip_requirements_enabled() -> bool:
    return string_to_bool(os.environ.get(EXTRA_PIP_REQUIREMENTS_ENABLED_ENV, "true"))


def get_extra_pip_requirements_allowlist() -> Optional[str]:
    """The raw allowlist, or None when the operator has not set one (no restriction)."""
    value = os.environ.get(EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV)
    if value is None or not value.strip():
        return None
    return value
