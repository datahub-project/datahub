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

from datahub.configuration.env_var_registry import EnvVarGroup, env_var

# ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH keeps its original name on purpose:
# renaming it would silently change behaviour for deployments that already set it.
DEFAULT_GMS_PAYLOAD_MAX_LENGTH = 15368520


def string_to_bool(string: str) -> bool:
    return string.lower() == "true"


@env_var(
    name="ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH",
    type=int,
    group=EnvVarGroup.EXECUTOR,
    default=DEFAULT_GMS_PAYLOAD_MAX_LENGTH,
    description=(
        "Largest payload, in bytes, the executor will send to GMS in one request. Keeps its "
        "legacy ACRYL_ prefix so deployments that already set it keep working."
    ),
)
def get_payload_max_length() -> int:
    val = os.environ.get("ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH")
    return int(val) if val and val.isdigit() else DEFAULT_GMS_PAYLOAD_MAX_LENGTH


@env_var(
    name="DATAHUB_BUNDLED_VENV_PATH",
    type=str,
    group=EnvVarGroup.EXECUTOR,
    default="/opt/datahub/venvs",
    description="Directory holding the pre-built ingestion virtualenvs bundled into the executor image.",
)
def get_bundled_venv_path() -> str:
    return os.environ.get("DATAHUB_BUNDLED_VENV_PATH", "/opt/datahub/venvs")


@env_var(
    name="INGESTION_DEPENDENCY_RESOLUTION_ENABLED",
    type=bool,
    group=EnvVarGroup.EXECUTOR,
    default=True,
    choices=["true", "false"],
    description="Whether the executor resolves a recipe's plugin dependencies before running it.",
)
def get_dependency_resolution_enabled() -> bool:
    return string_to_bool(
        os.environ.get("INGESTION_DEPENDENCY_RESOLUTION_ENABLED", "true")
    )


@env_var(
    name="DATAHUB_EXECUTOR_PRINT_SUBPROCESS_LOGS",
    type=bool,
    group=EnvVarGroup.EXECUTOR,
    default=True,
    choices=["true", "false"],
    description="Whether the ingestion subprocess's own output is echoed into the executor's logs.",
)
def get_print_subprocess_logs() -> bool:
    return string_to_bool(
        os.environ.get("DATAHUB_EXECUTOR_PRINT_SUBPROCESS_LOGS", "true")
    )


EXTRA_PIP_REQUIREMENTS_ENABLED_ENV = "INGESTION_EXTRA_PIP_REQUIREMENTS_ENABLED"
EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV = "INGESTION_EXTRA_PIP_REQUIREMENTS_ALLOWLIST"


@env_var(
    name="INGESTION_EXTRA_PIP_REQUIREMENTS_ENABLED",
    type=bool,
    group=EnvVarGroup.EXECUTOR,
    default=True,
    choices=["true", "false"],
    description=(
        "Whether recipes may request extra pip requirements. Set to false to stop a recipe "
        "installing arbitrary packages into the run's virtualenv."
    ),
)
def get_extra_pip_requirements_enabled() -> bool:
    return string_to_bool(os.environ.get(EXTRA_PIP_REQUIREMENTS_ENABLED_ENV, "true"))


@env_var(
    name="INGESTION_EXTRA_PIP_REQUIREMENTS_ALLOWLIST",
    type=str,
    group=EnvVarGroup.EXECUTOR,
    description=(
        "Comma-separated package names a recipe's extra pip requirements are restricted to. "
        "Unset means no restriction."
    ),
)
def get_extra_pip_requirements_allowlist() -> Optional[str]:
    """The raw allowlist, or None when the operator has not set one (no restriction)."""
    value = os.environ.get(EXTRA_PIP_REQUIREMENTS_ALLOWLIST_ENV)
    if value is None or not value.strip():
        return None
    return value
