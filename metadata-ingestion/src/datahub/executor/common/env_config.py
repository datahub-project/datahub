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

import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional, Union

logger = logging.getLogger(__name__)

# ACRYL_EXECUTOR_GMS_PAYLOAD_MAX_LENGTH keeps its original name on purpose:
# renaming it would silently change behaviour for deployments that already set it.
DEFAULT_GMS_PAYLOAD_MAX_LENGTH = 15368520


def string_to_bool(string: str) -> bool:
    return string.lower() == "true"


def env_to_bool(varname: str, default: str = "True") -> bool:
    val = os.getenv(varname)
    if val is None:
        return string_to_bool(default)
    return string_to_bool(val)


def env_to_int(varname: str, default_val: int) -> int:
    val = os.environ.get(varname)
    if val is None:
        return default_val
    if val.isdigit():
        return int(val)
    return default_val


def env_to_float(varname: str, default_val: float) -> float:
    val = os.environ.get(varname)
    if val is None:
        return default_val
    try:
        return float(val)
    except (ValueError, TypeError):
        logger.warning(
            f"Invalid value for {varname}: {val}, using default {default_val}"
        )
        return default_val


@dataclass(frozen=True)
class ReportedSetting:
    """A setting read from the environment, as this process parsed it."""

    env_name: str
    value: Union[bool, int, float]
    default: Union[bool, int, float]


def _report(
    registry: Dict[str, ReportedSetting],
    env_name: str,
    value: Union[bool, int, float],
    default: Union[bool, int, float],
) -> None:
    registry[env_name] = ReportedSetting(
        env_name=env_name, value=value, default=default
    )


def reported_int(
    registry: Dict[str, ReportedSetting], env_name: str, default: int
) -> int:
    value = env_to_int(env_name, default)
    _report(registry, env_name, value, default)
    return value


def reported_float(
    registry: Dict[str, ReportedSetting], env_name: str, default: float
) -> float:
    value = float(os.getenv(env_name, str(default)))
    _report(registry, env_name, value, default)
    return value


def reported_bool(
    registry: Dict[str, ReportedSetting], env_name: str, default: bool
) -> bool:
    value = env_to_bool(env_name, str(default))
    _report(registry, env_name, value, default)
    return value


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
