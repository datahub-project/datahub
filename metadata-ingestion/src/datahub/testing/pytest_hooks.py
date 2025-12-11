# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import dataclasses
from typing import Optional

import pytest

__all__ = [
    "load_golden_flags",
    "get_golden_settings",
    "pytest_addoption",
    "GoldenFileSettings",
]


@dataclasses.dataclass
class GoldenFileSettings:
    update_golden: bool
    copy_output: bool


_registered: bool = False
_settings: Optional[GoldenFileSettings] = None


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-golden-files",
        action="store_true",
        default=False,
    )

    # TODO: Deprecate and remove this flag.
    parser.addoption("--copy-output-files", action="store_true", default=False)

    global _registered
    _registered = True


@pytest.fixture(scope="session", autouse=True)
def load_golden_flags(pytestconfig: pytest.Config) -> None:
    global _settings
    _settings = GoldenFileSettings(
        update_golden=pytestconfig.getoption("--update-golden-files"),
        copy_output=pytestconfig.getoption("--copy-output-files"),
    )


def get_golden_settings() -> GoldenFileSettings:
    if not _registered:
        raise ValueError(
            "Golden files aren't set up properly. Call register_golden_flags from a conftest pytest_addoptions method."
        )
    if not _settings:
        raise ValueError(
            "Golden files aren't set up properly. Ensure load_golden_flags is imported in your conftest."
        )
    return _settings
