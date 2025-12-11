# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest
import setuptools

from datahub.testing.check_imports import ensure_no_indirect_model_imports


def test_package_list_match_inits():
    where = "./src"
    package_list = set(setuptools.find_packages(where))
    namespace_packages = set(setuptools.find_namespace_packages(where))
    assert package_list == namespace_packages, "are you missing a package init file?"


def test_check_import_paths(pytestconfig: pytest.Config) -> None:
    root = pytestconfig.rootpath

    ensure_no_indirect_model_imports([root / "src", root / "tests"])
