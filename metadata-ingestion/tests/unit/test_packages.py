import pytest
import setuptools

from datahub.testing.check_imports import (
    ban_direct_datahub_imports,
    ensure_no_indirect_model_imports,
)
from datahub.testing.check_str_enum import ensure_no_enum_mixin


def test_package_list_match_inits():
    where = "./src"
    package_list = set(setuptools.find_packages(where))
    namespace_packages = set(setuptools.find_namespace_packages(where))
    assert package_list == namespace_packages, "are you missing a package init file?"


def test_check_import_paths(pytestconfig: pytest.Config) -> None:
    root = pytestconfig.rootpath

    ensure_no_indirect_model_imports([root / "src", root / "tests"])
    ban_direct_datahub_imports([root / "src", root / "tests"])


def test_check_str_enum_usage(pytestconfig: pytest.Config) -> None:
    root = pytestconfig.rootpath

    ensure_no_enum_mixin([root / "src", root / "tests"])
