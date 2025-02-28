import setuptools

from datahub.testing.check_imports import ensure_no_indirect_model_imports
from tests.utils import PytestConfig


def test_package_list_match_inits():
    where = "./src"
    package_list = set(setuptools.find_packages(where))
    namespace_packages = set(setuptools.find_namespace_packages(where))
    assert package_list == namespace_packages, "are you missing a package init file?"


def test_check_import_paths(pytestconfig: PytestConfig) -> None:
    root = pytestconfig.rootpath

    ensure_no_indirect_model_imports([root / "src", root / "tests"])
