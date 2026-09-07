from pathlib import Path

import pytest

from datahub.testing.check_imports import (
    ban_direct_datahub_imports,
    ensure_no_indirect_model_imports,
)
from datahub.testing.check_str_enum import ensure_no_enum_mixin


def test_package_list_match_inits():
    # Every directory under src/ that holds Python modules must be an importable
    # regular package (have an __init__.py). Implemented without setuptools:
    # py3.12 venvs no longer seed it and it is intentionally not a dependency, so
    # pytest.importorskip would silently disable this guard in exactly those envs.
    src = Path(__file__).parent.parent.parent / "src"
    missing = set()
    for py_file in src.rglob("*.py"):
        directory = py_file.parent
        while directory != src:
            if not (directory / "__init__.py").exists():
                missing.add(str(directory.relative_to(src)))
            directory = directory.parent
    assert not missing, f"directories missing __init__.py: {sorted(missing)}"


def test_check_import_paths(pytestconfig: pytest.Config) -> None:
    root = pytestconfig.rootpath

    ensure_no_indirect_model_imports([root / "src", root / "tests"])
    ban_direct_datahub_imports([root / "src", root / "tests"])


def test_check_str_enum_usage(pytestconfig: pytest.Config) -> None:
    root = pytestconfig.rootpath

    ensure_no_enum_mixin([root / "src", root / "tests"])
