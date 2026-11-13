from pathlib import Path

import pytest

from datahub.testing.check_imports import (
    ban_direct_datahub_imports,
    ensure_no_indirect_model_imports,
)
from datahub.testing.check_str_enum import ensure_no_enum_mixin


def test_package_list_match_inits():
    # Every dir under src/ must be a real package (have __init__.py), including
    # data-only dirs: resource_filename returns an unusable path for a PEP 420
    # namespace dir. Walk all dirs (not just ones containing .py files, which would
    # miss data-only dirs) and skip build artifacts. find_packages isn't used: it
    # false-positives on the built node_modules and needs setuptools, which py3.12
    # no longer seeds.
    src = Path(__file__).parent.parent.parent / "src"
    ignore = {"__pycache__", "node_modules"}
    missing = sorted(
        str(d.relative_to(src))
        for d in src.rglob("*")
        if d.is_dir()
        and not (d / "__init__.py").exists()
        and not any(
            part in ignore or part.endswith(".egg-info")
            for part in d.relative_to(src).parts
        )
    )
    assert not missing, f"directories missing __init__.py: {missing}"


def test_check_import_paths(pytestconfig: pytest.Config) -> None:
    root = pytestconfig.rootpath

    ensure_no_indirect_model_imports([root / "src", root / "tests"])
    ban_direct_datahub_imports([root / "src", root / "tests"])


def test_check_str_enum_usage(pytestconfig: pytest.Config) -> None:
    root = pytestconfig.rootpath

    ensure_no_enum_mixin([root / "src", root / "tests"])
