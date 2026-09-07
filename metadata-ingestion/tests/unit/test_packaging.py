import importlib.metadata
import re
from pathlib import Path

import toml
from packaging.requirements import Requirement
from packaging.version import Version

import datahub._version as datahub_version

_METADATA_INGESTION = Path(__file__).parent.parent.parent


def test_datahub_version():
    # Checks that the installed distribution metadata is present and resolvable.
    assert importlib.metadata.version(datahub_version.__package_name__)


def test_setuptools_not_capped_below_83():
    # ING-3241 acceptance: acryl-datahub must not cap setuptools, so it installs
    # alongside setuptools>=83 (CVE-2026-59890). setup.py declares no cap; the
    # >=83 floor is enforced at lock time (pyproject [tool.uv]) and resolved in
    # constraints.txt. Guards against a cap being reintroduced.
    setup_py = (_METADATA_INGESTION / "setup.py").read_text()
    assert not re.search(r"setuptools\s*<", setup_py), (
        "setup.py must not cap setuptools (blocks setuptools>=83 / CVE-2026-59890)"
    )

    # Lock-time floor: pyproject [tool.uv] constraint-dependencies must floor
    # setuptools at >=83 (kept out of base_requirements for Airflow compat).
    pyproject = toml.load(_METADATA_INGESTION / "pyproject.toml")
    uv_constraints = pyproject["tool"]["uv"]["constraint-dependencies"]
    setuptools_reqs = [
        r for r in (Requirement(c) for c in uv_constraints) if r.name == "setuptools"
    ]
    assert setuptools_reqs, (
        "pyproject [tool.uv] constraint-dependencies must floor setuptools>=83"
    )
    for req in setuptools_reqs:
        assert req.specifier.contains("83.0.0") and not req.specifier.contains(
            "82.0.0"
        ), f"[tool.uv] setuptools constraint '{req}' does not floor at >=83"

    # Resolved lock: constraints.txt must land on >=83.
    constraints = (_METADATA_INGESTION / "constraints.txt").read_text()
    m = re.search(r"^setuptools==(\S+)", constraints, re.MULTILINE)
    assert m, "setuptools must be pinned in the locked constraints.txt"
    assert Version(m.group(1)) >= Version("83"), (
        f"constraints.txt locks setuptools=={m.group(1)}, below the >=83 floor"
    )
