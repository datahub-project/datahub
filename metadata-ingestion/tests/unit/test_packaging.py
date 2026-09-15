import importlib.metadata
import re
from pathlib import Path

import pytest
import toml
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version

import datahub._version as datahub_version

_METADATA_INGESTION = Path(__file__).resolve().parent.parent.parent

# Sub-83 versions spanning the range so any relaxed floor is caught (>82 allows
# 82.5, !=82.* allows 81.x, >=78.1.1, etc.), not just ==82.0.0.
_SUB_83 = ("0.0.1", "78.1.1", "81.0.0", "82.0.0", "82.5.0", "82.99.0")


def _load_setup_args(monkeypatch):
    # Exec setup.py in-process (the loader lives in scripts/; setup.py reads
    # relative paths, so chdir into metadata-ingestion) and return the setup()
    # kwargs (install_requires list + extras_require dict).
    monkeypatch.chdir(_METADATA_INGESTION)
    monkeypatch.syspath_prepend(str(_METADATA_INGESTION / "scripts"))
    return importlib.import_module("generate_pyproject_deps").load_setup_py_variables()[
        "_setup_args"
    ]


def _declared_requirements(setup_args):
    # Every parseable Requirement across install_requires + all extras.
    declared = list(setup_args["install_requires"])
    for extra_reqs in setup_args["extras_require"].values():
        declared.extend(extra_reqs)
    reqs = []
    for dep in declared:
        try:
            reqs.append(Requirement(dep))
        except InvalidRequirement:
            continue
    return reqs


def _assert_allows_83(specifier: SpecifierSet, label: str) -> None:
    # Not-capped: must permit setuptools 83 (CVE-2026-59890 fix). A bare or
    # relaxed requirement is fine; only a cap that blocks 83 fails.
    assert specifier.contains("83.0.0"), (
        f"{label} caps setuptools below 83 (CVE-2026-59890)"
    )


def _assert_floored_at_83(specifier: SpecifierSet, label: str) -> None:
    # Enforced floor: permit 83 and reject every sub-83 version, so a relaxed
    # floor (>82, !=82.*, >=78.1.1, ...) is caught, not just ==82.0.0.
    _assert_allows_83(specifier, label)
    for below in _SUB_83:
        assert not specifier.contains(below), f"{label} allows sub-83 {below}"


def test_datahub_version():
    # version() raises if the distribution isn't installed; guards a broken/partial install.
    assert importlib.metadata.version(datahub_version.__package_name__)


def test_setuptools_not_capped_below_83(monkeypatch):
    # Acceptance: acryl-datahub must not cap setuptools, so it installs
    # alongside setuptools>=83 (CVE-2026-59890). setup.py declares no cap; the
    # >=83 floor is enforced at lock time (pyproject [tool.uv]) and resolved in
    # constraints.txt. Guards against a cap being reintroduced.

    # setup.py source of truth: parse the real requirement sets rather than
    # grepping text, so a cap in any operator form (==, ~=, !=, multi-clause)
    # is caught in install_requires and in every extra.
    for req in _declared_requirements(_load_setup_args(monkeypatch)):
        if req.name.lower() == "setuptools":
            _assert_allows_83(req.specifier, f"setup.py requirement '{req}'")

    # Lock-time floor: pyproject [tool.uv] constraint-dependencies must floor
    # setuptools at >=83 (kept out of base_requirements for Airflow compat).
    pyproject = toml.load(_METADATA_INGESTION / "pyproject.toml")
    uv_constraints = pyproject["tool"]["uv"]["constraint-dependencies"]
    setuptools_reqs = [
        r
        for r in (Requirement(c) for c in uv_constraints)
        if r.name.lower() == "setuptools"
    ]
    assert setuptools_reqs, (
        "pyproject [tool.uv] constraint-dependencies must floor setuptools>=83"
    )
    for req in setuptools_reqs:
        _assert_floored_at_83(req.specifier, f"[tool.uv] setuptools constraint '{req}'")

    # Resolved lock: constraints.txt must land on >=83.
    constraints = (_METADATA_INGESTION / "constraints.txt").read_text()
    m = re.search(r"^setuptools==(\S+)", constraints, re.MULTILINE)
    assert m, "setuptools must be pinned in the locked constraints.txt"
    assert Version(m.group(1)) >= Version("83"), (
        f"constraints.txt locks setuptools=={m.group(1)}, below the >=83 floor"
    )


def test_stopit_not_a_dependency(monkeypatch):
    # stopit is the permanent pkg_resources case (unmaintained, imports it at
    # load): its timeout code is vendored in datahub.utilities._stopit, so it
    # must not return as a dependency. The runtime shim would mask a re-add, so
    # guard it here. See datahub/utilities/threading_timeout.py.
    offenders = [
        str(req)
        for req in _declared_requirements(_load_setup_args(monkeypatch))
        if req.name.lower() == "stopit"
    ]
    assert not offenders, (
        "stopit must not be a dependency (its timeout code is vendored in "
        f"datahub.utilities._stopit); found: {offenders}"
    )


def test_sqlalchemy_stays_below_2_until_shim_removed():
    # Expiry tripwire for the pkg_resources shim. The shim exists only because
    # the redshift/cockroachdb SQLAlchemy dialects import pkg_resources at load;
    # both ship pkg_resources-free releases that require SQLAlchemy>=2. The
    # resolved lock is the authoritative signal for whether the shim is still
    # needed: some extras legitimately declare sqlalchemy<3.0, so a per-declared-
    # specifier check would false-trip; constraints.txt is the version actually
    # installed. When a sqlalchemy>=2 bump lands, this fails and names the cleanup.
    constraints = (_METADATA_INGESTION / "constraints.txt").read_text()
    m = re.search(r"^sqlalchemy==(\S+)", constraints, re.MULTILINE | re.IGNORECASE)
    assert m, "sqlalchemy must be pinned in the locked constraints.txt"
    assert Version(m.group(1)) < Version("2"), (
        f"sqlalchemy resolves to {m.group(1)} (>=2): sqlalchemy-redshift>=1.0.0 and "
        "sqlalchemy-cockroachdb>=2.0.4 ship pkg_resources-free releases. Remove the "
        "compatibility shim: src/datahub/utilities/pkg_resources_shim.py, "
        "src/datahub/_pkg_resources_finder.py, its import in src/datahub/__init__.py, "
        "tests/unit/utilities/test_pkg_resources_shim.py, and this test."
    )


def test_docker_ingestion_snippet_floors_setuptools():
    # setup.py delegates the CVE-2026-59890 setuptools floor to this Docker
    # snippet (wired into the datahub-actions image via UV_CONSTRAINT). Guard the
    # floor stays >=83 here so it can't silently regress. Wiring the same
    # constraint into the datahub-ingestion image is tracked separately.
    snippet_path = (
        _METADATA_INGESTION.parent / "docker/snippets/ingestion/constraints.txt"
    )
    if not snippet_path.exists():
        pytest.skip("docker snippet not present (partial checkout)")
    snippet = snippet_path.read_text()
    setuptools_reqs = []
    for raw in snippet.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            req = Requirement(line)
        except InvalidRequirement:
            continue
        if req.name.lower() == "setuptools":
            setuptools_reqs.append(req)
    assert setuptools_reqs, (
        "docker/snippets/ingestion/constraints.txt must floor setuptools>=83 "
        "(CVE-2026-59890)"
    )
    for req in setuptools_reqs:
        _assert_floored_at_83(req.specifier, f"Docker snippet '{req}'")
