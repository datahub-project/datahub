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

# Sub-26.2 pip versions spanning every CVE window the floor closes: 26.0/26.0.1
# (CVE-2026-3219, CVE-2026-6357), 26.1/26.1.1 (CVE-2026-8643), 26.1.2
# (CVE-2026-13346). Sweeping the range catches a relaxed floor (>26, !=26.1.*,
# >=26.0, ...), not just ==26.0.
_SUB_26_2 = ("0.0.1", "25.3", "26.0", "26.0.1", "26.1", "26.1.1", "26.1.2")


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


def _pip_floor_ok(specifier: SpecifierSet, label: str) -> None:
    # Permit 26.2 and reject every sub-26.2 release, so a relaxed floor is
    # caught rather than only an exact-pin regression.
    assert specifier.contains("26.2"), f"{label} blocks pip 26.2 (CVE-2026-13346)"
    for below in _SUB_26_2:
        assert not specifier.contains(below), f"{label} allows sub-26.2 pip {below}"


def test_pip_floored_at_26_2_everywhere():
    # setup.py declares a bare "pip" (no upper bound: pip is a system tool), so
    # the CVE floor lives in two places that must not drift apart:
    #   - [tool.uv] constraint-dependencies, which resolves into constraints.txt
    #     and ships as datahub/constraints.txt (the executor constrains every
    #     venv it builds with it),
    #   - the Docker snippet, wired into the datahub-actions image via
    #     UV_CONSTRAINT and passed to the bundled venv builder.
    # Floor is 26.2: CVE-2026-3219 and CVE-2026-6357 (fixed 26.1), CVE-2026-8643
    # (fixed 26.1.2) and CVE-2026-13346 (fixed 26.2).
    pyproject = toml.load(_METADATA_INGESTION / "pyproject.toml")
    uv_constraints = pyproject["tool"]["uv"]["constraint-dependencies"]
    pip_reqs = [
        r for r in (Requirement(c) for c in uv_constraints) if r.name.lower() == "pip"
    ]
    assert pip_reqs, (
        "pyproject [tool.uv] constraint-dependencies must floor pip>=26.2 "
        "(CVE-2026-13346)"
    )
    for req in pip_reqs:
        _pip_floor_ok(req.specifier, f"[tool.uv] pip constraint '{req}'")

    # Resolved lock: the version the wheel actually ships to the executor.
    constraints = (_METADATA_INGESTION / "constraints.txt").read_text()
    m = re.search(r"^pip==(\S+)", constraints, re.MULTILINE)
    assert m, "pip must be pinned in the locked constraints.txt"
    assert Version(m.group(1)) >= Version("26.2"), (
        f"constraints.txt locks pip=={m.group(1)}, below the >=26.2 floor "
        "(CVE-2026-13346); re-run ./gradlew :metadata-ingestion:updateLockFile"
    )

    # Docker snippet: governs the bundled venvs, which install pip explicitly.
    snippet_path = (
        _METADATA_INGESTION.parent / "docker/snippets/ingestion/constraints.txt"
    )
    if not snippet_path.exists():
        pytest.skip("docker snippet not present (partial checkout)")
    snippet_reqs = []
    for raw in snippet_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            req = Requirement(line)
        except InvalidRequirement:
            continue
        if req.name.lower() == "pip":
            snippet_reqs.append(req)
    assert snippet_reqs, (
        "docker/snippets/ingestion/constraints.txt must floor pip>=26.2 "
        "(CVE-2026-13346)"
    )
    for req in snippet_reqs:
        _pip_floor_ok(req.specifier, f"Docker snippet '{req}'")
