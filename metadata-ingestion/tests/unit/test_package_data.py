"""Resource files ship by default, and a wheel that drops one cannot be built.

`package_data = {"": ["*"]}` applies to every package, so a new file in a new
directory needs no declaration and the fact that package_data keys do not
recurse into subpackages cannot drop anything. The only human-maintained input
is `_EXCLUDE_PACKAGE_DATA` in setup.py, and a forgotten entry there ships an
extra README rather than dropping a runtime file. `_verified_build_py` then
refuses to produce a wheel that lost a file.

Four groups of tests:
  1. configuration  — the mechanism is still in place and consistent
  2. semantics      — the exclusion and verification functions behave
  3. review guard   — nothing excluded is referenced by runtime code
  4. end to end     — the real build hook, on the real tree and on a sabotaged one
"""

import ast
import fnmatch
import os
import pathlib
import subprocess
import sys
from typing import Iterator

if sys.version_info >= (3, 11):
    import tomllib

    def _load_toml(text: str) -> dict:
        return tomllib.loads(text)

else:  # `toml` is a core dependency of acryl-datahub; no extra test dependency needed
    import toml

    def _load_toml(text: str) -> dict:
        return toml.loads(text)


import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
_PKG_ROOT = _SRC / "datahub"


# --------------------------------------------------------------------------- helpers


def _setup_namespace() -> dict:
    """Execute setup.py with setup() neutralised — the same trick scripts/generate_pyproject_deps.py uses."""
    code = (
        (_ROOT / "setup.py")
        .read_text()
        .replace("setuptools.setup(", "_setup_args = dict(")
    )
    ns = {"__name__": "__not_main__", "__file__": str(_ROOT / "setup.py")}
    cwd = os.getcwd()
    os.chdir(_ROOT)  # setup.py opens ./src/datahub/_version.py relative to itself
    try:
        exec(compile(code, "setup.py", "exec"), ns)
    finally:
        os.chdir(cwd)
    return ns


@pytest.fixture(scope="module")
def setup_ns() -> dict:
    return _setup_namespace()


def _pyproject_table(name: str) -> dict:
    data = _load_toml((_ROOT / "pyproject.toml").read_text())
    return data["tool"]["setuptools"].get(name, {})


def _as_pyproject_keys(mapping: dict) -> dict:
    # setup.py spells "all packages" as ""; pyproject.toml spells it "*".
    return {("*" if k == "" else k): v for k, v in mapping.items()}


def _tree_files() -> dict:
    """{dotted package: [non-Python filenames]} for every directory under src/datahub.

    setup.py uses find_namespace_packages, so every directory is a package.
    """
    out = {}
    for path in [_PKG_ROOT, *sorted(p for p in _PKG_ROOT.rglob("*") if p.is_dir())]:
        if "__pycache__" in path.parts:
            continue
        files = [
            f.name
            for f in path.iterdir()
            if f.is_file() and not f.name.endswith((".py", ".pyc", ".pyi"))
        ]
        if files:
            out[".".join(path.relative_to(_SRC).parts)] = files
    return out


# --------------------------------------------------------------------------- 1. configuration


def test_ship_by_default_is_configured(setup_ns: dict) -> None:
    """Regressing to explicit per-package keys reintroduces the silent-drop bug."""
    assert setup_ns["_setup_args"]["package_data"] == {"": ["*"]}, (
        "setup.py must ship every resource file by default"
    )
    assert _pyproject_table("package-data") == {"*": ["*"]}, (
        "pyproject.toml overrides setup.py at build time; regenerate it with scripts/generate_pyproject_deps.py"
    )


def test_setup_py_and_pyproject_agree(setup_ns: dict) -> None:
    assert _as_pyproject_keys(
        setup_ns["_setup_args"]["exclude_package_data"]
    ) == _pyproject_table("exclude-package-data"), (
        "exclude_package_data has drifted between setup.py and pyproject.toml; regenerate pyproject.toml"
    )


def test_every_exclusion_still_matches_a_file(setup_ns: dict) -> None:
    """A per-package exclusion that matches nothing is a typo, a rename, or dead weight.

    The "" (all packages) entries describe source-file types rather than files
    present in a checkout, so they are not subject to this check.
    """
    tree = _tree_files()
    dead = [
        f"{pkg}: {pattern}"
        for pkg, patterns in setup_ns["_setup_args"]["exclude_package_data"].items()
        if pkg != ""
        for pattern in patterns
        if not any(fnmatch.fnmatch(f, pattern) for f in tree.get(pkg, []))
    ]
    assert not dead, f"exclude_package_data entries match no file in the tree: {dead}"


def test_build_verifier_is_wired(setup_ns: dict) -> None:
    """The build-time check is the last line of defence; make sure nobody unplugged it."""
    cmdclass = setup_ns["_setup_args"].get("cmdclass", {})
    assert cmdclass.get("build_py") is setup_ns["_verified_build_py"]
    assert cmdclass.get("sdist") is setup_ns["_verified_sdist"], (
        "the release path is sdist -> wheel; a file dropped at sdist time is "
        "invisible to the build_py check, so the sdist hook must stay wired"
    )


def test_sources_are_never_treated_as_resources(setup_ns: dict) -> None:
    """Python sources are copied by build_modules; listing them as data would double-copy."""
    assert {"*.py", "*.pyc", "*.pyi"} <= set(
        setup_ns["_setup_args"]["exclude_package_data"][""]
    )


# --------------------------------------------------------------------------- 2. semantics

_EXCLUDE = {
    "": ["*.py", ".*", "README.md"],
    "pkg.docs": ["*.md"],
    "pkg.a": ["notes.txt"],
}


@pytest.mark.parametrize(
    "package,name,expected",
    [
        ("pkg.anything", "README.md", True),  # "" applies to every package
        ("pkg.anything", ".gitignore", True),
        ("pkg.docs", "design.md", True),  # per-package pattern
        ("pkg.other", "design.md", False),  # ...does not leak into siblings
        ("pkg.a", "notes.txt", True),
        ("pkg.a.sub", "notes.txt", False),  # ...nor into subpackages
        ("pkg.a", "schema.json", False),
        ("pkg.docs", "README.md", True),  # both layers apply
    ],
)
def test_is_excluded(setup_ns: dict, package: str, name: str, expected: bool) -> None:
    assert setup_ns["_is_excluded"](_EXCLUDE, package, name) is expected


def _make_tree(root: pathlib.Path, files: dict) -> None:
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)


def _run_verifier(
    setup_ns: dict,
    src: pathlib.Path,
    build: pathlib.Path,
    packages: list,
    exclude: dict,
) -> list:
    return setup_ns["_missing_resource_files"](
        packages,
        lambda pkg: str(src / pkg.replace(".", "/")),
        exclude,
        lambda pkg, name: (build / pkg.replace(".", "/") / name).exists(),
    )


def test_verifier_with_sdist_style_file_list(
    setup_ns: dict, tmp_path: pathlib.Path
) -> None:
    """The sdist hook checks a manifest, not a directory — same rules, different predicate."""
    _make_tree(
        tmp_path / "src",
        {"pkg/__init__.py": "", "pkg/schema.json": "{}", "pkg/README.md": ""},
    )
    listed = {"src/pkg/__init__.py"}  # what the sdist manifest would contain
    missing = setup_ns["_missing_resource_files"](
        ["pkg"],
        lambda pkg: str(tmp_path / "src" / pkg.replace(".", "/")),
        _EXCLUDE,
        lambda pkg, name: f"src/{pkg.replace('.', '/')}/{name}" in listed,
    )
    assert missing == ["pkg: schema.json"]


def test_verifier_reports_dropped_resource(
    setup_ns: dict, tmp_path: pathlib.Path
) -> None:
    _make_tree(tmp_path / "src", {"pkg/__init__.py": "", "pkg/schema.json": "{}"})
    _make_tree(
        tmp_path / "build", {"pkg/__init__.py": ""}
    )  # schema.json did not make it
    assert _run_verifier(
        setup_ns, tmp_path / "src", tmp_path / "build", ["pkg"], _EXCLUDE
    ) == ["pkg: schema.json"]


def test_verifier_is_silent_when_build_is_complete(
    setup_ns: dict, tmp_path: pathlib.Path
) -> None:
    files = {"pkg/__init__.py": "", "pkg/schema.json": "{}", "pkg/data.yaml": "a: 1"}
    _make_tree(tmp_path / "src", files)
    _make_tree(tmp_path / "build", files)
    assert (
        _run_verifier(setup_ns, tmp_path / "src", tmp_path / "build", ["pkg"], _EXCLUDE)
        == []
    )


def test_verifier_honours_exclusions_and_ignores_sources(
    setup_ns: dict, tmp_path: pathlib.Path
) -> None:
    _make_tree(
        tmp_path / "src",
        {
            "pkg/docs/__init__.py": "",
            "pkg/docs/design.md": "",
            "pkg/docs/README.md": "",
            "pkg/docs/.hidden": "",
            "pkg/docs/mod.py": "",
        },
    )
    _make_tree(
        tmp_path / "build", {"pkg/docs/__init__.py": ""}
    )  # nothing else copied — and that is fine
    assert (
        _run_verifier(
            setup_ns, tmp_path / "src", tmp_path / "build", ["pkg.docs"], _EXCLUDE
        )
        == []
    )


def test_verifier_sees_nested_namespace_packages(
    setup_ns: dict, tmp_path: pathlib.Path
) -> None:
    """No __init__.py anywhere — find_namespace_packages still lists these, so the verifier must check them."""
    _make_tree(
        tmp_path / "src", {"pkg/deep/er/seed.csv": "a,b", "pkg/deep/er/mod.py": ""}
    )
    _make_tree(tmp_path / "build", {"pkg/deep/er/mod.py": ""})
    assert _run_verifier(
        setup_ns,
        tmp_path / "src",
        tmp_path / "build",
        ["pkg", "pkg.deep", "pkg.deep.er"],
        _EXCLUDE,
    ) == ["pkg.deep.er: seed.csv"]


def test_verifier_lists_every_missing_file_not_just_the_first(
    setup_ns: dict, tmp_path: pathlib.Path
) -> None:
    _make_tree(tmp_path / "src", {"a/x.json": "", "a/y.json": "", "b/z.gql": ""})
    _make_tree(tmp_path / "build", {"a/x.json": ""})
    assert _run_verifier(
        setup_ns, tmp_path / "src", tmp_path / "build", ["a", "b"], _EXCLUDE
    ) == ["a: y.json", "b: z.gql"]


# --------------------------------------------------------------------------- 3. review guard


def _code_string_constants(path: pathlib.Path) -> Iterator[str]:
    """String constants in code — docstrings excluded, since they may legitimately mention docs."""
    tree = ast.parse(path.read_text(), filename=str(path))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            if (
                node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
            ):
                docstrings.add(id(node.body[0].value))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
        ):
            yield node.value


def test_excluded_files_are_not_referenced_by_runtime_code(setup_ns: dict) -> None:
    """The one mistake ship-by-default cannot make benign: excluding something the code reads.

    If a source file names an excluded file in a string constant, that file is
    probably loaded at runtime and must not be excluded.
    """
    tree = _tree_files()
    excluded_names = {
        name
        for pkg, patterns in setup_ns["_setup_args"]["exclude_package_data"].items()
        if pkg != ""
        for name in tree.get(pkg, [])
        if any(fnmatch.fnmatch(name, p) for p in patterns)
    }
    offenders = []
    for py in sorted(_PKG_ROOT.rglob("*.py")):
        for const in _code_string_constants(py):
            hit = next((n for n in excluded_names if n in const), None)
            if hit:
                offenders.append(f"{py.relative_to(_ROOT)} mentions {hit!r}")
    assert not offenders, (
        "files listed in _EXCLUDE_PACKAGE_DATA are referenced from runtime code; "
        "they are almost certainly read at runtime and must ship:\n  "
        + "\n  ".join(offenders)
    )


# --------------------------------------------------------------------------- 4. end to end

_KNOWN_RESOURCES = [
    "datahub/cli/datapack/resources/registry.json",
    "datahub/cli/gql/search.gql",
    "datahub/ingestion/source/odcs/odcs_schema/odcs-v3.1.0.json",
    "datahub/ingestion/source/snowplow/snowplow_field_structured_properties.yaml",
    "datahub/ingestion/autogenerated/connector_registry/datahub.json",
]


def test_real_build_ships_known_resources(tmp_path: pathlib.Path) -> None:
    """Run the real build_py hook on the real tree: it must succeed and copy the historically dropped files."""
    build_lib = tmp_path / "lib"
    result = subprocess.run(
        [sys.executable, "setup.py", "-q", "build_py", "--build-lib", str(build_lib)],
        cwd=_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    missing = [f for f in _KNOWN_RESOURCES if not (build_lib / f).exists()]
    assert not missing, f"build_py did not copy: {missing}"
    assert not (build_lib / "datahub/ingestion/source/rdf/docs/README.md").exists(), (
        "excluded docs leaked into the build"
    )


def _mini_project(root: pathlib.Path, package_data_toml: str) -> None:
    """The real setup.py and pyproject.toml, a handful of resource files — enough to run build_py."""
    (root / "setup.py").write_text((_ROOT / "setup.py").read_text())
    for pth in _ROOT.glob(
        "*.pth"
    ):  # setup.py lists these under data_files; sdist copies them
        (root / pth.name).write_text(pth.read_text())
    _make_tree(
        root / "src",
        {
            "datahub/__init__.py": "",
            "datahub/_version.py": (_SRC / "datahub/_version.py").read_text(),
            "datahub/cli/gql/search.gql": "query { me { corpUser { username } } }",
            "datahub/cli/resources/INIT_AGENT_CONTEXT.md": "# ctx",
        },
    )
    pyproject = (_ROOT / "pyproject.toml").read_text()
    head, _, rest = pyproject.partition("[tool.setuptools.package-data]\n")
    assert rest, "pyproject.toml has no [tool.setuptools.package-data] table"
    _, _, tail = rest.partition("\n\n")  # drop the existing table body
    (root / "pyproject.toml").write_text(
        head + package_data_toml.rstrip() + "\n\n" + tail
    )


def _build(root: pathlib.Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            "setup.py",
            "-q",
            "build_py",
            "--build-lib",
            str(root / "lib"),
        ],
        cwd=root,
        capture_output=True,
        text=True,
    )


def test_build_refuses_when_pyproject_regresses_to_explicit_keys(
    tmp_path: pathlib.Path,
) -> None:
    """The exact historical failure: a key exists for one package, another is forgotten — the build must not succeed."""
    _mini_project(
        tmp_path, '[tool.setuptools.package-data]\n"datahub.cli.resources" = ["*.md"]'
    )
    result = _build(tmp_path)
    assert result.returncode != 0
    assert "refusing to build" in result.stderr
    assert "datahub.cli.gql: search.gql" in result.stderr
    assert not (tmp_path / "lib/datahub/cli/gql/search.gql").exists()


def test_sdist_refuses_when_pyproject_regresses_to_explicit_keys(
    tmp_path: pathlib.Path,
) -> None:
    """`python -m build` goes sdist -> wheel. The drop happens at sdist time, so the sdist must refuse."""
    _mini_project(
        tmp_path,
        '[tool.setuptools.package-data]\n"datahub.cli.resources" = ["*.md"]',
    )
    result = subprocess.run(
        [
            sys.executable,
            "setup.py",
            "-q",
            "sdist",
            "--dist-dir",
            str(tmp_path / "out"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert (
        "refusing to build" in result.stderr
        and "missing from the sdist" in result.stderr
    )
    assert "datahub.cli.gql: search.gql" in result.stderr
    assert (
        not list((tmp_path / "out").glob("*.tar.gz"))
        if (tmp_path / "out").exists()
        else True
    )


def test_sdist_to_wheel_path_ships_resources(tmp_path: pathlib.Path) -> None:
    """Control: correct configuration, the release path end to end — sdist, unpack, wheel from the sdist."""
    import tarfile
    import zipfile

    _mini_project(tmp_path, '[tool.setuptools.package-data]\n"*" = ["*"]')
    sdist = subprocess.run(
        [
            sys.executable,
            "setup.py",
            "-q",
            "sdist",
            "--dist-dir",
            str(tmp_path / "out"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert sdist.returncode == 0, sdist.stderr[-2000:]
    archive = next((tmp_path / "out").glob("*.tar.gz"))
    with tarfile.open(archive) as tf:
        members = [m.name for m in tf.getmembers()]
        assert any(m.endswith("src/datahub/cli/gql/search.gql") for m in members)
        # The `filter` argument exists from 3.12 (backported to 3.10.12 / 3.11.4);
        # feature-detect it rather than the interpreter version, as PEP 706 recommends.
        if hasattr(tarfile, "data_filter"):
            tf.extractall(tmp_path / "unpacked", filter="data")
        else:
            tf.extractall(tmp_path / "unpacked")
    unpacked = next((tmp_path / "unpacked").iterdir())
    wheel = subprocess.run(
        [
            sys.executable,
            "-c",
            "import setuptools.build_meta as b, sys; b.build_wheel(sys.argv[1])",
            str(tmp_path / "out"),
        ],
        cwd=unpacked,
        capture_output=True,
        text=True,
        check=False,
    )
    assert wheel.returncode == 0, wheel.stderr[-2000:]
    names = zipfile.ZipFile(next((tmp_path / "out").glob("*.whl"))).namelist()
    assert "datahub/cli/gql/search.gql" in names
    assert "datahub/cli/resources/INIT_AGENT_CONTEXT.md" in names


def test_editable_build_is_never_blocked(tmp_path: pathlib.Path) -> None:
    """PEP 660 editable installs serve files from the source tree; the verifier must stand aside.

    Same sabotaged configuration as the test above: build_wheel must refuse,
    build_editable must succeed — otherwise `pip install -e .` breaks for everyone.
    """
    _mini_project(
        tmp_path, '[tool.setuptools.package-data]\n"datahub.cli.resources" = ["*.md"]'
    )
    script = (
        "import setuptools.build_meta as b, sys\n"
        "kind = sys.argv[1]\n"
        "getattr(b, kind)(sys.argv[2])\n"
    )
    editable = subprocess.run(
        [sys.executable, "-c", script, "build_editable", str(tmp_path / "out")],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert editable.returncode == 0, editable.stderr[-2000:]
    wheel = subprocess.run(
        [sys.executable, "-c", script, "build_wheel", str(tmp_path / "out")],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert wheel.returncode != 0 and "refusing to build" in wheel.stderr


def test_build_succeeds_with_ship_by_default(tmp_path: pathlib.Path) -> None:
    """Control for the test above: same project, correct configuration."""
    _mini_project(tmp_path, '[tool.setuptools.package-data]\n"*" = ["*"]')
    result = _build(tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    assert (tmp_path / "lib/datahub/cli/gql/search.gql").exists()
    assert (tmp_path / "lib/datahub/cli/resources/INIT_AGENT_CONTEXT.md").exists()
