"""Verify DATAHUB_SQLGLOT_DISABLE_C forces pure-Python sqlglot at startup.

The end-to-end tests run in a fresh interpreter (sqlglot's import state is
process-wide and cannot be reset once loaded), following the subprocess pattern
used by tests/unit/pgqueue/test_pgqueue_import_isolation.py. They require the
real sqlglot[c] build, so they are skipped when it is not installed.

The unit tests below them exercise the finder and env-var logic directly,
in-process, against synthetic packages — no sqlglot install required.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

import datahub._force_pure_python_sqlglot as hook

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"


def _run(code: str, *, disable_c: bool) -> subprocess.CompletedProcess[str]:
    env_setup = (
        'import os; os.environ["DATAHUB_SQLGLOT_DISABLE_C"] = "1"\n'
        if disable_c
        else 'import os; os.environ.pop("DATAHUB_SQLGLOT_DISABLE_C", None)\n'
    )
    preamble = f"import sys; sys.path.insert(0, {SRC.as_posix()!r})\n{env_setup}"
    return subprocess.run(
        [sys.executable, "-c", preamble + code],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )


def _sqlglotc_installed() -> bool:
    result = _run(
        "from sqlglot import tokenizer_core\n"
        "print(tokenizer_core.__file__.endswith('.so'))\n",
        disable_c=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "True"


requires_sqlglotc = pytest.mark.skipif(
    not _sqlglotc_installed(),
    reason="sqlglot[c] compiled extensions are not installed",
)


@requires_sqlglotc
def test_sqlglotc_loaded_by_default() -> None:
    result = _run(
        "from sqlglot import tokenizer_core\nprint(tokenizer_core.__file__)\n",
        disable_c=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith(".so")


@requires_sqlglotc
def test_pure_python_when_disabled() -> None:
    # Import a spread of submodules, then report any sqlglot module that still
    # resolved to a compiled .so. tokenizer_core alone being .py is not enough:
    # the guard is that *no* sqlglot submodule slipped through to the C path.
    result = _run(
        "import datahub._force_pure_python_sqlglot  # noqa: F401\n"
        "import sqlglot\n"
        "import sqlglot.lineage\n"
        "import sqlglot.optimizer\n"
        "compiled = sorted(\n"
        "    name for name, mod in sys.modules.items()\n"
        "    if name.startswith('sqlglot')\n"
        "    and getattr(mod, '__file__', '') and mod.__file__.endswith('.so')\n"
        ")\n"
        "print('\\n'.join(compiled))\n",
        disable_c=True,
    )
    assert result.returncode == 0, result.stderr
    compiled = result.stdout.split()
    assert not compiled, f"sqlglot modules loaded from .so: {compiled}"


@requires_sqlglotc
def test_sqlglot_functional_in_pure_python_mode() -> None:
    result = _run(
        "import datahub._force_pure_python_sqlglot  # noqa: F401\n"
        "import sqlglot\n"
        "print(sqlglot.transpile('SELECT 1', write='postgres')[0])\n"
        "print(sqlglot.parse_one('SELECT a FROM t').sql())\n",
        disable_c=True,
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.strip().splitlines()
    assert lines[0] == "SELECT 1"
    assert lines[1] == "SELECT a FROM t"


@requires_sqlglotc
def test_hook_noop_when_env_unset() -> None:
    # Importing the hook without the env var must not touch imports at all: the
    # finder is never installed, and sqlglot still loads its compiled .so.
    result = _run(
        "from datahub._force_pure_python_sqlglot import _PurePythonSqlglotFinder\n"
        "assert not any(\n"
        "    isinstance(f, _PurePythonSqlglotFinder) for f in sys.meta_path\n"
        "), 'finder was installed despite env var being unset'\n"
        "from sqlglot import tokenizer_core\n"
        "print(tokenizer_core.__file__)\n",
        disable_c=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith(".so")


# ---------------------------------------------------------------------------
# In-process unit tests for the finder and env-var logic. These run against
# synthetic packages and need no sqlglot install, so they are not gated on
# requires_sqlglotc.
# ---------------------------------------------------------------------------


def _make_fake_sqlglot(tmp_path: Path) -> Path:
    pkg = tmp_path / "sqlglot"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "tokenizer_core.py").write_text("")
    # A compiled artifact that must be ignored in favour of the .py above.
    (pkg / "tokenizer_core.cpython-311-x86_64-linux-gnu.so").write_text("")
    return pkg


def test_finder_ignores_non_sqlglot_modules() -> None:
    finder = hook._PurePythonSqlglotFinder()
    assert finder.find_spec("os", None) is None
    # A prefix match without the dot boundary must not be intercepted.
    assert finder.find_spec("sqlglotx", None) is None


def test_finder_resolves_package_to_py(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pkg = _make_fake_sqlglot(tmp_path)
    # Top-level import: path is None, so the finder searches sys.path.
    monkeypatch.syspath_prepend(str(tmp_path))
    spec = hook._PurePythonSqlglotFinder().find_spec("sqlglot", None)
    assert spec is not None and spec.origin is not None
    assert spec.origin.endswith("__init__.py")
    assert spec.submodule_search_locations == [str(pkg)]


def test_finder_resolves_submodule_to_py(tmp_path: Path) -> None:
    pkg = _make_fake_sqlglot(tmp_path)
    # Submodule import: path is the parent package's __path__.
    spec = hook._PurePythonSqlglotFinder().find_spec(
        "sqlglot.tokenizer_core", [str(pkg)]
    )
    assert spec is not None and spec.origin is not None
    assert spec.origin.endswith("tokenizer_core.py")


def test_finder_returns_none_when_source_missing(tmp_path: Path) -> None:
    finder = hook._PurePythonSqlglotFinder()
    # Non-str path entries are skipped; a missing source falls through to None.
    assert finder.find_spec("sqlglot.missing", [object(), str(tmp_path)]) is None  # type: ignore[list-item]


def test_disable_flag_truthiness(monkeypatch: pytest.MonkeyPatch) -> None:
    for falsy in ("", "0", "false", "FALSE", "no"):
        monkeypatch.setenv("DATAHUB_SQLGLOT_DISABLE_C", falsy)
        assert not hook._sqlglot_c_disabled()
    for truthy in ("1", "true", "yes", "  1  "):
        monkeypatch.setenv("DATAHUB_SQLGLOT_DISABLE_C", truthy)
        assert hook._sqlglot_c_disabled()
    monkeypatch.delenv("DATAHUB_SQLGLOT_DISABLE_C", raising=False)
    assert not hook._sqlglot_c_disabled()


def test_install_inserts_finder_once_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATAHUB_SQLGLOT_DISABLE_C", "1")
    original = list(sys.meta_path)
    try:
        hook._install()
        installed = [
            f for f in sys.meta_path if isinstance(f, hook._PurePythonSqlglotFinder)
        ]
        assert len(installed) == 1
        assert sys.meta_path[0] is installed[0]
        # Idempotent: a second call must not add a duplicate.
        hook._install()
        assert (
            sum(isinstance(f, hook._PurePythonSqlglotFinder) for f in sys.meta_path)
            == 1
        )
    finally:
        sys.meta_path[:] = original


def test_install_noop_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAHUB_SQLGLOT_DISABLE_C", raising=False)
    original = list(sys.meta_path)
    try:
        hook._install()
        assert not any(
            isinstance(f, hook._PurePythonSqlglotFinder) for f in sys.meta_path
        )
    finally:
        sys.meta_path[:] = original
