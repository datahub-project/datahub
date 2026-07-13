"""Verify DATAHUB_SQLGLOT_DISABLE_C forces pure-Python sqlglot at startup.

Each test runs in a fresh interpreter (sqlglot's import state is process-wide
and cannot be reset once loaded), following the subprocess pattern used by
tests/unit/pgqueue/test_pgqueue_import_isolation.py.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

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


pytestmark = pytest.mark.skipif(
    not _sqlglotc_installed(),
    reason="sqlglot[c] compiled extensions are not installed",
)


def test_sqlglotc_loaded_by_default() -> None:
    result = _run(
        "from sqlglot import tokenizer_core\nprint(tokenizer_core.__file__)\n",
        disable_c=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith(".so")


def test_pure_python_when_disabled() -> None:
    result = _run(
        "import datahub._force_pure_python_sqlglot  # noqa: F401\n"
        "import sqlglot\n"
        "import sqlglot.lineage\n"
        "import sqlglot.optimizer\n"
        "from sqlglot import tokenizer_core\n"
        "so_modules = [\n"
        "    name for name, mod in sys.modules.items()\n"
        "    if name.startswith('sqlglot')\n"
        "    and getattr(mod, '__file__', '') and mod.__file__.endswith('.so')\n"
        "]\n"
        "print(tokenizer_core.__file__)\n"
        "print('SO_LEAKS:' + ','.join(sorted(so_modules)))\n",
        disable_c=True,
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.strip().splitlines()
    assert lines[0].endswith(".py")
    assert lines[1] == "SO_LEAKS:"


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


def test_hook_noop_when_env_unset() -> None:
    result = _run(
        "import datahub._force_pure_python_sqlglot  # noqa: F401\n"
        "from sqlglot import tokenizer_core\n"
        "print(tokenizer_core.__file__)\n",
        disable_c=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith(".so")
