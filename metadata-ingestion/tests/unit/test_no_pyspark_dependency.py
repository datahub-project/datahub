"""Guardrail: PySpark/PyDeequ must not creep back into any ingestion extra.

Data-lake profiling (s3/gcs/abs) is now a pure-Python implementation
(pyarrow + Apache DataSketches), so nothing in the package should depend on
pyspark or pydeequ anymore. This test parses setup.py's declared extras and
fails if either dependency reappears in any of them.
"""

import runpy
import sys
import types
from pathlib import Path
from typing import Any, Dict

SETUP_PY = Path(__file__).parent.parent.parent / "setup.py"
FORBIDDEN = ("pyspark", "pydeequ")


def _extras_require() -> Dict[str, Any]:
    captured: Dict[str, Any] = {}

    def fake_setup(**kwargs: Any) -> None:
        captured.update(kwargs)

    # setup.py imports setuptools; py3.12 venvs no longer seed it and it is
    # intentionally not a dependency. Stub it, scoped to this exec, to capture
    # the setup() kwargs without a global sys.modules leak.
    stub = types.ModuleType("setuptools")
    stub.__dict__.update(
        setup=fake_setup,
        find_packages=lambda *a, **k: [],
        find_namespace_packages=lambda *a, **k: [],
    )
    saved = sys.modules.get("setuptools")
    sys.modules["setuptools"] = stub
    original_argv = sys.argv
    sys.argv = ["setup.py"]
    try:
        runpy.run_path(str(SETUP_PY), run_name="__main__")
    finally:
        if saved is None:
            sys.modules.pop("setuptools", None)
        else:
            sys.modules["setuptools"] = saved
        sys.argv = original_argv

    return captured["extras_require"]


def test_no_extra_pulls_pyspark_or_pydeequ() -> None:
    offenders = {
        extra: sorted(
            req for req in reqs if any(pkg in req.lower() for pkg in FORBIDDEN)
        )
        for extra, reqs in _extras_require().items()
    }
    offenders = {extra: reqs for extra, reqs in offenders.items() if reqs}

    assert offenders == {}, (
        f"pyspark/pydeequ must not be pulled by any extra, but found: {offenders}"
    )
