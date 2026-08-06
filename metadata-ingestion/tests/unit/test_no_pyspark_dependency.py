"""Guardrail: PySpark/PyDeequ must not creep back into any ingestion extra.

Data-lake profiling (s3/gcs/abs) is now a pure-Python implementation
(pyarrow + Apache DataSketches), so nothing in the package should depend on
pyspark or pydeequ anymore. This test parses setup.py's declared extras and
fails if either dependency reappears in any of them.
"""

import runpy
import sys
from pathlib import Path
from typing import Any, Dict

import setuptools

SETUP_PY = Path(__file__).parent.parent.parent / "setup.py"
FORBIDDEN = ("pyspark", "pydeequ")


def _extras_require() -> Dict[str, Any]:
    captured: Dict[str, Any] = {}

    def fake_setup(**kwargs: Any) -> None:
        captured.update(kwargs)

    original = setuptools.setup
    setuptools.setup = fake_setup  # type: ignore[assignment]
    original_argv = sys.argv
    sys.argv = ["setup.py"]
    try:
        runpy.run_path(str(SETUP_PY), run_name="__main__")
    finally:
        setuptools.setup = original  # type: ignore[assignment]
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
