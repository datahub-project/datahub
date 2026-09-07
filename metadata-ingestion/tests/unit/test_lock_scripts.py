import importlib
import sys
from pathlib import Path

import pytest

METADATA_INGESTION_DIR = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = METADATA_INGESTION_DIR / "scripts"


@pytest.fixture()
def generate_pyproject_deps(monkeypatch):
    # The lock scripts live in scripts/ and run from metadata-ingestion/.
    monkeypatch.chdir(METADATA_INGESTION_DIR)
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    return importlib.import_module("generate_pyproject_deps")


def test_load_setup_py_variables_reads_dependency_sets(generate_pyproject_deps):
    ns = generate_pyproject_deps.load_setup_py_variables()
    assert "base_requirements" in ns
    assert "dev_requirements" in ns
    assert "_setup_args" in ns


def test_load_setup_py_variables_without_setuptools(
    generate_pyproject_deps, monkeypatch
):
    # Python 3.12 venvs no longer seed setuptools, so `import setuptools` in
    # setup.py fails. Parsing setup.py's variables must not depend on it.
    monkeypatch.setitem(sys.modules, "setuptools", None)
    ns = generate_pyproject_deps.load_setup_py_variables()
    assert "base_requirements" in ns
    assert "_setup_args" in ns
