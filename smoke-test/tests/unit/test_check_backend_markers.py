"""Tests for .github/scripts/check_backend_markers.py on synthetic module sources."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[3] / ".github/scripts/check_backend_markers.py"
)


@pytest.fixture(scope="module")
def lint() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_backend_markers", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def capabilities(lint: ModuleType) -> dict[str, str]:
    return lint.load_capabilities()


def test_undeclared_kafka_and_docker_use_is_reported(
    lint: ModuleType, capabilities: dict[str, str]
) -> None:
    source = """
import subprocess
from tests.e2e.utils import get_kafka_broker_url

def test_x():
    get_kafka_broker_url()
    subprocess.run(["docker", "ps"])
"""
    problems = lint.check_source(source, capabilities)

    assert len(problems) == 2
    assert any("'kafka'" in p for p in problems)
    assert any("'docker'" in p for p in problems)


def test_requires_anywhere_in_the_module_satisfies_it(
    lint: ModuleType, capabilities: dict[str, str]
) -> None:
    source = """
import pytest
from utilities.backends import Capability
from tests.e2e.utils import get_db_url, get_kafka_schema_registry

def _helper():
    return get_kafka_schema_registry(), get_db_url()

@pytest.mark.parametrize("sink", ["rest", pytest.param("kafka", marks=pytest.mark.requires(Capability.KAFKA))])
def test_x(sink):
    _helper()

@pytest.mark.requires("sql_db")
def test_y():
    _helper()
"""
    assert lint.check_source(source, capabilities) == []


def test_unknown_capability_is_reported(
    lint: ModuleType, capabilities: dict[str, str]
) -> None:
    source = """
import pytest
from utilities.backends import Capability

pytestmark = pytest.mark.requires(Capability.KAFAK, "sql-db")
"""
    problems = lint.check_source(source, capabilities)

    assert len(problems) == 2
    assert all("unknown capability" in p for p in problems)


def test_docker_canary_must_be_p0(
    lint: ModuleType, capabilities: dict[str, str]
) -> None:
    source = """
import pytest

pytestmark = pytest.mark.p0

@pytest.mark.docker_canary
def test_inherits_p0():
    pass

class TestGroup:
    @pytest.mark.docker_canary
    def test_also_inherits_p0(self):
        pass
"""
    assert lint.check_source(source, capabilities) == []

    problems = lint.check_source(
        source.replace("pytestmark = pytest.mark.p0", ""), capabilities
    )
    assert len(problems) == 2
    assert all("docker_canary but not p0" in p for p in problems)
