"""Unit tests for Teradata OpenLineage patch import compatibility."""

import sys
import types
from collections.abc import Mapping
from unittest import mock

import pytest

from datahub_airflow_plugin.airflow3._teradata_openlineage_patch import (
    _create_teradata_openlineage_wrapper,
)

_EXTRACTORS = "airflow.providers.openlineage.extractors"
_EXTRACTORS_BASE = f"{_EXTRACTORS}.base"


class _FakeOperatorLineage:
    """Stand-in for airflow.providers.openlineage OperatorLineage."""

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


def _restore_modules(
    keys: tuple[str, ...], saved: Mapping[str, types.ModuleType]
) -> None:
    for key in keys:
        if key in saved:
            sys.modules[key] = saved[key]
        elif key in sys.modules:
            del sys.modules[key]


@pytest.fixture
def openlineage_sys_modules_sandbox():
    """Save and restore OpenLineage-related entries in sys.modules."""
    keys = (_EXTRACTORS, _EXTRACTORS_BASE)
    saved: dict[str, types.ModuleType] = {
        k: sys.modules[k] for k in keys if k in sys.modules
    }
    yield
    _restore_modules(keys, saved)


def test_create_wrapper_uses_primary_operator_lineage_import(
    openlineage_sys_modules_sandbox: None,
) -> None:
    """Airflow 3.x path: OperatorLineage from extractors package (covers primary import)."""
    ext = types.ModuleType(_EXTRACTORS)
    ext.OperatorLineage = _FakeOperatorLineage  # type: ignore[attr-defined]
    sys.modules[_EXTRACTORS] = ext
    if _EXTRACTORS_BASE in sys.modules:
        del sys.modules[_EXTRACTORS_BASE]

    original = mock.Mock()
    wrapper = _create_teradata_openlineage_wrapper(original)

    assert wrapper is not original


def test_create_wrapper_falls_back_to_base_operator_lineage_import(
    openlineage_sys_modules_sandbox: None,
) -> None:
    """Airflow 2.x provider path: OperatorLineage from extractors.base (covers fallback)."""
    ext = types.ModuleType(_EXTRACTORS)
    sys.modules[_EXTRACTORS] = ext

    base = types.ModuleType(_EXTRACTORS_BASE)
    base.OperatorLineage = _FakeOperatorLineage  # type: ignore[attr-defined]
    sys.modules[_EXTRACTORS_BASE] = base

    original = mock.Mock()
    wrapper = _create_teradata_openlineage_wrapper(original)

    assert wrapper is not original


def test_create_wrapper_returns_original_when_both_imports_fail(
    openlineage_sys_modules_sandbox: None,
) -> None:
    """If OperatorLineage cannot be resolved, the original callable is returned."""
    ext = types.ModuleType(_EXTRACTORS)
    sys.modules[_EXTRACTORS] = ext

    base = types.ModuleType(_EXTRACTORS_BASE)
    sys.modules[_EXTRACTORS_BASE] = base

    original = mock.Mock()
    wrapper = _create_teradata_openlineage_wrapper(original)

    assert wrapper is original
