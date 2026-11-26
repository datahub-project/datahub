from typing import Any

import pytest


class _FakeSession:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass


class _FakePycarloClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def __call__(self, query: str, variables: dict) -> dict:
        raise NotImplementedError("Unit tests must stub MonteCarloClient._call")


@pytest.fixture(autouse=True)
def fake_pycarlo(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    import types

    core = types.ModuleType("pycarlo.core")
    core.Client = _FakePycarloClient  # type: ignore[attr-defined]
    core.Session = _FakeSession  # type: ignore[attr-defined]
    pycarlo = types.ModuleType("pycarlo")
    pycarlo.core = core  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pycarlo", pycarlo)
    monkeypatch.setitem(sys.modules, "pycarlo.core", core)
