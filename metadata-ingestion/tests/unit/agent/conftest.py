import pytest


@pytest.fixture(autouse=True)
def _verbose_logs_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests pin what the probe withholds, which an exported
    DATAHUB_PROBE_VERBOSE_LOGS (the local-debugging switch) turns off. A test
    of the switch sets it itself."""
    monkeypatch.delenv("DATAHUB_PROBE_VERBOSE_LOGS", raising=False)
