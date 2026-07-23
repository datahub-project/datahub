from types import SimpleNamespace

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.subtypes import DataFlowSubTypes

# The probe reuses the connector's own client factory, which lives in a module that
# imports requests/tenacity at import time — skip when the flink extra is absent.
pytest.importorskip("datahub.ingestion.source.flink.client")

import datahub.ingestion.source.flink.client as flink_client_mod
from datahub.ingestion.source.flink.flink_probe import list_flink_children


class _FakeJob:
    def __init__(self, name):
        self.name = name


class _FakeClient:
    def __init__(self, job_names):
        self._jobs = [_FakeJob(name) for name in job_names]
        self.closed = False

    def get_jobs_overview(self):
        return self._jobs

    def close(self):
        self.closed = True


def _config():
    return SimpleNamespace(
        job_name_pattern=AllowDenyPattern(allow=[".*"], deny=["^internal_.*"]),
        connection=SimpleNamespace(),
    )


def test_list_jobs_reuses_job_name_pattern_verdict(monkeypatch):
    client = _FakeClient(["orders_pipeline", "internal_metrics"])
    monkeypatch.setattr(flink_client_mod, "get_flink_client", lambda config: client)
    result = list_flink_children(_config(), [], 100)
    assert result.supported
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders_pipeline"].kind == DataFlowSubTypes.KINESIS_FIREHOSE_STREAM
    assert by_name["orders_pipeline"].pattern_field == "job_name_pattern"
    assert by_name["orders_pipeline"].included is True
    # The connector's own job_name_pattern deny drops internal jobs — reused, not
    # re-implemented.
    assert by_name["internal_metrics"].included is False
    assert by_name["internal_metrics"].excluded_by == "job_name_pattern"
    assert client.closed


def test_jobs_are_a_flat_level(monkeypatch):
    client = _FakeClient(["orders_pipeline"])
    monkeypatch.setattr(flink_client_mod, "get_flink_client", lambda config: client)
    # Jobs have no children, so a non-empty parent path lists nothing.
    result = list_flink_children(_config(), ["orders_pipeline"], 100)
    assert result.nodes == []
