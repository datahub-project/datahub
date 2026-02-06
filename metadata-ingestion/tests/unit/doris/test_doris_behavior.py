"""Tests for Doris connector behavioral aspects."""

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.doris import DorisConfig, DorisSource


def test_doris_config_defaults():
    """Verifies DorisConfig has correct defaults."""
    config = DorisConfig(host_port="localhost:9030")

    assert config.scheme == "doris+pymysql"
    assert config.include_stored_procedures is False


def test_doris_source_creation_idempotent():
    """Verifies DorisSource can be created multiple times without side effects."""
    config = DorisConfig(host_port="localhost:9030", database="test")

    for _ in range(5):
        source = DorisSource(ctx=PipelineContext(run_id="test"), config=config)
        assert source.config.scheme == "doris+pymysql"
