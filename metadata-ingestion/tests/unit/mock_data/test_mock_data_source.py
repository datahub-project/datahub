from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.mock_data.datahub_mock_data import (
    DataHubMockDataConfig,
    DataHubMockDataSource,
)


def test_mock_data_source_config():
    """Test that the mock data source configuration can be created."""
    config = DataHubMockDataConfig(enabled=True, description="Test mock data source")
    assert config.enabled is True
    assert config.description == "Test mock data source"


def test_mock_data_source_instantiation():
    """Test that the mock data source can be instantiated."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")

    source = DataHubMockDataSource(ctx, config)
    assert source is not None
    assert source.config == config
    assert source.ctx == ctx


def test_mock_data_source_no_workunits():
    """Test that the mock data source doesn't produce any workunits."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")

    source = DataHubMockDataSource(ctx, config)
    workunits = list(source.get_workunits_internal())

    # The mock source should not produce any workunits
    assert len(workunits) == 0


def test_mock_data_source_report():
    """Test that the mock data source has a report."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")

    source = DataHubMockDataSource(ctx, config)
    report = source.get_report()

    assert report is not None
