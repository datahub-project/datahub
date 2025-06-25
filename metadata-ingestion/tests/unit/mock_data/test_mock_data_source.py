from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.mock_data.datahub_mock_data import (
    DataHubMockDataConfig,
    DataHubMockDataSource,
    LineageConfig,
)
from datahub.metadata.schema_classes import StatusClass, UpstreamLineageClass
from datahub.metadata.urns import DatasetUrn


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
    workunits = list(source.get_workunits())

    # The mock source should not produce any workunits
    assert len(workunits) == 0


def test_mock_data_source_report():
    """Test that the mock data source has a report."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")

    source = DataHubMockDataSource(ctx, config)
    report = source.get_report()

    assert report is not None


def test_lineage_config_custom_values():
    """Test that LineageConfig can be configured with custom values."""
    lineage_config = LineageConfig(emit_lineage=True, lineage_fan_out=5, lineage_hops=3)
    assert lineage_config.emit_lineage is True
    assert lineage_config.lineage_fan_out == 5
    assert lineage_config.lineage_hops == 3


def test_generate_lineage_data_basic():
    """Test basic lineage data generation with default config."""
    config = DataHubMockDataConfig(
        lineage=LineageConfig(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data())

    # With fan_out=2, hops=1, we expect:
    # - Level 0: 1 table (2^0)
    # - Level 1: 2 tables (2^1)
    # Total tables: 3
    # Each table gets a status aspect
    # Level 0 table connects to 2 level 1 tables = 2 lineage aspects
    expected_workunits = 3 + 2  # status aspects + lineage aspects
    assert len(workunits) == expected_workunits


def test_generate_lineage_data_no_hops():
    """Test lineage data generation with 0 hops."""
    config = DataHubMockDataConfig(
        lineage=LineageConfig(emit_lineage=True, lineage_fan_out=3, lineage_hops=0)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data())

    # With hops=0, we expect only 1 table with no lineage
    assert len(workunits) == 1

    # Verify it's a status aspect
    workunit = workunits[0]
    assert workunit.metadata.aspect is not None
    assert isinstance(workunit.metadata.aspect, StatusClass)


def test_generate_lineage_data_large_fan_out():
    """Test lineage data generation with large fan out."""
    config = DataHubMockDataConfig(
        lineage=LineageConfig(emit_lineage=True, lineage_fan_out=4, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data())

    # With fan_out=4, hops=1:
    # - Level 0: 1 table
    # - Level 1: 4 tables
    # Total: 5 status aspects + 4 lineage aspects = 9 workunits
    assert len(workunits) == 9


def test_generate_lineage_data_table_naming():
    """Test that generated tables have correct naming pattern."""
    config = DataHubMockDataConfig(
        lineage=LineageConfig(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data())

    # Check that we have tables with expected naming pattern
    table_names = set()
    for workunit in workunits:
        if workunit.metadata.aspect and isinstance(
            workunit.metadata.aspect, StatusClass
        ):
            # Extract table name from URN using DatasetUrn
            urn = workunit.metadata.entityUrn
            dataset_urn = DatasetUrn.from_string(urn)
            table_name = dataset_urn.name
            table_names.add(table_name)

    expected_names = {
        "hops_1_f_2_h0_t0",  # Level 0, table 0
        "hops_1_f_2_h1_t0",  # Level 1, table 0
        "hops_1_f_2_h1_t1",  # Level 1, table 1
    }
    assert table_names == expected_names


def test_generate_lineage_data_lineage_relationships():
    """Test that lineage relationships are correctly established."""
    config = DataHubMockDataConfig(
        lineage=LineageConfig(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data())

    # Find lineage workunits
    lineage_workunits = [
        w
        for w in workunits
        if w.metadata.aspect and isinstance(w.metadata.aspect, UpstreamLineageClass)
    ]

    assert len(lineage_workunits) == 2

    # Check that level 0 table is upstream of both level 1 tables
    upstream_table_urn = None
    downstream_table_urns = set()

    for workunit in lineage_workunits:
        lineage = workunit.metadata.aspect
        assert len(lineage.upstreams) == 1
        upstream = lineage.upstreams[0]

        if upstream_table_urn is None:
            upstream_table_urn = upstream.dataset
        else:
            assert upstream.dataset == upstream_table_urn

        downstream_table_urns.add(workunit.metadata.entityUrn)

    # Verify upstream table name pattern
    upstream_dataset_urn = DatasetUrn.from_string(upstream_table_urn)
    upstream_table_name = upstream_dataset_urn.name
    assert upstream_table_name == "hops_1_f_2_h0_t0"

    # Verify downstream table name patterns
    downstream_names = {
        DatasetUrn.from_string(urn).name for urn in downstream_table_urns
    }
    expected_downstream = {"hops_1_f_2_h1_t0", "hops_1_f_2_h1_t1"}
    assert downstream_names == expected_downstream


def test_generate_lineage_data_disabled():
    """Test that no lineage data is generated when lineage is disabled."""
    config = DataHubMockDataConfig(
        lineage=LineageConfig(emit_lineage=False, lineage_fan_out=3, lineage_hops=2)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source.get_workunits())

    # Should not generate any workunits when lineage is disabled
    assert len(workunits) == 0


def test_get_status_aspect():
    """Test that status aspects are correctly created."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunit = source._get_status_aspect("test_table")

    # Check that the URN contains the correct table name
    dataset_urn = DatasetUrn.from_string(workunit.metadata.entityUrn)
    assert dataset_urn.name == "test_table"
    assert workunit.metadata.entityType == "dataset"
    assert isinstance(workunit.metadata.aspect, StatusClass)
    assert workunit.metadata.aspect.removed is False


def test_get_upstream_aspect():
    """Test that upstream lineage aspects are correctly created."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunit = source._get_upstream_aspect("upstream_table", "downstream_table")

    # Check that the URNs contain the correct table names
    dataset_urn = DatasetUrn.from_string(workunit.metadata.entityUrn)
    assert dataset_urn.name == "downstream_table"
    assert workunit.metadata.entityType == "dataset"
    assert isinstance(workunit.metadata.aspect, UpstreamLineageClass)

    lineage = workunit.metadata.aspect
    assert len(lineage.upstreams) == 1

    upstream = lineage.upstreams[0]
    upstream_dataset_urn = DatasetUrn.from_string(upstream.dataset)
    assert upstream_dataset_urn.name == "upstream_table"
    assert upstream.type == "TRANSFORMED"
