from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.mock_data.datahub_mock_data import (
    DataHubMockDataConfig,
    DataHubMockDataSource,
    LineageConfigGen1,
)
from datahub.metadata.schema_classes import StatusClass, UpstreamLineageClass
from datahub.metadata.urns import DatasetUrn


def test_calculate_lineage_tables_zero_hops():
    """Test _calculate_lineage_tables with 0 hops (edge case)."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(fan_out=3, hops=0)

    assert total_tables == 1
    assert tables_at_levels == [1]


def test_calculate_lineage_tables_one_hop():
    """Test _calculate_lineage_tables with 1 hop."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(fan_out=2, hops=1)

    assert total_tables == 3  # 1 (level 0) + 2 (level 1)
    assert tables_at_levels == [1, 2]


def test_calculate_lineage_tables_two_hops_exponential():
    """Test _calculate_lineage_tables with 2 hops using exponential growth."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(fan_out=3, hops=2)

    assert total_tables == 13  # 1 (level 0) + 3 (level 1) + 9 (level 2)
    assert tables_at_levels == [1, 3, 9]


def test_calculate_lineage_tables_with_fan_out_after_first():
    """Test _calculate_lineage_tables with fan_out_after_first_hop set."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(
        fan_out=3, hops=3, fan_out_after_first=2
    )

    # Level 0: 1 table
    # Level 1: 3 tables (using fan_out)
    # Level 2: 3 * 2 = 6 tables (using fan_out_after_first)
    # Level 3: 6 * 2 = 12 tables (using fan_out_after_first)
    # Total: 1 + 3 + 6 + 12 = 22
    assert total_tables == 22
    assert tables_at_levels == [1, 3, 6, 12]


def test_calculate_lineage_tables_large_hops_with_limit():
    """Test _calculate_lineage_tables with large hops but limited fanout."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(
        fan_out=5, hops=4, fan_out_after_first=1
    )

    # Level 0: 1 table
    # Level 1: 5 tables (using fan_out)
    # Level 2: 5 * 1 = 5 tables (using fan_out_after_first)
    # Level 3: 5 * 1 = 5 tables (using fan_out_after_first)
    # Level 4: 5 * 1 = 5 tables (using fan_out_after_first)
    # Total: 1 + 5 + 5 + 5 + 5 = 21
    assert total_tables == 21
    assert tables_at_levels == [1, 5, 5, 5, 5]


def test_calculate_lineage_tables_fan_out_one():
    """Test _calculate_lineage_tables with fan_out=1."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(fan_out=1, hops=3)

    # Level 0: 1 table
    # Level 1: 1 table (1^1)
    # Level 2: 1 table (1^2)
    # Level 3: 1 table (1^3)
    # Total: 1 + 1 + 1 + 1 = 4
    assert total_tables == 4
    assert tables_at_levels == [1, 1, 1, 1]


def test_calculate_lineage_tables_fan_out_after_first_one():
    """Test _calculate_lineage_tables with fan_out_after_first=1."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(
        fan_out=3, hops=3, fan_out_after_first=1
    )

    # Level 0: 1 table
    # Level 1: 3 tables (using fan_out)
    # Level 2: 3 * 1 = 3 tables (using fan_out_after_first)
    # Level 3: 3 * 1 = 3 tables (using fan_out_after_first)
    # Total: 1 + 3 + 3 + 3 = 10
    assert total_tables == 10
    assert tables_at_levels == [1, 3, 3, 3]


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


def test_lineage_config_gen1_custom_values():
    """Test that LineageConfigGen1 can be configured with custom values."""
    lineage_config = LineageConfigGen1(
        emit_lineage=True,
        lineage_fan_out=5,
        lineage_hops=3,
        lineage_fan_out_after_first_hop=2,
    )
    assert lineage_config.emit_lineage is True
    assert lineage_config.lineage_fan_out == 5
    assert lineage_config.lineage_hops == 3
    assert lineage_config.lineage_fan_out_after_first_hop == 2


def test_generate_lineage_data_gen1_basic():
    """Test basic lineage data generation with default config."""
    config = DataHubMockDataConfig(
        lineage_gen_1=LineageConfigGen1(
            emit_lineage=True, lineage_fan_out=2, lineage_hops=1
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data_gen_1())

    # With fan_out=2, hops=1, we expect:
    # - Level 0: 1 table (2^0)
    # - Level 1: 2 tables (2^1)
    # Total tables: 3
    # Each table gets a status aspect
    # Level 0 table connects to 2 level 1 tables = 2 lineage aspects
    expected_workunits = 3 + 2  # status aspects + lineage aspects
    assert len(workunits) == expected_workunits


def test_generate_lineage_data_gen1_no_hops():
    """Test lineage data generation with 0 hops."""
    config = DataHubMockDataConfig(
        lineage_gen_1=LineageConfigGen1(
            emit_lineage=True, lineage_fan_out=3, lineage_hops=0
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data_gen_1())

    # With hops=0, we expect only 1 table with no lineage
    assert len(workunits) == 1

    # Verify it's a status aspect
    workunit = workunits[0]
    assert workunit.metadata.aspect is not None
    assert isinstance(workunit.metadata.aspect, StatusClass)


def test_generate_lineage_data_gen1_large_fan_out():
    """Test lineage data generation with large fan out."""
    config = DataHubMockDataConfig(
        lineage_gen_1=LineageConfigGen1(
            emit_lineage=True, lineage_fan_out=4, lineage_hops=1
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data_gen_1())

    # With fan_out=4, hops=1:
    # - Level 0: 1 table
    # - Level 1: 4 tables
    # Total: 5 status aspects + 4 lineage aspects = 9 workunits
    assert len(workunits) == 9


def test_generate_lineage_data_gen1_table_naming():
    """Test that generated tables have correct naming pattern."""
    config = DataHubMockDataConfig(
        lineage_gen_1=LineageConfigGen1(
            emit_lineage=True, lineage_fan_out=2, lineage_hops=1
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data_gen_1())

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


def test_generate_lineage_data_gen1_lineage_relationships():
    """Test that lineage relationships are correctly established."""
    config = DataHubMockDataConfig(
        lineage_gen_1=LineageConfigGen1(
            emit_lineage=True, lineage_fan_out=2, lineage_hops=1
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._generate_lineage_data_gen_1())

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


def test_generate_lineage_data_gen1_disabled():
    """Test that no lineage data is generated when lineage is disabled."""
    config = DataHubMockDataConfig(
        lineage_gen_1=LineageConfigGen1(
            emit_lineage=False, lineage_fan_out=3, lineage_hops=2
        )
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
