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


def test_calculate_lineage_tables_ten_hops_exponential():
    """Test _calculate_lineage_tables with 10 hops using exponential growth."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(
        fan_out=3, hops=10
    )

    # Calculate expected exponential behavior:
    # Level 0: 1 table (3^0)
    # Level 1: 3 tables (3^1)
    # Level 2: 9 tables (3^2)
    # Level 3: 27 tables (3^3)
    # Level 4: 81 tables (3^4)
    # Level 5: 243 tables (3^5)
    # Level 6: 729 tables (3^6)
    # Level 7: 2187 tables (3^7)
    # Level 8: 6561 tables (3^8)
    # Level 9: 19683 tables (3^9)
    # Level 10: 59049 tables (3^10)
    expected_levels = [1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049]
    expected_total = sum(expected_levels)  # 88573

    assert total_tables == expected_total
    assert tables_at_levels == expected_levels


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


def test_mock_data_source_basic_functionality():
    """Test basic mock data source functionality."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")

    # Test instantiation
    source = DataHubMockDataSource(ctx, config)
    assert source is not None
    assert source.config == config
    assert source.ctx == ctx

    # Test that no workunits are produced by default
    workunits = list(source.get_workunits())
    assert len(workunits) == 0

    # Test that report is available
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


def test_generate_lineage_data_gen1_workunit_counts():
    """Test lineage data generation workunit counts with different configurations."""
    ctx = PipelineContext(run_id="test")

    # Test basic configuration: fan_out=2, hops=1
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    source = DataHubMockDataSource(ctx, config)
    workunits = list(source._data_gen_1())

    # With fan_out=2, hops=1, we expect:
    # - Level 0: 1 table (2^0)
    # - Level 1: 2 tables (2^1)
    # Total tables: 3
    # Each table gets a status aspect
    # Level 0 table connects to 2 level 1 tables = 2 lineage aspects
    expected_workunits = 3 + 2  # status aspects + lineage aspects
    assert len(workunits) == expected_workunits

    # Test large fan out: fan_out=4, hops=1
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=4, lineage_hops=1)
    )
    source = DataHubMockDataSource(ctx, config)
    workunits = list(source._data_gen_1())

    # With fan_out=4, hops=1:
    # - Level 0: 1 table
    # - Level 1: 4 tables
    # Total: 5 status aspects + 4 lineage aspects = 9 workunits
    assert len(workunits) == 9


def test_generate_lineage_data_gen1_no_hops():
    """Test lineage data generation with 0 hops."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=3, lineage_hops=0)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

    # With hops=0, we expect only 1 table with no lineage
    assert len(workunits) == 1

    # Verify it's a status aspect
    workunit = workunits[0]
    assert workunit.metadata.aspect is not None
    assert isinstance(workunit.metadata.aspect, StatusClass)


def test_generate_lineage_data_gen1_table_naming():
    """Test that generated tables have correct naming pattern."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

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
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

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
        gen_1=LineageConfigGen1(emit_lineage=False, lineage_fan_out=3, lineage_hops=2)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source.get_workunits())

    # Should not generate any workunits when lineage is disabled
    assert len(workunits) == 0


def test_aspect_generation():
    """Test that status and upstream lineage aspects are correctly created."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    # Test status aspect generation
    status_workunit = source._get_status_aspect("test_table")
    dataset_urn = DatasetUrn.from_string(status_workunit.metadata.entityUrn)
    assert dataset_urn.name == "test_table"
    assert status_workunit.metadata.entityType == "dataset"
    assert isinstance(status_workunit.metadata.aspect, StatusClass)
    assert status_workunit.metadata.aspect.removed is False

    # Test upstream lineage aspect generation
    lineage_workunit = source._get_upstream_aspect("upstream_table", "downstream_table")
    dataset_urn = DatasetUrn.from_string(lineage_workunit.metadata.entityUrn)
    assert dataset_urn.name == "downstream_table"
    assert lineage_workunit.metadata.entityType == "dataset"
    assert isinstance(lineage_workunit.metadata.aspect, UpstreamLineageClass)

    lineage = lineage_workunit.metadata.aspect
    assert len(lineage.upstreams) == 1

    upstream = lineage.upstreams[0]
    upstream_dataset_urn = DatasetUrn.from_string(upstream.dataset)
    assert upstream_dataset_urn.name == "upstream_table"
    assert upstream.type == "TRANSFORMED"


def test_calculate_fanout_for_level():
    """Test _calculate_fanout_for_level method with various scenarios."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    # Level 0 should always use the standard fan_out
    assert source._calculate_fanout_for_level(level=0, fan_out=3) == 3
    assert (
        source._calculate_fanout_for_level(level=0, fan_out=5, fan_out_after_first=2)
        == 5
    )

    # Level 1+ should use fan_out_after_first when set
    assert (
        source._calculate_fanout_for_level(level=1, fan_out=3, fan_out_after_first=2)
        == 2
    )
    assert (
        source._calculate_fanout_for_level(level=2, fan_out=4, fan_out_after_first=1)
        == 1
    )

    # Level 1+ should use standard fan_out when fan_out_after_first is None
    assert source._calculate_fanout_for_level(level=1, fan_out=3) == 3
    assert source._calculate_fanout_for_level(level=2, fan_out=5) == 5

    # Edge cases
    assert (
        source._calculate_fanout_for_level(level=1, fan_out=3, fan_out_after_first=0)
        == 0
    )
    assert (
        source._calculate_fanout_for_level(
            level=10, fan_out=100, fan_out_after_first=50
        )
        == 50
    )
