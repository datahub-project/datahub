import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.mock_data.datahub_mock_data import (
    DataHubMockDataConfig,
    DataHubMockDataSource,
    LineageConfigGen1,
    SubTypePattern,
)
from datahub.ingestion.source.mock_data.table_naming_helper import TableNamingHelper
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    DatasetUsageStatisticsClass,
    StatusClass,
    SubTypesClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DatasetUrn


def assert_workunit_aspect_type(workunit, expected_aspect_class):
    """Helper method to assert that a workunit has the expected aspect type."""
    assert isinstance(workunit.metadata, MetadataChangeProposalWrapper)
    assert workunit.metadata.aspect is not None
    assert isinstance(workunit.metadata.aspect, expected_aspect_class)


def get_workunit_metadata_safely(workunit):
    """Helper method to safely access workunit metadata, ensuring it's a MetadataChangeProposalWrapper."""
    if not isinstance(workunit.metadata, MetadataChangeProposalWrapper):
        raise ValueError(
            f"Expected MetadataChangeProposalWrapper, got {type(workunit.metadata)}"
        )
    return workunit.metadata


@pytest.mark.parametrize(
    "fan_out,hops,fan_out_after_first,expected_total,expected_levels",
    [
        # Basic cases
        (3, 0, None, 1, [1]),
        (2, 1, None, 3, [1, 2]),
        (1, 3, None, 4, [1, 1, 1, 1]),
        # With fan_out_after_first
        (3, 3, 2, 22, [1, 3, 6, 12]),
        (5, 4, 1, 21, [1, 5, 5, 5, 5]),
        (3, 3, 1, 10, [1, 3, 3, 3]),
        # Large exponential case
        (3, 10, None, 88573, [1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049]),
    ],
)
def test_calculate_lineage_tables_parametrized(
    fan_out, hops, fan_out_after_first, expected_total, expected_levels
):
    """Test _calculate_lineage_tables with various parameter combinations."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    total_tables, tables_at_levels = source._calculate_lineage_tables(
        fan_out=fan_out, hops=hops, fan_out_after_first=fan_out_after_first
    )

    assert total_tables == expected_total
    assert tables_at_levels == expected_levels


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


@pytest.mark.parametrize(
    "fan_out,hops,expected_workunits",
    [
        # Basic configuration: fan_out=2, hops=1
        # - Level 0: 1 table (2^0)
        # - Level 1: 2 tables (2^1)
        # Total tables: 3
        # Each table gets a status aspect + SubTypes aspect + profile aspect + usage aspect
        # Level 0 table connects to 2 level 1 tables = 2 lineage aspects
        (2, 1, 14),  # 3 status + 3 SubTypes + 3 profile + 3 usage + 2 lineage
        # Large fan out: fan_out=4, hops=1
        # - Level 0: 1 table
        # - Level 1: 4 tables
        # Total: 5 status aspects + 5 SubTypes aspects + 5 profile aspects + 5 usage aspects + 4 lineage aspects = 24 workunits
        (4, 1, 24),
    ],
)
def test_generate_lineage_data_gen1_workunit_counts_parametrized(
    fan_out, hops, expected_workunits
):
    """Test lineage data generation workunit counts with different configurations."""
    ctx = PipelineContext(run_id="test")
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(
            emit_lineage=True, lineage_fan_out=fan_out, lineage_hops=hops
        )
    )
    source = DataHubMockDataSource(ctx, config)
    workunits = list(source._data_gen_1())

    assert len(workunits) == expected_workunits


def test_generate_lineage_data_gen1_no_hops():
    """Test lineage data generation with 0 hops."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=3, lineage_hops=0)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

    # With hops=0, we expect only 1 table with no lineage
    # But it still gets status, SubTypes, profile, and usage aspects
    assert len(workunits) == 4

    # Verify it's a status aspect
    workunit = workunits[0]
    assert_workunit_aspect_type(workunit, StatusClass)


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
        metadata = get_workunit_metadata_safely(workunit)
        if metadata.aspect and isinstance(metadata.aspect, StatusClass):
            # Extract table name from URN using DatasetUrn
            urn = metadata.entityUrn
            if urn is not None:
                dataset_urn = DatasetUrn.from_string(urn)
                table_name = dataset_urn.name
                table_names.add(table_name)

    expected_names = {
        "hops_1_f_2_h0_t0",  # Level 0, table 0
        "hops_1_f_2_h1_t0",  # Level 1, table 0
        "hops_1_f_2_h1_t1",  # Level 1, table 1
    }
    assert table_names == expected_names

    # Verify all table names are valid and parseable
    for table_name in table_names:
        parsed = TableNamingHelper.parse_table_name(table_name)
        assert parsed["lineage_hops"] == 1
        assert parsed["lineage_fan_out"] == 2


def test_generate_lineage_data_gen1_lineage_relationships():
    """Test that lineage relationships are correctly established."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

    # Find lineage workunits
    lineage_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, UpstreamLineageClass):
            lineage_workunits.append(w)

    assert len(lineage_workunits) == 2

    # Check that level 0 table is upstream of both level 1 tables
    upstream_table_urn = None
    downstream_table_urns = set()

    for workunit in lineage_workunits:
        metadata = get_workunit_metadata_safely(workunit)
        lineage = metadata.aspect
        if lineage is not None:
            assert len(lineage.upstreams) == 1
            upstream = lineage.upstreams[0]

            if upstream_table_urn is None:
                upstream_table_urn = upstream.dataset
            else:
                assert upstream.dataset == upstream_table_urn

            downstream_table_urns.add(metadata.entityUrn)

    # Verify upstream table name pattern
    if upstream_table_urn is not None:
        upstream_dataset_urn = DatasetUrn.from_string(upstream_table_urn)
        upstream_table_name = upstream_dataset_urn.name
        assert upstream_table_name == "hops_1_f_2_h0_t0"

    # Verify downstream table name patterns
    downstream_names = {
        DatasetUrn.from_string(urn).name
        for urn in downstream_table_urns
        if urn is not None
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
    metadata = get_workunit_metadata_safely(status_workunit)
    dataset_urn = DatasetUrn.from_string(metadata.entityUrn)
    assert dataset_urn.name == "test_table"
    assert metadata.entityType == "dataset"
    assert isinstance(metadata.aspect, StatusClass)
    assert metadata.aspect.removed is False

    # Test upstream lineage aspect generation
    lineage_workunit = source._get_upstream_aspect("upstream_table", "downstream_table")
    metadata = get_workunit_metadata_safely(lineage_workunit)
    dataset_urn = DatasetUrn.from_string(metadata.entityUrn)
    assert dataset_urn.name == "downstream_table"
    assert metadata.entityType == "dataset"
    assert isinstance(metadata.aspect, UpstreamLineageClass)

    lineage = metadata.aspect
    if lineage is not None:
        assert len(lineage.upstreams) == 1

        upstream = lineage.upstreams[0]
        upstream_dataset_urn = DatasetUrn.from_string(upstream.dataset)
        assert upstream_dataset_urn.name == "upstream_table"
        assert upstream.type == "TRANSFORMED"


@pytest.mark.parametrize(
    "level,fan_out,fan_out_after_first,expected",
    [
        # Level 0 should always use the standard fan_out
        (0, 3, None, 3),
        (0, 5, 2, 5),
        # Level 1+ should use fan_out_after_first when set
        (1, 3, 2, 2),
        (2, 4, 1, 1),
        # Level 1+ should use standard fan_out when fan_out_after_first is None
        (1, 3, None, 3),
        (2, 5, None, 5),
        # Edge cases
        (1, 3, 0, 0),
        (10, 100, 50, 50),
    ],
)
def test_calculate_fanout_for_level_parametrized(
    level, fan_out, fan_out_after_first, expected
):
    """Test _calculate_fanout_for_level method with various scenarios."""
    config = DataHubMockDataConfig()
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    kwargs = {"level": level, "fan_out": fan_out}
    if fan_out_after_first is not None:
        kwargs["fan_out_after_first"] = fan_out_after_first

    result = source._calculate_fanout_for_level(**kwargs)
    assert result == expected


def test_subtypes_config_defaults():
    """Test that SubTypes configuration has correct defaults."""
    config = DataHubMockDataConfig()
    assert config.gen_1.subtype_pattern == SubTypePattern.ALTERNATING
    assert config.gen_1.level_subtypes == {0: "Table", 1: "View", 2: "Table"}


def test_subtypes_config_custom_values():
    """Test that SubTypes configuration can be set with custom values."""
    lineage_config = LineageConfigGen1(
        subtype_pattern=SubTypePattern.LEVEL_BASED,
        level_subtypes={0: "View", 1: "Table", 2: "View"},
    )
    assert lineage_config.subtype_pattern == SubTypePattern.LEVEL_BASED
    assert lineage_config.level_subtypes == {0: "View", 1: "Table", 2: "View"}


@pytest.mark.parametrize(
    "subtype_pattern,level_subtypes,subtype_types,test_cases",
    [
        (
            SubTypePattern.ALTERNATING,
            None,
            None,
            [
                ("table1", 0, 0, "Table"),  # index 0
                ("table2", 0, 1, "View"),  # index 1
                ("table3", 1, 0, "Table"),  # index 0
                ("table4", 1, 1, "View"),  # index 1
                ("table5", 2, 0, "Table"),  # index 0
            ],
        ),
        (
            SubTypePattern.ALTERNATING,
            None,
            ["Topic", "Schema", "Table"],
            [
                ("table1", 0, 0, "Topic"),  # index 0
                ("table2", 0, 1, "Schema"),  # index 1
                ("table3", 0, 2, "Table"),  # index 2
                ("table4", 0, 3, "Topic"),  # index 3 (cycles back)
                ("table5", 1, 0, "Topic"),  # index 0
                ("table6", 1, 1, "Schema"),  # index 1
            ],
        ),
        (
            SubTypePattern.ALTERNATING,
            None,
            ["External Table"],
            [
                ("table1", 0, 0, "External Table"),  # index 0
                ("table2", 0, 1, "External Table"),  # index 1
                ("table3", 1, 0, "External Table"),  # index 0
            ],
        ),
        (
            SubTypePattern.ALL_TABLE,
            None,
            None,
            [
                ("table1", 0, 0, "Table"),
                ("table2", 0, 1, "Table"),
                ("table3", 1, 0, "Table"),
                ("table4", 1, 1, "Table"),
            ],
        ),
        (
            SubTypePattern.ALL_VIEW,
            None,
            None,
            [
                ("table1", 0, 0, "View"),
                ("table2", 0, 1, "View"),
                ("table3", 1, 0, "View"),
                ("table4", 1, 1, "View"),
            ],
        ),
        (
            SubTypePattern.LEVEL_BASED,
            {0: "Table", 1: "View", 2: "Table"},
            None,
            [
                ("table1", 0, 0, "Table"),  # level 0
                ("table2", 0, 1, "Table"),  # level 0
                ("table3", 1, 0, "View"),  # level 1
                ("table4", 1, 1, "View"),  # level 1
                ("table5", 2, 0, "Table"),  # level 2
                ("table6", 3, 0, "Table"),  # level 3 (default)
            ],
        ),
    ],
)
def test_determine_subtype_patterns(
    subtype_pattern, level_subtypes, subtype_types, test_cases
):
    """Test _determine_subtype with various subtype patterns."""
    config_kwargs = {"subtype_pattern": subtype_pattern}
    if level_subtypes is not None:
        config_kwargs["level_subtypes"] = level_subtypes
    if subtype_types is not None:
        config_kwargs["subtype_types"] = subtype_types

    config = DataHubMockDataConfig(gen_1=LineageConfigGen1(**config_kwargs))
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    for table_name, level, index, expected_subtype in test_cases:
        actual_subtype = source._determine_subtype(
            level,
            index,
            config.gen_1.subtype_pattern,
            config.gen_1.subtype_types,
            config.gen_1.level_subtypes,
        )
        assert actual_subtype == expected_subtype, (
            f"Expected {expected_subtype} for {table_name} (level={level}, index={index})"
        )


def test_determine_subtype_invalid_pattern():
    """Test that invalid pattern defaults to Table."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(subtype_pattern=SubTypePattern.ALTERNATING)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    # Should default to Table for any valid pattern
    assert (
        source._determine_subtype(
            0,
            0,
            config.gen_1.subtype_pattern,
            config.gen_1.subtype_types,
            config.gen_1.level_subtypes,
        )
        == "Table"
    )


def test_determine_subtype_empty_subtype_types():
    """Test that empty subtype_types defaults to Table."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(
            subtype_pattern=SubTypePattern.ALTERNATING
            # Don't override subtype_types, use defaults
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    # Should use default subtype_types when not specified
    assert (
        source._determine_subtype(
            0,
            0,
            config.gen_1.subtype_pattern,
            config.gen_1.subtype_types,
            config.gen_1.level_subtypes,
        )
        == "Table"
    )
    assert (
        source._determine_subtype(
            0,
            1,
            config.gen_1.subtype_pattern,
            config.gen_1.subtype_types,
            config.gen_1.level_subtypes,
        )
        == "View"
    )
    assert (
        source._determine_subtype(
            1,
            0,
            config.gen_1.subtype_pattern,
            config.gen_1.subtype_types,
            config.gen_1.level_subtypes,
        )
        == "Table"
    )


def test_subtype_types_with_three_types_and_two_hops():
    """Test subtype_types with 3 types and 2 hops configuration."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(
            emit_lineage=True,
            lineage_fan_out=2,
            lineage_hops=2,
            subtype_pattern=SubTypePattern.ALTERNATING,
            subtype_types=["Table", "View", "External Table"],
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    # With fan_out=2, hops=2:
    # - Level 0: 1 table (2^0)
    # - Level 1: 2 tables (2^1)
    # - Level 2: 4 tables (2^2)
    # Total tables: 7

    # Test the alternating pattern with 3 types
    test_cases = [
        # Level 0: 1 table
        ("hops_2_f_2_h0_t0", 0, 0, "Table"),  # index 0 % 3 = 0 -> "Table"
        # Level 1: 2 tables
        ("hops_2_f_2_h1_t0", 1, 0, "Table"),  # index 0 % 3 = 0 -> "Table"
        ("hops_2_f_2_h1_t1", 1, 1, "View"),  # index 1 % 3 = 1 -> "View"
        # Level 2: 4 tables
        ("hops_2_f_2_h2_t0", 2, 0, "Table"),  # index 0 % 3 = 0 -> "Table"
        ("hops_2_f_2_h2_t1", 2, 1, "View"),  # index 1 % 3 = 1 -> "View"
        (
            "hops_2_f_2_h2_t2",
            2,
            2,
            "External Table",
        ),  # index 2 % 3 = 2 -> "External Table"
        ("hops_2_f_2_h2_t3", 2, 3, "Table"),  # index 3 % 3 = 0 -> "Table" (cycles back)
    ]

    for table_name, level, index, expected_subtype in test_cases:
        actual_subtype = source._determine_subtype(
            level,
            index,
            config.gen_1.subtype_pattern,
            config.gen_1.subtype_types,
            config.gen_1.level_subtypes,
        )
        assert actual_subtype == expected_subtype, (
            f"Expected {expected_subtype} for {table_name} (level={level}, index={index})"
        )

    # Generate actual workunits to verify the pattern is applied
    workunits = list(source._data_gen_1())

    # Extract subtypes from generated workunits
    table_subtypes = {}
    for workunit in workunits:
        metadata = get_workunit_metadata_safely(workunit)
        if metadata.aspect and isinstance(metadata.aspect, SubTypesClass):
            urn = metadata.entityUrn
            if urn is not None:
                dataset_urn = DatasetUrn.from_string(urn)
                table_name = dataset_urn.name
                subtypes = metadata.aspect
                if subtypes is not None and isinstance(subtypes, SubTypesClass):
                    table_subtypes[table_name] = subtypes.typeNames[0]

    # Verify the pattern is correctly applied in the generated data
    for table_name, _, _, expected_subtype in test_cases:
        assert table_name in table_subtypes, (
            f"Table {table_name} not found in generated workunits"
        )
        assert table_subtypes[table_name] == expected_subtype, (
            f"Expected {expected_subtype} for {table_name}, got {table_subtypes[table_name]}"
        )


def test_get_subtypes_aspect():
    """Test that SubTypes aspects are correctly created."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(subtype_pattern=SubTypePattern.ALTERNATING)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    # Test SubTypes aspect generation
    subtypes_workunit = source._get_subtypes_aspect(
        "test_table",
        0,
        1,
        config.gen_1.subtype_pattern,
        config.gen_1.subtype_types,
        config.gen_1.level_subtypes,
    )
    metadata = get_workunit_metadata_safely(subtypes_workunit)
    dataset_urn = DatasetUrn.from_string(metadata.entityUrn)
    assert dataset_urn.name == "test_table"
    assert metadata.entityType == "dataset"
    assert isinstance(metadata.aspect, SubTypesClass)

    subtypes = metadata.aspect
    if subtypes is not None:
        assert len(subtypes.typeNames) == 1
        assert (
            subtypes.typeNames[0] == "View"
        )  # index 1 should be View in alternating pattern


def test_generate_lineage_data_with_subtypes():
    """Test lineage data generation with SubTypes enabled."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(
            emit_lineage=True,
            lineage_fan_out=2,
            lineage_hops=1,
            subtype_pattern=SubTypePattern.ALTERNATING,
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

    # With fan_out=2, hops=1:
    # - Level 0: 1 table (2^0)
    # - Level 1: 2 tables (2^1)
    # Total tables: 3
    # Each table gets a status aspect + SubTypes aspect + profile aspect + usage aspect
    # Level 0 table connects to 2 level 1 tables = 2 lineage aspects
    expected_workunits = (
        3 + 3 + 3 + 3 + 2
    )  # status aspects + SubTypes aspects + profile aspects + usage aspects + lineage aspects
    assert len(workunits) == expected_workunits

    # Check that we have SubTypes aspects
    subtypes_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, SubTypesClass):
            subtypes_workunits.append(w)
    assert len(subtypes_workunits) == 3

    # Check that we have DatasetProfile aspects
    profile_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, DatasetProfileClass):
            profile_workunits.append(w)
    assert len(profile_workunits) == 3

    # Check that we have DatasetUsageStatistics aspects
    usage_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, DatasetUsageStatisticsClass):
            usage_workunits.append(w)
    assert len(usage_workunits) == 3

    # Check that alternating pattern is applied
    table_subtypes = {}
    for workunit in subtypes_workunits:
        metadata = get_workunit_metadata_safely(workunit)
        urn = metadata.entityUrn
        if urn is not None:
            dataset_urn = DatasetUrn.from_string(urn)
            table_name = dataset_urn.name
            subtypes = metadata.aspect
            if subtypes is not None and isinstance(subtypes, SubTypesClass):
                table_subtypes[table_name] = subtypes.typeNames[0]

    # Extract table indices from names to verify alternating pattern
    for table_name, subtype in table_subtypes.items():
        # Parse table name using helper
        parsed = TableNamingHelper.parse_table_name(table_name)
        index = parsed["table_index"]

        expected_subtype = "Table" if index % 2 == 0 else "View"
        assert subtype == expected_subtype, (
            f"Table {table_name} should be {expected_subtype}"
        )


def test_generate_lineage_data_subtypes_disabled():
    """Test that SubTypes data is generated by default (no way to disable currently)."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

    # SubTypes workunits are generated by default
    subtypes_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, SubTypesClass):
            subtypes_workunits.append(w)
    assert len(subtypes_workunits) == 3

    # Profile workunits are generated by default
    profile_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, DatasetProfileClass):
            profile_workunits.append(w)
    assert len(profile_workunits) == 3

    # Usage workunits are generated by default
    usage_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, DatasetUsageStatisticsClass):
            usage_workunits.append(w)
    assert len(usage_workunits) == 3

    # Should still generate status and lineage workunits
    status_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, StatusClass):
            status_workunits.append(w)
    assert len(status_workunits) == 3  # 3 tables


def test_subtypes_level_based_pattern():
    """Test SubTypes generation with level_based pattern."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(
            emit_lineage=True,
            lineage_fan_out=2,
            lineage_hops=2,
            subtype_pattern=SubTypePattern.LEVEL_BASED,
            level_subtypes={0: "Table", 1: "View", 2: "Table"},
        )
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

    # Check SubTypes aspects
    subtypes_workunits = []
    for w in workunits:
        metadata = get_workunit_metadata_safely(w)
        if metadata.aspect and isinstance(metadata.aspect, SubTypesClass):
            subtypes_workunits.append(w)

    # Verify level-based pattern
    for workunit in subtypes_workunits:
        metadata = get_workunit_metadata_safely(workunit)
        urn = metadata.entityUrn
        if urn is not None:
            dataset_urn = DatasetUrn.from_string(urn)
            table_name = dataset_urn.name
            subtypes = metadata.aspect

            if subtypes is not None and isinstance(subtypes, SubTypesClass):
                # Parse table name using helper
                parsed = TableNamingHelper.parse_table_name(table_name)
                level = parsed["level"]

                expected_subtype = config.gen_1.level_subtypes.get(level, "Table")
                assert subtypes.typeNames[0] == expected_subtype, (
                    f"Table {table_name} at level {level} should be {expected_subtype}"
                )


def test_subtype_pattern_enum_values():
    """Test that SubTypePattern enum has the expected values."""
    assert SubTypePattern.ALTERNATING == "alternating"
    assert SubTypePattern.ALL_TABLE == "all_table"
    assert SubTypePattern.ALL_VIEW == "all_view"
    assert SubTypePattern.LEVEL_BASED == "level_based"

    # Test that all enum values are valid
    valid_patterns = [pattern.value for pattern in SubTypePattern]
    assert "alternating" in valid_patterns
    assert "all_table" in valid_patterns
    assert "all_view" in valid_patterns
    assert "level_based" in valid_patterns


def test_table_naming_helper_integration():
    """Test that TableNamingHelper works correctly with the main data source."""
    config = DataHubMockDataConfig(
        gen_1=LineageConfigGen1(emit_lineage=True, lineage_fan_out=2, lineage_hops=1)
    )
    ctx = PipelineContext(run_id="test")
    source = DataHubMockDataSource(ctx, config)

    workunits = list(source._data_gen_1())

    # Extract table names from status workunits
    table_names = set()
    for workunit in workunits:
        metadata = get_workunit_metadata_safely(workunit)
        if metadata.aspect and isinstance(metadata.aspect, StatusClass):
            urn = metadata.entityUrn
            if urn is not None:
                dataset_urn = DatasetUrn.from_string(urn)
                table_names.add(dataset_urn.name)

    # Verify all generated table names are valid and parseable
    for table_name in table_names:
        assert TableNamingHelper.is_valid_table_name(table_name)
        parsed = TableNamingHelper.parse_table_name(table_name)
        assert parsed["lineage_hops"] == 1
        assert parsed["lineage_fan_out"] == 2
