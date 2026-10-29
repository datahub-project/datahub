from typing import Dict, Iterable, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub._version import __version__
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.redshift.redshift_schema import (
    REDSHIFT_QUERY_TAG_COMMENT_TEMPLATE,
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
    _add_redshift_query_tag,
)
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from tests.unit.redshift.conftest import make_source, run_extract_metadata


def redshift_source_setup(custom_props_flag: bool) -> Iterable[MetadataWorkUnit]:
    source = make_source(patch_custom_properties=custom_props_flag)
    gen = source.gen_dataset_workunits(
        table=RedshiftTable(
            name="category",
            columns=[],
            created=None,
            comment="",
        ),
        database="dev",
        schema="public",
        sub_type="test_sub_type",
        custom_properties={"my_key": "my_value"},
    )
    return gen


def test_gen_dataset_workunits_patch_custom_properties_patch():
    gen = redshift_source_setup(True)
    custom_props_exist = False
    for item in gen:
        mcp = item.metadata
        assert not isinstance(mcp, MetadataChangeEventClass)
        if mcp.aspectName == "datasetProperties":
            assert isinstance(mcp, MetadataChangeProposalClass)
            assert mcp.changeType == "PATCH"
            custom_props_exist = True
        else:
            assert isinstance(mcp, MetadataChangeProposalWrapper)

    assert custom_props_exist


def test_gen_dataset_workunits_patch_custom_properties_upsert():
    gen = redshift_source_setup(False)
    custom_props_exist = False
    for item in gen:
        assert isinstance(item.metadata, MetadataChangeProposalWrapper)
        mcp = item.metadata
        if mcp.aspectName == "datasetProperties":
            assert mcp.changeType == "UPSERT"
            custom_props_exist = True

    assert custom_props_exist


def test_add_redshift_query_tag() -> None:
    query = "SELECT * FROM test_table"
    tagged_query = _add_redshift_query_tag(query)
    expected_tag = REDSHIFT_QUERY_TAG_COMMENT_TEMPLATE.format(version=__version__)
    assert tagged_query == expected_tag + query


def test_add_redshift_query_tag_multiline_query_preserves_text() -> None:
    query = "SELECT 1 AS a\nUNION ALL\nSELECT 2 AS a\n"
    tagged_query = _add_redshift_query_tag(query)
    expected_tag = REDSHIFT_QUERY_TAG_COMMENT_TEMPLATE.format(version=__version__)
    assert tagged_query == expected_tag + query


def test_add_redshift_query_tag_existing_leading_comment() -> None:
    query = "-- existing comment\nSELECT * FROM test_table"
    tagged_query = _add_redshift_query_tag(query)
    expected_tag = REDSHIFT_QUERY_TAG_COMMENT_TEMPLATE.format(version=__version__)
    assert tagged_query == expected_tag + query


def test_add_redshift_query_tag_leading_whitespace_and_blank_lines() -> None:
    query = "\n\n  SELECT * FROM test_table"
    tagged_query = _add_redshift_query_tag(query)
    expected_tag = REDSHIFT_QUERY_TAG_COMMENT_TEMPLATE.format(version=__version__)
    assert tagged_query == expected_tag + query


def _gen_ownership_workunits(
    owner: Optional[str],
    extract_ownership: bool,
    email_domain: Optional[str] = None,
) -> list[MetadataWorkUnit]:
    source = make_source(extract_ownership=extract_ownership, email_domain=email_domain)
    return list(
        source.gen_dataset_workunits(
            table=RedshiftTable(
                name="my_table", columns=[], created=None, comment="", owner=owner
            ),
            database="dev",
            schema="public",
            sub_type="Table",
        )
    )


def test_gen_dataset_workunits_emits_ownership_when_enabled() -> None:
    workunits = _gen_ownership_workunits(owner="etluser", extract_ownership=True)
    ownership_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "ownership"
    ]
    assert len(ownership_mcps) == 1
    ownership = ownership_mcps[0].aspect
    assert isinstance(ownership, OwnershipClass)
    assert len(ownership.owners) == 1
    assert ownership.owners[0].owner == "urn:li:corpuser:etluser"
    assert ownership.owners[0].type == OwnershipTypeClass.TECHNICAL_OWNER


def test_gen_dataset_workunits_ownership_appends_email_domain() -> None:
    workunits = _gen_ownership_workunits(
        owner="etluser", extract_ownership=True, email_domain="company.com"
    )
    ownership_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "ownership"
    ]
    assert len(ownership_mcps) == 1
    ownership = ownership_mcps[0].aspect
    assert isinstance(ownership, OwnershipClass)
    assert ownership.owners[0].owner == "urn:li:corpuser:etluser@company.com"


def test_gen_dataset_workunits_ownership_preserves_existing_email() -> None:
    workunits = _gen_ownership_workunits(
        owner="etluser@other.com", extract_ownership=True, email_domain="company.com"
    )
    ownership_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "ownership"
    ]
    assert len(ownership_mcps) == 1
    ownership = ownership_mcps[0].aspect
    assert isinstance(ownership, OwnershipClass)
    assert ownership.owners[0].owner == "urn:li:corpuser:etluser@other.com"


@pytest.mark.parametrize(
    "owner, extract_ownership",
    [
        ("etluser", False),  # flag disabled
        (None, True),  # no owner in catalog
    ],
)
def test_gen_dataset_workunits_no_ownership_emitted(
    owner: Optional[str], extract_ownership: bool
) -> None:
    workunits = _gen_ownership_workunits(
        owner=owner, extract_ownership=extract_ownership
    )
    ownership_aspects = [
        wu
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "ownership"
    ]
    assert len(ownership_aspects) == 0


def _gen_schema_container_workunits(
    schema_owner: Optional[str],
    extract_ownership: bool,
    email_domain: Optional[str] = None,
) -> list[MetadataWorkUnit]:
    source = make_source(extract_ownership=extract_ownership, email_domain=email_domain)
    schema = RedshiftSchema(
        name="public",
        database="dev",
        type="local",
        owner=schema_owner,
    )
    source.db_tables = {"dev": {}}
    source.db_views = {"dev": {}}
    with patch.object(
        source.data_dictionary,
        "get_columns_for_schema",
        return_value={},
    ):
        return list(
            source.process_schema(
                connection=MagicMock(),
                database="dev",
                schema=schema,
            )
        )


def test_process_schema_emits_owner_urn_when_enabled() -> None:
    workunits = _gen_schema_container_workunits(
        schema_owner="alice", extract_ownership=True
    )
    container_props_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "ownership"
    ]
    assert len(container_props_wus) == 1
    mcp = container_props_wus[0].metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    ownership = mcp.aspect
    assert isinstance(ownership, OwnershipClass)
    assert ownership.owners[0].owner == "urn:li:corpuser:alice"
    assert ownership.owners[0].type == OwnershipTypeClass.TECHNICAL_OWNER


def test_process_schema_ownership_appends_email_domain() -> None:
    workunits = _gen_schema_container_workunits(
        schema_owner="alice", extract_ownership=True, email_domain="acme.com"
    )
    ownership_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "ownership"
    ]
    assert len(ownership_wus) == 1
    mcp = ownership_wus[0].metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    ownership = mcp.aspect
    assert isinstance(ownership, OwnershipClass)
    assert ownership.owners[0].owner == "urn:li:corpuser:alice@acme.com"


@pytest.mark.parametrize(
    "schema_owner, extract_ownership",
    [
        ("alice", False),  # flag disabled
        (None, True),  # no owner in catalog
    ],
)
def test_process_schema_no_owner_urn_emitted(
    schema_owner: Optional[str], extract_ownership: bool
) -> None:
    workunits = _gen_schema_container_workunits(
        schema_owner=schema_owner, extract_ownership=extract_ownership
    )
    ownership_wus = [
        wu
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "ownership"
    ]
    assert len(ownership_wus) == 0


def test_extract_metadata_enters_lineage_block_for_query_usage_only() -> None:
    """The v2 lineage block must run when only query usage stats are requested,
    even with every lineage flag off — otherwise query usage is silently skipped."""
    source = make_source(
        email_domain="example.com",
        include_table_lineage=False,
        include_view_lineage=False,
        include_copy_lineage=False,
        include_unload_lineage=False,
        include_share_lineage=False,
        include_table_rename_lineage=False,
        include_usage_statistics=True,
        include_column_usage_stats=False,
        include_query_usage_statistics=True,
        include_operational_stats=False,
    )
    assert source.config.lineage_enabled is False

    run_extract_metadata(source).extract_lineage.assert_called_once()


def test_extract_metadata_skips_lineage_block_when_all_disabled() -> None:
    """With all lineage flags off and no usage stats, the v2 lineage block must not run."""
    source = make_source(
        include_table_lineage=False,
        include_view_lineage=False,
        include_copy_lineage=False,
        include_unload_lineage=False,
        include_share_lineage=False,
        include_table_rename_lineage=False,
        include_usage_statistics=False,
    )

    run_extract_metadata(source).extract_lineage.assert_not_called()


def _warnings_for(**config_overrides: object) -> list[str]:
    source = make_source(**config_overrides)
    source._warn_deprecated_configs()
    return [
        warning.title for warning in source.report.warnings if warning.title is not None
    ]


def test_warns_when_query_usage_stats_without_usage_statistics() -> None:
    assert "Config option has no effect" in _warnings_for(
        include_usage_statistics=False,
        include_query_usage_statistics=True,
    )


def test_warns_when_query_usage_stats_without_generate_queries() -> None:
    assert "Config option has no effect" in _warnings_for(
        email_domain="example.com",
        include_usage_statistics=True,
        include_query_usage_statistics=True,
        lineage_generate_queries=False,
    )


def test_no_query_usage_warning_when_properly_configured() -> None:
    assert "Config option has no effect" not in _warnings_for(
        email_domain="example.com",
        include_usage_statistics=True,
        include_query_usage_statistics=True,
        lineage_generate_queries=True,
    )


def test_warns_when_column_usage_requested_without_usage_statistics() -> None:
    assert "Config option has no effect" in _warnings_for(
        include_column_usage_stats=True
    )


def test_warns_about_legacy_schema_pattern_matching() -> None:
    """A non-default schema_pattern with match_fully_qualified_names=False still
    matches bare schema names; operators are told to move to the qualified
    form before the legacy mode is removed."""
    assert "Config option deprecation warning" in _warnings_for(
        schema_pattern=AllowDenyPattern(deny=["internal"]),
        match_fully_qualified_names=False,
    )


@pytest.mark.parametrize(
    "patterns, kept",
    [
        ({}, True),
        ({"view_pattern": {"deny": ["dev.public.v1"]}}, False),
        ({"table_pattern": {"deny": ["dev.public.v1"]}}, False),
    ],
    ids=["allowed_by_both", "refused_by_view_pattern", "refused_by_table_pattern"],
)
def test_process_view_applies_view_and_table_pattern(
    patterns: Dict[str, Dict[str, List[str]]], kept: bool
) -> None:
    source = make_source(**patterns)
    view = RedshiftView(
        name="v1",
        comment=None,
        created=None,
        last_altered=None,
        size_in_bytes=None,
        rows_count=None,
    )
    schema = RedshiftSchema(name="public", database="dev", type="local")
    emitted = MagicMock()
    with patch.object(
        source, "gen_view_dataset_workunits", return_value=iter([emitted])
    ):
        workunits = list(source._process_view(view, "dev", schema))

    assert workunits == ([emitted] if kept else [])
    assert ("dev.public.v1" in source.report.filtered) is not kept
