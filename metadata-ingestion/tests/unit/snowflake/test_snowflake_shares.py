import json
from typing import List
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowflake.snowflake_config import (
    DatabaseId,
    SnowflakeShareConfig,
    SnowflakeV2Config,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDatabase,
    SnowflakeSchema,
)
from datahub.ingestion.source.snowflake.snowflake_shares import (
    SHARE_GRANT_HISTORY_QUERY_LIMIT,
    SnowflakeSharesHandler,
    parse_share_grants,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal
from datahub.metadata.schema_classes import DataPlatformInstancePropertiesClass


@pytest.fixture(scope="module")
def snowflake_databases() -> List[SnowflakeDatabase]:
    # Snowflake returns database/schema/table names in UPPERCASE.
    # Tests deliberately use lowercase config values (e.g. database="db1") to
    # exercise case-insensitive matching and verify that URNs are lowercased.
    return [
        SnowflakeDatabase(
            name="DB1",
            created=None,
            comment=None,
            last_altered=None,
            schemas=[
                SnowflakeSchema(
                    name="SCHEMA11",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["TABLE111", "TABLE112"],
                    views=["VIEW111"],
                ),
                SnowflakeSchema(
                    name="SCHEMA12",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["TABLE121", "TABLE122"],
                    views=["VIEW121"],
                ),
            ],
        ),
        SnowflakeDatabase(
            name="DB2",
            created=None,
            comment=None,
            last_altered=None,
            schemas=[
                SnowflakeSchema(
                    name="SCHEMA21",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["TABLE211", "TABLE212"],
                    views=["VIEW211"],
                ),
                SnowflakeSchema(
                    name="SCHEMA22",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["TABLE221", "TABLE222"],
                    views=["VIEW221"],
                ),
            ],
        ),
        SnowflakeDatabase(
            name="DB3",
            created=None,
            comment=None,
            last_altered=None,
            schemas=[
                SnowflakeSchema(
                    name="SCHEMA31",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["TABLE311", "TABLE312"],
                    views=["VIEW311"],
                )
            ],
        ),
    ]


def make_snowflake_urn(table_name, instance_name=None):
    return make_dataset_urn_with_platform_instance(
        "snowflake", table_name, instance_name
    )


def test_snowflake_shares_workunit_no_shares(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(account_id="abc12345", platform_instance="instance1")
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    wus = list(handler.get_shares_workunits(snowflake_databases))

    assert len(wus) == 0


def test_same_database_inbound_and_outbound_invalid_config() -> None:
    with pytest.raises(
        ValueError,
        match="Same database can not be present as consumer in more than one share",
    ):
        SnowflakeV2Config(
            account_id="abc12345",
            platform_instance="instance1",
            shares={
                "share1": SnowflakeShareConfig(
                    database="db1",
                    platform_instance="instance2",
                    consumers=[
                        DatabaseId(database="db1", platform_instance="instance1")
                    ],
                ),
                "share2": SnowflakeShareConfig(
                    database="db1",
                    platform_instance="instance3",
                    consumers=[
                        DatabaseId(database="db1", platform_instance="instance1")
                    ],
                ),
            },
        )

    with pytest.raises(
        ValueError,
        match="Database included in a share can not be present as consumer in any share",
    ):
        SnowflakeV2Config(
            account_id="abc12345",
            platform_instance="instance1",
            shares={
                "share1": SnowflakeShareConfig(
                    database="db1",
                    platform_instance="instance2",
                    consumers=[
                        DatabaseId(database="db1", platform_instance="instance1")
                    ],
                ),
                "share2": SnowflakeShareConfig(
                    database="db1",
                    platform_instance="instance1",
                    consumers=[
                        DatabaseId(database="db1", platform_instance="instance3")
                    ],
                ),
            },
        )

    with pytest.raises(
        ValueError,
        match="Database included in a share can not be present as consumer in any share",
    ):
        SnowflakeV2Config(
            account_id="abc12345",
            platform_instance="instance1",
            shares={
                "share2": SnowflakeShareConfig(
                    database="db1",
                    platform_instance="instance1",
                    consumers=[
                        DatabaseId(database="db1", platform_instance="instance3")
                    ],
                ),
                "share1": SnowflakeShareConfig(
                    database="db1",
                    platform_instance="instance2",
                    consumers=[
                        DatabaseId(database="db1", platform_instance="instance1")
                    ],
                ),
            },
        )


def test_snowflake_shares_workunit_inbound_share(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="instance1",
        shares={
            "share1": SnowflakeShareConfig(
                database="db1",
                platform_instance="instance2",
                consumers=[DatabaseId(database="db1", platform_instance="instance1")],
            )
        },
    )

    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    wus = list(handler.get_shares_workunits(snowflake_databases))

    # 2 schemas - 2 tables and 1 view in each schema making total 6 datasets
    # Hence 6 Sibling and 6 upstreamLineage aspects
    assert len(wus) == 12
    upstream_lineage_aspect_entity_urns = set()
    sibling_aspect_entity_urns = set()

    for wu in wus:
        assert isinstance(
            wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
        )
        if wu.metadata.aspectName == "upstreamLineage":
            upstream_aspect = wu.get_aspect_of_type(UpstreamLineage)
            assert upstream_aspect is not None
            assert len(upstream_aspect.upstreams) == 1
            assert upstream_aspect.upstreams[0].dataset == wu.get_urn().replace(
                "instance1.db1", "instance2.db1"
            )
            upstream_lineage_aspect_entity_urns.add(wu.get_urn())
        else:
            siblings_aspect = wu.get_aspect_of_type(Siblings)
            assert siblings_aspect is not None
            assert not siblings_aspect.primary
            assert len(siblings_aspect.siblings) == 1
            assert siblings_aspect.siblings == [
                wu.get_urn().replace("instance1.db1", "instance2.db1")
            ]
            sibling_aspect_entity_urns.add(wu.get_urn())

    assert upstream_lineage_aspect_entity_urns == sibling_aspect_entity_urns


def test_snowflake_shares_workunit_outbound_share(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="instance1",
        shares={
            "share2": SnowflakeShareConfig(
                database="db2",
                platform_instance="instance1",
                consumers=[
                    DatabaseId(
                        database="db2_from_share", platform_instance="instance2"
                    ),
                    DatabaseId(database="db2", platform_instance="instance3"),
                ],
            )
        },
    )

    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    wus = list(handler.get_shares_workunits(snowflake_databases))

    # 2 schemas - 2 tables and 1 view in each schema making total 6 datasets
    # Hence 6 Sibling aspects
    assert len(wus) == 6
    entity_urns = set()

    for wu in wus:
        siblings_aspect = wu.get_aspect_of_type(Siblings)
        assert siblings_aspect is not None
        assert siblings_aspect.primary
        assert len(siblings_aspect.siblings) == 2
        assert siblings_aspect.siblings == [
            wu.get_urn().replace("instance1.db2", "instance2.db2_from_share"),
            wu.get_urn().replace("instance1.db2", "instance3.db2"),
        ]
        entity_urns.add(wu.get_urn())

    assert len(entity_urns) == 6


def test_snowflake_shares_workunit_inbound_and_outbound_share(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="instance1",
        shares={
            "share1": SnowflakeShareConfig(
                database="db1",
                platform_instance="instance2",
                consumers=[DatabaseId(database="db1", platform_instance="instance1")],
            ),
            "share2": SnowflakeShareConfig(
                database="db2",
                platform_instance="instance1",
                consumers=[
                    DatabaseId(
                        database="db2_from_share", platform_instance="instance2"
                    ),
                    DatabaseId(database="db2", platform_instance="instance3"),
                ],
            ),
        },
    )

    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    wus = list(handler.get_shares_workunits(snowflake_databases))

    # 6 Sibling and 6 upstreamLineage aspects for db1 tables
    # 6 Sibling aspects for db2 tables
    assert len(wus) == 18

    for wu in wus:
        assert isinstance(
            wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
        )
        if wu.metadata.aspectName == "upstreamLineage":
            upstream_aspect = wu.get_aspect_of_type(UpstreamLineage)
            assert upstream_aspect is not None
            assert len(upstream_aspect.upstreams) == 1
            assert upstream_aspect.upstreams[0].dataset == wu.get_urn().replace(
                "instance1.db1", "instance2.db1"
            )
        else:
            siblings_aspect = wu.get_aspect_of_type(Siblings)
            assert siblings_aspect is not None
            if "db1" in wu.get_urn():
                assert not siblings_aspect.primary
                assert len(siblings_aspect.siblings) == 1
                assert siblings_aspect.siblings == [
                    wu.get_urn().replace("instance1.db1", "instance2.db1")
                ]
            else:
                assert siblings_aspect.primary
                assert len(siblings_aspect.siblings) == 2
                assert siblings_aspect.siblings == [
                    wu.get_urn().replace("instance1.db2", "instance2.db2_from_share"),
                    wu.get_urn().replace("instance1.db2", "instance3.db2"),
                ]


def test_snowflake_shares_workunit_inbound_and_outbound_share_no_platform_instance(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        shares={
            "share1": SnowflakeShareConfig(
                database="db1",
                consumers=[
                    DatabaseId(database="db1_from_share"),
                    DatabaseId(database="db1_other"),
                ],
            ),
            "share2": SnowflakeShareConfig(
                database="db2_main",
                consumers=[
                    DatabaseId(database="db2"),
                    DatabaseId(database="db2_other"),
                ],
            ),
        },
    )

    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    # outbounds()/inbounds() upper-case keys so callers can match Snowflake's
    # uppercase identifiers without per-call `.upper()` plumbing.
    assert sorted(config.outbounds().keys()) == ["DB1", "DB2_MAIN"]
    assert sorted(config.inbounds().keys()) == [
        "DB1_FROM_SHARE",
        "DB1_OTHER",
        "DB2",
        "DB2_OTHER",
    ]
    wus = list(handler.get_shares_workunits(snowflake_databases))

    # 6 Sibling aspects for db1 tables
    # 6 Sibling aspects and and 6 upstreamLineage for db2 tables
    assert len(wus) == 18

    for wu in wus:
        assert isinstance(
            wu.metadata, (MetadataChangeProposal, MetadataChangeProposalWrapper)
        )
        if wu.metadata.aspectName == "upstreamLineage":
            upstream_aspect = wu.get_aspect_of_type(UpstreamLineage)
            assert upstream_aspect is not None
            assert len(upstream_aspect.upstreams) == 1
            assert upstream_aspect.upstreams[0].dataset == wu.get_urn().replace(
                "db2.", "db2_main."
            )
        else:
            siblings_aspect = wu.get_aspect_of_type(Siblings)
            assert siblings_aspect is not None
            if "db1" in wu.get_urn():
                assert siblings_aspect.primary
                assert len(siblings_aspect.siblings) == 2
                assert siblings_aspect.siblings == [
                    wu.get_urn().replace("db1.", "db1_from_share."),
                    wu.get_urn().replace("db1.", "db1_other."),
                ]
            else:
                assert not siblings_aspect.primary
                assert len(siblings_aspect.siblings) == 1
                assert siblings_aspect.siblings == [
                    wu.get_urn().replace("db2.", "db2_main.")
                ]


# ---------------------------------------------------------------------------
# Auto-discover inbound shares from `origin` field
# ---------------------------------------------------------------------------


def _shared_db(name: str, origin: str) -> SnowflakeDatabase:
    return SnowflakeDatabase(
        name=name,
        created=None,
        comment=None,
        last_altered=None,
        origin=origin,
        kind="IMPORTED DATABASE",
        schemas=[
            SnowflakeSchema(
                name="SCHEMA1",
                created=None,
                comment=None,
                last_altered=None,
                tables=["TABLE1"],
                views=["VIEW1"],
            ),
        ],
    )


def test_auto_share_emits_siblings_and_lineage_when_resolved() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))

    assert len(wus) == 4
    assert report.num_auto_shares_discovered == 1
    assert report.num_auto_shares_skipped_unresolved_producer == 0
    assert report.num_auto_shares_skipped_unknown_share_db == 0

    siblings_count = sum(1 for wu in wus if wu.get_aspect_of_type(Siblings) is not None)
    lineage_count = sum(
        1 for wu in wus if wu.get_aspect_of_type(UpstreamLineage) is not None
    )
    assert siblings_count == 2
    assert lineage_count == 2

    for wu in wus:
        siblings = wu.get_aspect_of_type(Siblings)
        lineage = wu.get_aspect_of_type(UpstreamLineage)
        if siblings is not None:
            assert siblings.primary is False
            assert len(siblings.siblings) == 1
            assert "producer_inst.prod_analytics" in siblings.siblings[0]
        elif lineage is not None:
            assert len(lineage.upstreams) == 1
            assert "producer_inst.prod_analytics" in lineage.upstreams[0].dataset


def test_auto_share_skips_when_disabled() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        auto_discover_inbound_shares=False,
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    assert list(handler.get_auto_share_workunits([db])) == []
    assert report.num_auto_shares_discovered == 0


def test_auto_share_skips_unresolved_producer() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    assert wus == []
    assert report.num_auto_shares_skipped_unresolved_producer == 1


def test_auto_share_uses_account_locator_fallback_when_enabled() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_locator_fallback=True,
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    assert len(wus) == 4
    assert report.num_auto_shares_discovered == 1
    for wu in wus:
        siblings = wu.get_aspect_of_type(Siblings)
        if siblings is not None:
            assert "acct_a.prod_analytics" in siblings.siblings[0]


def test_auto_share_skips_unknown_share_db() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
    )
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    assert wus == []
    assert report.num_auto_shares_skipped_unknown_share_db == 1


def test_auto_share_skips_when_manual_config_covers_db() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
        shares={
            "manual_share": SnowflakeShareConfig(
                database="shared_db",
                platform_instance="manual_producer",
                consumers=[
                    DatabaseId(database="shared_db", platform_instance="consumer_inst")
                ],
            )
        },
    )
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    assert list(handler.get_auto_share_workunits([db])) == []
    assert report.num_auto_shares_discovered == 0


def test_auto_share_ignores_non_shared_databases(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    assert list(handler.get_auto_share_workunits(snowflake_databases)) == []
    assert report.num_auto_shares_discovered == 0


def test_auto_share_locator_only_origin_resolves_via_locator() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"xy12345": "producer_inst"},
        share_database_mapping={"SHARE_X": "PROD_DB"},
    )
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    db = _shared_db("SHARED_DB", origin="XY12345.SHARE_X")
    wus = list(handler.get_auto_share_workunits([db]))
    assert len(wus) == 4
    assert report.num_auto_shares_discovered == 1


# ---------------------------------------------------------------------------
# Producer-side QUERY_HISTORY parser
# ---------------------------------------------------------------------------


def test_parse_share_grants_basic() -> None:
    queries = ["GRANT USAGE ON DATABASE prod_analytics TO SHARE analytics_share"]
    result = parse_share_grants(queries)
    assert result.mapping == {"ANALYTICS_SHARE": "PROD_ANALYTICS"}
    assert result.skipped_oversized_queries == 0


def test_parse_share_grants_quoted_identifiers() -> None:
    queries = [
        'GRANT USAGE ON DATABASE "My Prod DB" TO SHARE "My Share"',
    ]
    assert parse_share_grants(queries).mapping == {"My Share": "My Prod DB"}


def test_parse_share_grants_extra_whitespace_and_newlines() -> None:
    queries = [
        "GRANT  USAGE\n  ON\tDATABASE   prod_db\n  TO   SHARE   my_share",
    ]
    assert parse_share_grants(queries).mapping == {"MY_SHARE": "PROD_DB"}


def test_parse_share_grants_first_seen_wins() -> None:
    # Reverse-chronological input means most-recent grant wins per share.
    queries = [
        "GRANT USAGE ON DATABASE new_db TO SHARE share_x",
        "GRANT USAGE ON DATABASE old_db TO SHARE share_x",
    ]
    assert parse_share_grants(queries).mapping == {"SHARE_X": "NEW_DB"}


def test_parse_share_grants_multiple_shares() -> None:
    queries = [
        "GRANT USAGE ON DATABASE db1 TO SHARE share1",
        "GRANT USAGE ON DATABASE db2 TO SHARE share2",
    ]
    assert parse_share_grants(queries).mapping == {"SHARE1": "DB1", "SHARE2": "DB2"}


def test_parse_share_grants_ignores_non_matching_queries() -> None:
    queries = [
        "SELECT 1",
        "GRANT SELECT ON TABLE foo TO ROLE bar",
        "GRANT USAGE ON DATABASE prod_db TO SHARE my_share",
    ]
    assert parse_share_grants(queries).mapping == {"MY_SHARE": "PROD_DB"}


def test_parse_share_grants_empty_input() -> None:
    # Empty input is "nothing to scan", not "we skipped oversized things".
    assert parse_share_grants([]).mapping == {}
    assert parse_share_grants([]).skipped_oversized_queries == 0
    assert parse_share_grants([""]).mapping == {}
    assert parse_share_grants([""]).skipped_oversized_queries == 0
    assert parse_share_grants([None]).mapping == {}  # type: ignore[list-item]


# ---------------------------------------------------------------------------
# Graph-backed share->DB resolution and producer URN validation
# ---------------------------------------------------------------------------


def _graph_with_published_mapping(mapping: dict) -> MagicMock:
    graph = MagicMock()
    props = DataPlatformInstancePropertiesClass(
        customProperties={"share_database_mapping": json.dumps(mapping)},
    )
    graph.get_aspect.return_value = props
    graph.exists.return_value = True
    return graph


def test_auto_share_reads_mapping_from_graph_when_available() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        # Note: no local share_database_mapping — must come from graph
    )
    report = SnowflakeV2Report()
    graph = _graph_with_published_mapping({"ANALYTICS_SHARE": "PROD_ANALYTICS"})
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))

    assert len(wus) == 4
    assert report.num_auto_shares_discovered == 1
    for wu in wus:
        siblings = wu.get_aspect_of_type(Siblings)
        if siblings is not None:
            assert "producer_inst.prod_analytics" in siblings.siblings[0]


def test_auto_share_graph_mapping_takes_precedence_over_local_config() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "STALE_DB"},
    )
    report = SnowflakeV2Report()
    graph = _graph_with_published_mapping({"ANALYTICS_SHARE": "PROD_ANALYTICS"})
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))

    for wu in wus:
        siblings = wu.get_aspect_of_type(Siblings)
        if siblings is not None:
            assert "producer_inst.prod_analytics" in siblings.siblings[0]
            assert "stale_db" not in siblings.siblings[0]


def test_auto_share_falls_back_to_local_config_when_graph_has_no_mapping() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    graph = MagicMock()
    graph.get_aspect.return_value = None  # producer not yet ingested
    graph.exists.return_value = True
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    assert len(wus) == 4
    for wu in wus:
        siblings = wu.get_aspect_of_type(Siblings)
        if siblings is not None:
            assert "producer_inst.prod_analytics" in siblings.siblings[0]


def test_auto_share_skipped_when_validate_urns_and_producer_missing() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
        validate_producer_urns_in_graph=True,
    )
    report = SnowflakeV2Report()
    graph = MagicMock()
    graph.get_aspect.return_value = None
    graph.exists.return_value = False
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    assert wus == []
    assert report.num_auto_shares_skipped_producer_urn_missing == 2  # table + view


def test_auto_share_fails_open_when_graph_exists_raises() -> None:
    # A transient graph error during validation must not crash the ingestion
    # nor strip lineage for the rest of the run. Emit a warning, fail open.
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
        validate_producer_urns_in_graph=True,
    )
    report = SnowflakeV2Report()
    graph = MagicMock()
    graph.get_aspect.return_value = None
    graph.exists.side_effect = Exception("graph unreachable")
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))

    assert len(wus) == 4
    assert report.num_auto_shares_skipped_producer_urn_missing == 0
    assert any(
        "Producer URN existence check failed" in (w.title or "")
        for w in report.warnings
    )


def test_auto_share_emits_when_validate_urns_off_and_producer_missing() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
        # validate_producer_urns_in_graph defaults to False
    )
    report = SnowflakeV2Report()
    graph = MagicMock()
    graph.get_aspect.return_value = None
    graph.exists.return_value = False  # would skip if validation were on
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    assert len(wus) == 4
    assert report.num_auto_shares_skipped_producer_urn_missing == 0


def test_auto_share_handles_corrupt_published_mapping_gracefully() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    graph = MagicMock()
    graph.get_aspect.return_value = DataPlatformInstancePropertiesClass(
        customProperties={"share_database_mapping": "{not valid json"},
    )
    graph.exists.return_value = True
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    # Falls back to local config, which has the mapping
    assert len(wus) == 4


# ---------------------------------------------------------------------------
# discover_share_database_mapping (producer-side wrapper)
# ---------------------------------------------------------------------------


def test_discover_share_database_mapping_parses_query_history() -> None:
    config = SnowflakeV2Config(account_id="abc12345")
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    conn = MagicMock()
    conn.query.return_value = [
        {"QUERY_TEXT": "GRANT USAGE ON DATABASE prod_db TO SHARE my_share"},
        {"QUERY_TEXT": "SELECT 1"},
        {"QUERY_TEXT": None},
        {"QUERY_TEXT": "GRANT USAGE ON DATABASE other_db TO SHARE other_share"},
    ]
    mapping = handler.discover_share_database_mapping(conn)
    assert mapping == {"MY_SHARE": "PROD_DB", "OTHER_SHARE": "OTHER_DB"}


def test_discover_share_database_mapping_query_failure_warns() -> None:
    config = SnowflakeV2Config(account_id="abc12345")
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    conn = MagicMock()
    conn.query.side_effect = Exception("permission denied")
    mapping = handler.discover_share_database_mapping(conn)
    assert mapping == {}
    assert any(
        "Share-to-database mining skipped" in (w.title or "") for w in report.warnings
    )


def test_discover_share_database_mapping_truncation_warns() -> None:
    # Receiving more than the cap means QUERY_HISTORY had additional grants we
    # couldn't see — the SQL over-fetches by one, so cap+1 rows is the trigger.
    config = SnowflakeV2Config(account_id="abc12345")
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    conn = MagicMock()
    conn.query.return_value = [
        {"QUERY_TEXT": f"GRANT USAGE ON DATABASE db_{i} TO SHARE share_{i}"}
        for i in range(SHARE_GRANT_HISTORY_QUERY_LIMIT + 1)
    ]
    handler.discover_share_database_mapping(conn)
    assert any(
        "Share grant history may be incomplete" in (w.title or "")
        for w in report.warnings
    )


def test_discover_share_database_mapping_at_cap_does_not_warn() -> None:
    # Receiving exactly the cap means we got every grant — no truncation.
    config = SnowflakeV2Config(account_id="abc12345")
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    conn = MagicMock()
    conn.query.return_value = [
        {"QUERY_TEXT": f"GRANT USAGE ON DATABASE db_{i} TO SHARE share_{i}"}
        for i in range(SHARE_GRANT_HISTORY_QUERY_LIMIT)
    ]
    handler.discover_share_database_mapping(conn)
    assert not any(
        "Share grant history may be incomplete" in (w.title or "")
        for w in report.warnings
    )


def test_parse_share_grants_with_sql_comments() -> None:
    queries = [
        "-- this is a comment\nGRANT USAGE ON DATABASE prod_db TO SHARE my_share",
        "/* block comment */ GRANT USAGE ON DATABASE other_db TO SHARE other_share",
    ]
    assert parse_share_grants(queries).mapping == {
        "MY_SHARE": "PROD_DB",
        "OTHER_SHARE": "OTHER_DB",
    }


def test_parse_share_grants_skips_huge_query_text() -> None:
    huge_text = "x" * 2_000_000 + " GRANT USAGE ON DATABASE big TO SHARE big_share"
    queries = [
        huge_text,
        "GRANT USAGE ON DATABASE small TO SHARE small_share",
    ]
    # Huge text is skipped without scanning; only the small one is parsed.
    # The skip is counted so callers can surface it as an observability signal.
    result = parse_share_grants(queries)
    assert result.mapping == {"SMALL_SHARE": "SMALL"}
    assert result.skipped_oversized_queries == 1


def test_discover_share_database_mapping_counts_oversized_skips() -> None:
    config = SnowflakeV2Config(account_id="abc12345")
    report = SnowflakeV2Report()
    handler = SnowflakeSharesHandler(config, report)

    huge = "x" * 2_000_000 + " GRANT USAGE ON DATABASE big TO SHARE big_share"
    conn = MagicMock()
    conn.query.return_value = [
        {"QUERY_TEXT": huge},
        {"QUERY_TEXT": "GRANT USAGE ON DATABASE small TO SHARE small_share"},
    ]
    mapping = handler.discover_share_database_mapping(conn)

    assert mapping == {"SMALL_SHARE": "SMALL"}
    assert report.num_share_grant_queries_skipped_oversized == 1


# ---------------------------------------------------------------------------
# customProperties=None edge cases for _fetch_published_share_mapping
# ---------------------------------------------------------------------------


def test_fetch_published_share_mapping_handles_null_custom_properties() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    graph = MagicMock()
    graph.get_aspect.return_value = DataPlatformInstancePropertiesClass(
        customProperties=None
    )
    graph.exists.return_value = True
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    # Falls back to local config; emission still works
    assert len(wus) == 4


def test_fetch_published_share_mapping_handles_non_dict_json() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        share_database_mapping={"ANALYTICS_SHARE": "PROD_ANALYTICS"},
    )
    report = SnowflakeV2Report()
    graph = MagicMock()
    graph.get_aspect.return_value = DataPlatformInstancePropertiesClass(
        customProperties={"share_database_mapping": "[1, 2, 3]"},
    )
    graph.exists.return_value = True
    handler = SnowflakeSharesHandler(config, report, graph=graph)

    db = _shared_db("SHARED_DB", origin="MYORG.ACCT_A.ANALYTICS_SHARE")
    wus = list(handler.get_auto_share_workunits([db]))
    assert len(wus) == 4
    assert any(
        "Producer published invalid share_database_mapping" in (w.title or "")
        for w in report.warnings
    )
