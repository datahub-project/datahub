from typing import List

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
from datahub.ingestion.source.snowflake.snowflake_shares import SnowflakeSharesHandler
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal


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

    assert sorted(config.outbounds().keys()) == ["db1", "db2_main"]
    assert sorted(config.inbounds().keys()) == [
        "db1_from_share",
        "db1_other",
        "db2",
        "db2_other",
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
# Phase E.1: auto-discover inbound shares from `origin` field
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

    # 1 table + 1 view × (Siblings + UpstreamLineage) = 4 workunits
    assert len(wus) == 4
    assert report.num_auto_shares_discovered == 1
    assert report.num_auto_shares_skipped_unresolved_producer == 0
    assert report.num_auto_shares_skipped_unknown_share_db == 0

    siblings_count = sum(1 for wu in wus if isinstance(wu.metadata.aspect, Siblings))
    lineage_count = sum(
        1 for wu in wus if isinstance(wu.metadata.aspect, UpstreamLineage)
    )
    assert siblings_count == 2
    assert lineage_count == 2

    for wu in wus:
        if isinstance(wu.metadata.aspect, Siblings):
            assert wu.metadata.aspect.primary is False
            assert len(wu.metadata.aspect.siblings) == 1
            assert "producer_inst.prod_analytics" in wu.metadata.aspect.siblings[0]
        elif isinstance(wu.metadata.aspect, UpstreamLineage):
            assert len(wu.metadata.aspect.upstreams) == 1
            assert (
                "producer_inst.prod_analytics"
                in wu.metadata.aspect.upstreams[0].dataset
            )


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
    assert len(wus) == 4  # producer URN built from account_locator
    assert report.num_auto_shares_discovered == 1
    for wu in wus:
        if isinstance(wu.metadata.aspect, Siblings):
            assert "acct_a.prod_analytics" in wu.metadata.aspect.siblings[0]


def test_auto_share_skips_unknown_share_db() -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="consumer_inst",
        account_mapping={"myorg.acct_a": "producer_inst"},
        # No share_database_mapping
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

    # snowflake_databases fixture has no `origin` set on any DB
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

    # Origin without org prefix: "ACCOUNT.SHARE"
    db = _shared_db("SHARED_DB", origin="XY12345.SHARE_X")
    wus = list(handler.get_auto_share_workunits([db]))
    assert len(wus) == 4
    assert report.num_auto_shares_discovered == 1
