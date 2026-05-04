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
