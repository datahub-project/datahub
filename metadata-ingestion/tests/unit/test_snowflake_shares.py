from typing import List

import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeDatabaseDataHubId,
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
    return [
        SnowflakeDatabase(
            name="db1",
            created=None,
            comment=None,
            last_altered=None,
            schemas=[
                SnowflakeSchema(
                    name="schema11",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["table111", "table112"],
                    views=["view111"],
                ),
                SnowflakeSchema(
                    name="schema12",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["table121", "table122"],
                    views=["view121"],
                ),
            ],
        ),
        SnowflakeDatabase(
            name="db2",
            created=None,
            comment=None,
            last_altered=None,
            schemas=[
                SnowflakeSchema(
                    name="schema21",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["table211", "table212"],
                    views=["view211"],
                ),
                SnowflakeSchema(
                    name="schema22",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["table221", "table222"],
                    views=["view221"],
                ),
            ],
        ),
        SnowflakeDatabase(
            name="db3",
            created=None,
            comment=None,
            last_altered=None,
            schemas=[
                SnowflakeSchema(
                    name="schema31",
                    created=None,
                    comment=None,
                    last_altered=None,
                    tables=["table311", "table312"],
                    views=["view311"],
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
    shares_handler = SnowflakeSharesHandler(
        config, report, lambda x: make_snowflake_urn(x)
    )

    wus = list(shares_handler.get_workunits(snowflake_databases))

    assert len(wus) == 0


def test_same_database_inbound_and_outbound_invalid_config() -> None:
    with pytest.raises(ValueError, match="Same database can not be present in both"):
        SnowflakeV2Config(
            account_id="abc12345",
            platform_instance="instance1",
            inbound_shares_map={
                "db1": SnowflakeDatabaseDataHubId(
                    database_name="original_db1", platform_instance="instance2"
                )
            },
            outbound_shares_map={
                "db1": [
                    SnowflakeDatabaseDataHubId(
                        database_name="db2_from_share", platform_instance="instance2"
                    ),
                    SnowflakeDatabaseDataHubId(
                        database_name="db2", platform_instance="instance3"
                    ),
                ]
            },
        )


def test_current_platform_instance_inbound_and_outbound_invalid() -> None:
    with pytest.raises(
        ValueError, match="Current `platform_instance` can not be present as"
    ):
        SnowflakeV2Config(
            account_id="abc12345",
            platform_instance="instance1",
            inbound_shares_map={
                "db1": SnowflakeDatabaseDataHubId(
                    database_name="db2", platform_instance="instance1"
                )
            },
            outbound_shares_map={
                "db2": [
                    SnowflakeDatabaseDataHubId(
                        database_name="db2_from_share", platform_instance="instance2"
                    ),
                    SnowflakeDatabaseDataHubId(
                        database_name="db2", platform_instance="instance3"
                    ),
                ]
            },
        )

    with pytest.raises(
        ValueError, match="Current `platform_instance` can not be present as"
    ):
        SnowflakeV2Config(
            account_id="abc12345",
            platform_instance="instance1",
            inbound_shares_map={
                "db1": SnowflakeDatabaseDataHubId(
                    database_name="db2", platform_instance="instance2"
                )
            },
            outbound_shares_map={
                "db2": [
                    SnowflakeDatabaseDataHubId(
                        database_name="db2_from_share", platform_instance="instance2"
                    ),
                    SnowflakeDatabaseDataHubId(
                        database_name="db2", platform_instance="instance1"
                    ),
                ]
            },
        )


def test_another_instance_database_inbound_and_outbound_invalid() -> None:
    with pytest.raises(ValueError, match="A database can exist only once either in"):
        SnowflakeV2Config(
            account_id="abc12345",
            platform_instance="instance1",
            inbound_shares_map={
                "db1": SnowflakeDatabaseDataHubId(
                    database_name="db2", platform_instance="instance3"
                )
            },
            outbound_shares_map={
                "db2": [
                    SnowflakeDatabaseDataHubId(
                        database_name="db2_from_share", platform_instance="instance2"
                    ),
                    SnowflakeDatabaseDataHubId(
                        database_name="db2", platform_instance="instance3"
                    ),
                ]
            },
        )


def test_snowflake_shares_workunit_inbound_share(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="instance1",
        inbound_shares_map={
            "db1": SnowflakeDatabaseDataHubId(
                database_name="original_db1", platform_instance="instance2"
            )
        },
    )

    report = SnowflakeV2Report()
    shares_handler = SnowflakeSharesHandler(
        config, report, lambda x: make_snowflake_urn(x, "instance1")
    )

    wus = list(shares_handler.get_workunits(snowflake_databases))

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
                "instance1.db1", "instance2.original_db1"
            )
            upstream_lineage_aspect_entity_urns.add(wu.get_urn())
        else:
            siblings_aspect = wu.get_aspect_of_type(Siblings)
            assert siblings_aspect is not None
            assert len(siblings_aspect.siblings) == 1
            assert siblings_aspect.siblings == [
                wu.get_urn().replace("instance1.db1", "instance2.original_db1")
            ]
            sibling_aspect_entity_urns.add(wu.get_urn())

    assert upstream_lineage_aspect_entity_urns == sibling_aspect_entity_urns


def test_snowflake_shares_workunit_outbound_share(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="instance1",
        outbound_shares_map={
            "db2": [
                SnowflakeDatabaseDataHubId(
                    database_name="db2_from_share", platform_instance="instance2"
                ),
                SnowflakeDatabaseDataHubId(
                    database_name="db2", platform_instance="instance3"
                ),
            ]
        },
    )

    report = SnowflakeV2Report()
    shares_handler = SnowflakeSharesHandler(
        config, report, lambda x: make_snowflake_urn(x, "instance1")
    )

    wus = list(shares_handler.get_workunits(snowflake_databases))

    # 2 schemas - 2 tables and 1 view in each schema making total 6 datasets
    # Hence 6 Sibling aspects
    assert len(wus) == 6
    entity_urns = set()

    for wu in wus:
        siblings_aspect = wu.get_aspect_of_type(Siblings)
        assert siblings_aspect is not None
        assert len(siblings_aspect.siblings) == 2
        assert siblings_aspect.siblings == [
            wu.get_urn().replace("instance1.db2", "instance2.db2_from_share"),
            wu.get_urn().replace("instance1.db2", "instance3.db2"),
        ]
        entity_urns.add(wu.get_urn())

    assert len((entity_urns)) == 6


def test_snowflake_shares_workunit_inbound_and_outbound_share(
    snowflake_databases: List[SnowflakeDatabase],
) -> None:
    config = SnowflakeV2Config(
        account_id="abc12345",
        platform_instance="instance1",
        inbound_shares_map={
            "db1": SnowflakeDatabaseDataHubId(
                database_name="original_db1", platform_instance="instance2"
            )
        },
        outbound_shares_map={
            "db2": [
                SnowflakeDatabaseDataHubId(
                    database_name="db2_from_share", platform_instance="instance2"
                ),
                SnowflakeDatabaseDataHubId(
                    database_name="db2", platform_instance="instance3"
                ),
            ]
        },
    )

    report = SnowflakeV2Report()
    shares_handler = SnowflakeSharesHandler(
        config, report, lambda x: make_snowflake_urn(x, "instance1")
    )

    wus = list(shares_handler.get_workunits(snowflake_databases))

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
                "instance1.db1", "instance2.original_db1"
            )
        else:
            siblings_aspect = wu.get_aspect_of_type(Siblings)
            assert siblings_aspect is not None
            if "db1" in wu.get_urn():
                assert len(siblings_aspect.siblings) == 1
                assert siblings_aspect.siblings == [
                    wu.get_urn().replace("instance1.db1", "instance2.original_db1")
                ]
            else:
                assert len(siblings_aspect.siblings) == 2
                assert siblings_aspect.siblings == [
                    wu.get_urn().replace("instance1.db2", "instance2.db2_from_share"),
                    wu.get_urn().replace("instance1.db2", "instance3.db2"),
                ]
