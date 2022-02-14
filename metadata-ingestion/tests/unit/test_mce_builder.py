from typing import Dict, Optional, Tuple

import pytest

import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnershipClass,
)


def test_can_add_aspect():
    dataset_mce: MetadataChangeEventClass = builder.make_lineage_mce(
        [
            builder.make_dataset_urn("bigquery", "upstream1"),
            builder.make_dataset_urn("bigquery", "upstream2"),
        ],
        builder.make_dataset_urn("bigquery", "downstream"),
    )
    assert isinstance(dataset_mce.proposedSnapshot, DatasetSnapshotClass)

    assert builder.can_add_aspect(dataset_mce, DatasetPropertiesClass)
    assert builder.can_add_aspect(dataset_mce, OwnershipClass)
    assert not builder.can_add_aspect(dataset_mce, DataFlowInfoClass)


test_make_dataset_urns_params: Dict[
    str, Tuple[Tuple[str, str, Optional[str], str], str]
] = {
    "athena": (
        ("athena", "ATABLE", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:athena,my_instance.atable,PROD)",
    ),
    "bigquery": (
        ("bigquery", "ATable", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_instance.atable,PROD)",
    ),
    "bigquery_no_instance": (
        ("bigquery", "ATable", None, "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,atable,PROD)",
    ),
    "druid": (
        ("druid", "AtaBLE", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:druid,my_instance.atable,PROD)",
    ),
    "hive": (
        ("hive", "ataBLE", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_instance.atable,PROD)",
    ),
    "mariadb": (
        ("mariadb", "aTAble", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:mariadb,my_instance.atable,PROD)",
    ),
    "mssql": (
        ("mssql", "aTAblE", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:mssql,my_instance.atable,PROD)",
    ),
    "mysql": (
        ("mysql", "aTABlE", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:mysql,my_instance.atable,PROD)",
    ),
    "oracle": (
        ("oracle", "AtAbLe", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:oracle,my_instance.atable,PROD)",
    ),
    "postgres": (
        ("postgres", "AtAbLE", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:postgres,my_instance.atable,PROD)",
    ),
    "redshift": (
        ("redshift", "atAbLE", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:redshift,my_instance.atable,PROD)",
    ),
    "snowflake": (
        ("snowflake", "atABle", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_instance.atable,PROD)",
    ),
    "trino": (
        ("trino", "AtaBle", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:trino,my_instance.atable,PROD)",
    ),
    "kafka_no_lower_casing": (
        ("kafka", "MyKafkaTopic", "MY_INSTANCE", "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:kafka,MY_INSTANCE.MyKafkaTopic,PROD)",
    ),
    "kafka_no_instance_no_lower_casing": (
        ("kafka", "MyKafkaTopic", None, "PROD"),
        "urn:li:dataset:(urn:li:dataPlatform:kafka,MyKafkaTopic,PROD)",
    ),
}


@pytest.mark.parametrize(
    "urnParts, expected",
    test_make_dataset_urns_params.values(),
    ids=test_make_dataset_urns_params.keys(),
)
def test_make_dataset_urns(
    urnParts: Tuple[str, str, Optional[str], str], expected: str
) -> None:
    dataset_urn = builder.make_dataset_urn_with_platform_instance(
        urnParts[0], urnParts[1], urnParts[2], urnParts[3]
    )
    assert dataset_urn == expected
