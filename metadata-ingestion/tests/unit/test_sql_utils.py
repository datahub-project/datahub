import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.sql.sql_utils import (
    check_table_with_profile_pattern,
    downgrade_schema_from_v2,
    gen_domain_urn,
    gen_lineage,
    gen_schema_key,
    get_dataplatform_instance_aspect,
    get_domain_wu,
    schema_requires_v2,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DomainsClass,
    StringTypeClass,
)
from datahub.utilities.registries.domain_registry import DomainRegistry


def test_guid_generators():
    expected_guid = "f5e571e4a9acce86333e6b427ba1651f"
    schema_key = gen_schema_key(
        db_name="hive",
        schema="db1",
        platform="hive",
        platform_instance="PROD",
        env="PROD",
    )

    guid = schema_key.guid()
    assert guid == expected_guid


test_profile_pattern_matching_on_table_allow_list_test_data = [
    ("db.table.column", "db.table", True),
    ("db.table.column2", "db.table", True),
    ("db.table..*", "db.table", True),
    ("db.*", "db.table", True),
    ("db.*", "db.table", True),
    ("db.*", "db.schema.table", True),
    ("db.schema.*", "db.schema.table", True),
    ("db\\.schema\\..*", "db.schema.table", True),
    ("db\\.schema\\.table\\.column_prefix.*", "db.schema.table", True),
    ("db\\.schema\\.table\\.column", "db.schema.table", True),
    ("db\\.schema\\.table2\\.column", "db.schema.table", False),
    ("db2\\.schema.*", "db.schema.table", False),
    ("db2\\.schema.*", "db.schema.table", False),
    ("db\\.schema\\.table\\..*", "db.table2", False),
]


@pytest.mark.parametrize(
    "allow_pattern, table_name, result",
    test_profile_pattern_matching_on_table_allow_list_test_data,
)
def test_profile_pattern_matching_on_table_allow_list(
    allow_pattern: str, table_name: str, result: bool
) -> None:
    pattern = AllowDenyPattern(allow=[allow_pattern])
    assert check_table_with_profile_pattern(pattern, table_name) == result


test_profile_pattern_matching_on_table_deny_list_test_data = [
    ("db.table.column", "db.table", True),
    ("db.table.column2", "db.table", True),
    ("db.table..*", "db.table", True),
    ("db.*", "db.table", False),
    ("db.*", "db.table", False),
    ("db.*", "db.schema.table", False),
    ("db.schema.*", "db.schema.table", False),
    ("db\\.schema\\..*", "db.schema.table", False),
    ("db\\.schema\\.table\\.column_prefix.*", "db.schema.table", True),
    ("db\\.schema\\.table\\.column", "db.schema.table", True),
    ("db\\.schema\\.table2\\.column", "db.schema.table", True),
    ("db2\\.schema.*", "db.schema.table", True),
    ("db2\\.schema.*", "db.schema.table", True),
    ("db\\.schema\\.table\\..*", "db.table2", True),
]


@pytest.mark.parametrize(
    "deny_pattern, table_name, result",
    test_profile_pattern_matching_on_table_deny_list_test_data,
)
def test_profile_pattern_matching_on_table_deny_list(
    deny_pattern: str, table_name: str, result: bool
) -> None:
    pattern = AllowDenyPattern(deny=[deny_pattern])
    assert check_table_with_profile_pattern(pattern, table_name) == result


def _registry(*domains: str) -> DomainRegistry:
    return DomainRegistry(cached_domains=list(domains))


def test_the_last_matching_domain_pattern_wins() -> None:
    domain_config = {
        "urn:li:domain:all": AllowDenyPattern(allow=[".*"]),
        "urn:li:domain:finance": AllowDenyPattern(allow=["db\\.finance\\..*"]),
    }
    registry = _registry("urn:li:domain:all", "urn:li:domain:finance")

    assert (
        gen_domain_urn("db.finance.ledger", domain_config, registry)
        == "urn:li:domain:finance"
    )
    assert (
        gen_domain_urn("db.hr.people", domain_config, registry) == "urn:li:domain:all"
    )
    assert (
        gen_domain_urn(
            "other", {"urn:li:domain:x": AllowDenyPattern(allow=["nope"])}, registry
        )
        is None
    )


def test_domain_workunit_is_emitted_only_for_matching_datasets() -> None:
    domain_config = {
        "urn:li:domain:finance": AllowDenyPattern(allow=["db\\.finance\\..*"])
    }
    registry = _registry("urn:li:domain:finance")

    matched = list(
        get_domain_wu(
            "db.finance.ledger", "urn:li:dataset:ledger", domain_config, registry
        )
    )
    assert len(matched) == 1
    aspect = matched[0].metadata
    assert isinstance(aspect, MetadataChangeProposalWrapper)
    assert isinstance(aspect.aspect, DomainsClass)
    assert aspect.aspect.domains == ["urn:li:domain:finance"]

    assert not list(
        get_domain_wu("db.hr.people", "urn:li:dataset:people", domain_config, registry)
    )


def test_dataplatform_instance_aspect_only_for_instance_based_sources() -> None:
    assert get_dataplatform_instance_aspect("urn:li:dataset:x", "mysql", None) is None

    workunit = get_dataplatform_instance_aspect("urn:li:dataset:x", "mysql", "prod")
    assert workunit is not None
    mcp = workunit.metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert isinstance(mcp.aspect, DataPlatformInstanceClass)
    assert mcp.aspect.instance == (
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,prod)"
    )


def test_gen_lineage_emits_nothing_without_lineage() -> None:
    assert not list(gen_lineage("urn:li:dataset:x", None))
    upstream = UpstreamLineage(
        upstreams=[Upstream(dataset="urn:li:dataset:y", type=DatasetLineageType.COPY)]
    )
    workunits = list(gen_lineage("urn:li:dataset:x", upstream))
    assert len(workunits) == 1
    assert workunits[0].get_aspect_of_type(UpstreamLineage) == upstream


def _field(path: str) -> SchemaField:
    return SchemaField(
        fieldPath=path,
        type=SchemaFieldDataType(type=StringTypeClass()),
        nativeDataType="string",
    )


def test_v2_paths_are_downgraded_unless_arrays_or_unions_need_them() -> None:
    simple = [_field("[version=2.0].[type=struct].[type=string].a.b")]
    assert not schema_requires_v2(simple)
    assert [f.fieldPath for f in downgrade_schema_from_v2(simple)] == ["a.b"]

    assert schema_requires_v2([_field("[version=2.0].[type=array].[type=string].tags")])
    assert schema_requires_v2(
        [_field("[version=2.0].[type=union].[type=string].value")]
    )
