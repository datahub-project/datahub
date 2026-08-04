import pytest

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.utilities.urns.urn_iter import (
    list_urns_with_path,
    lowercase_dataset_urn,
    lowercase_dataset_urns,
)


def _datasetUrn(tbl: str) -> str:
    return builder.make_dataset_urn("bigquery", tbl, "PROD")


def _fldUrn(tbl: str, fld: str) -> str:
    return builder.make_schema_field_urn(_datasetUrn(tbl), fld)


def test_list_urns_upstream():
    upstream_table = Upstream(
        dataset=_datasetUrn("upstream_table_1"),
        type=DatasetLineageTypeClass.TRANSFORMED,
    )

    urns = list_urns_with_path(upstream_table)
    assert urns == [
        (
            "urn:li:corpuser:unknown",
            ["auditStamp", "actor"],
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)",
            ["dataset"],
        ),
    ]


def test_upstream_lineage_urn_iterator():
    upstream_table_1 = Upstream(
        dataset=_datasetUrn("upstream_table_1"),
        type=DatasetLineageTypeClass.TRANSFORMED,
    )
    upstream_table_2 = Upstream(
        dataset=_datasetUrn("upstream_table_2"),
        type=DatasetLineageTypeClass.TRANSFORMED,
    )

    # Construct a lineage aspect.
    upstream_lineage = UpstreamLineage(
        upstreams=[upstream_table_1, upstream_table_2],
        fineGrainedLineages=[
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[
                    _fldUrn("upstream_table_1", "c1"),
                    _fldUrn("upstream_table_2", "c2"),
                ],
                downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
                downstreams=[
                    _fldUrn("downstream_table", "c3"),
                    _fldUrn("downstream_table", "c4"),
                ],
            ),
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.DATASET,
                upstreams=[_datasetUrn("upstream_table_1")],
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[_fldUrn("downstream_table", "c5")],
            ),
        ],
    )

    urns = list_urns_with_path(upstream_lineage)
    assert urns == [
        (
            "urn:li:corpuser:unknown",
            ["upstreams", 0, "auditStamp", "actor"],
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)",
            ["upstreams", 0, "dataset"],
        ),
        ("urn:li:corpuser:unknown", ["upstreams", 1, "auditStamp", "actor"]),
        (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD)",
            ["upstreams", 1, "dataset"],
        ),
        (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)",
            ["fineGrainedLineages", 0, "upstreams", 0],
        ),
        (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)",
            ["fineGrainedLineages", 0, "upstreams", 1],
        ),
        (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,downstream_table,PROD),c3)",
            ["fineGrainedLineages", 0, "downstreams", 0],
        ),
        (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,downstream_table,PROD),c4)",
            ["fineGrainedLineages", 0, "downstreams", 1],
        ),
        (
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)",
            ["fineGrainedLineages", 1, "upstreams", 0],
        ),
        (
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,downstream_table,PROD),c5)",
            ["fineGrainedLineages", 1, "downstreams", 0],
        ),
    ]


def _make_test_lineage_obj(
    table: str, upstream: str, downstream: str
) -> MetadataChangeProposalWrapper:
    lineage = UpstreamLineage(
        upstreams=[
            Upstream(
                dataset=_datasetUrn(upstream),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.DATASET,
                upstreams=[_datasetUrn(upstream)],
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[_fldUrn(downstream, "c5")],
            ),
        ],
    )

    return MetadataChangeProposalWrapper(entityUrn=_datasetUrn(table), aspect=lineage)


def test_dataset_urn_lowercase_transformer():
    original = _make_test_lineage_obj(
        "mainTableName", "upstreamTable", "downstreamTable"
    )

    expected = _make_test_lineage_obj(
        "maintablename", "upstreamtable", "downstreamtable"
    )

    assert original != expected  # sanity check

    lowercase_dataset_urns(original)
    assert original == expected


# Cross-language contract with metadata-io AliasesUtilsTest: GMS derives the indexed
# aliases.lowercasedUrn with the same rule, and any drift makes lookups miss silently.
# Keep the two lists in sync.
LOWERCASE_DATASET_URN_CASES = [
    # Platform casing is preserved; only the name is lowercased.
    (
        "urn:li:dataset:(urn:li:dataPlatform:adlsGen2,Container/Folder,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:adlsGen2,container/folder,PROD)",
    ),
    (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.Schema.Table,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
    ),
    # env is untouched.
    (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,DEV)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,DEV)",
    ),
    # A platform instance is fused into the name, so it lowercases with it.
    (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,My_Instance.DB.Schema.Table,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_instance.db.schema.table,PROD)",
    ),
    # Non-ASCII: Java uses Locale.ROOT so the key cannot depend on the JVM locale, and
    # Python's str.lower() is locale-independent. Both must land on the same string.
    (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,CAFÉ.Ñ_TITLE,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,café.ñ_title,PROD)",
    ),
    # Idempotent on an already-lowercased URN.
    (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
    ),
]


@pytest.mark.parametrize("urn,expected", LOWERCASE_DATASET_URN_CASES)
def test_lowercase_dataset_urn_matches_server_derivation(
    urn: str, expected: str
) -> None:
    assert lowercase_dataset_urn(urn) == expected
