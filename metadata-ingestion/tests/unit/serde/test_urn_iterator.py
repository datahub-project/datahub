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
from datahub.utilities.urns.urn_iter import list_urns_with_path, lowercase_dataset_urns


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
