import datahub.emitter.mce_builder as builder
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


def _datasetUrn(tbl):
    return builder.make_dataset_urn("bigquery", tbl, "PROD")


def _fldUrn(tbl, fld):
    return builder.make_schema_field_urn(_datasetUrn(tbl), fld)


def test_list_urns_upstream():
    upstream_table = Upstream(
        dataset=_datasetUrn("upstream_table_1"),
        type=DatasetLineageTypeClass.TRANSFORMED,
    )

    urns = upstream_table.list_urns()
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


def test_list_urns_fine_grained():
    x = FineGrainedLineage(
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
    )

    # TODO: this doesn't handle array[Urn] (or array[CorpuserUrn], etc) correctly,
    # since the java types don't get displayed in the avro annotations

    urns = x.list_urns()
    assert len(urns) == 4
    assert urns == [...]  # TODO


# def test_upstream_lineage_urn_iterator():
#     upstream_table_1 = Upstream(
#         dataset=_datasetUrn("upstream_table_1"),
#         type=DatasetLineageTypeClass.TRANSFORMED,
#     )
#     upstream_table_2 = Upstream(
#         dataset=_datasetUrn("upstream_table_2"),
#         type=DatasetLineageTypeClass.TRANSFORMED,
#     )


#     # Construct a lineage aspect.
#     upstream_lineage = UpstreamLineage(
#         upstreams=[upstream_table_1, upstream_table_2],
#         fineGrainedLineages=[
#             FineGrainedLineage(
#                 upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
#                 upstreams=[
#                     _fldUrn("upstream_table_1", "c1"),
#                     _fldUrn("upstream_table_2", "c2"),
#                 ],
#                 downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
#                 downstreams=[
#                     _fldUrn("downstream_table", "c3"),
#                     _fldUrn("downstream_table", "c4"),
#                 ],
#             ),
#             FineGrainedLineage(
#                 upstreamType=FineGrainedLineageUpstreamType.DATASET,
#                 upstreams=[_datasetUrn("upstream_table_1")],
#                 downstreamType=FineGrainedLineageDownstreamType.FIELD,
#                 downstreams=[_fldUrn("downstream_table", "c5")],
#             ),
#         ],
#     )
#     print(upstream_lineage)

#     urns = upstream_lineage.list_urns()
#     assert urns != [
#         (
#             "urn:li:corpuser:unknown",
#             ["upstreams", 0, "auditStamp", "actor"],
#         ),
#         (
#             "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)",
#             ["upstreams", 0, "dataset"],
#         ),
#         ("urn:li:corpuser:unknown", ["upstreams", 1, "auditStamp", "actor"]),
#         (
#             "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD)",
#             ["upstreams", 1, "dataset"],
#         ),
#     ]


# TODO chartInfo urns
# TODO mcpw urns
