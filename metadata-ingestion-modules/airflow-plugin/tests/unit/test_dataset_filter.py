from typing import List, Optional

from datahub.api.entities.datajob import DataJob
from datahub.configuration.common import AllowDenyPattern
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub_airflow_plugin._dataset_filter import DatasetFilter, apply_dataset_filter

KEPT = DatasetUrn("bigquery", "my_project.my_dataset.events")
TMP_FILE = DatasetUrn("file", "/tmp/tmpab12/out.csv")
OUTPUT = DatasetUrn("bigquery", "my_project.my_dataset.daily")

# Deny local files and one scratch bucket; BigQuery hidden datasets are dropped
# by the default "_" prefix without any pattern.
DENY = DatasetFilter(
    pattern=AllowDenyPattern(deny=[r"file:.*", r"gcs:my-scratch-bucket/.*"]),
    bigquery_temp_table_dataset_prefix="_",
)


def _field(dataset: DatasetUrn, column: str) -> str:
    return f"urn:li:schemaField:({dataset},{column})"


def _fgl(upstreams: List[str], downstreams: List[str]) -> FineGrainedLineageClass:
    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        upstreams=upstreams,
        downstreams=downstreams,
    )


def _datajob(
    inlets: List[DatasetUrn],
    outlets: List[DatasetUrn],
    fine_grained_lineages: Optional[List[FineGrainedLineageClass]] = None,
) -> DataJob:
    return DataJob(
        id="my_task",
        flow_urn=DataFlowUrn("airflow", "my_dag", "prod"),
        inlets=inlets,
        outlets=outlets,
        fine_grained_lineages=fine_grained_lineages or [],
    )


def test_drops_denied_inlets_and_outlets() -> None:
    anonymous = DatasetUrn("bigquery", "my_project._abc123.anon")
    datajob = _datajob(inlets=[KEPT, TMP_FILE, anonymous], outlets=[OUTPUT, TMP_FILE])

    apply_dataset_filter(datajob, DENY)

    assert datajob.inlets == [KEPT]
    assert datajob.outlets == [OUTPUT]


def test_prunes_fine_grained_lineage_to_kept_datasets() -> None:
    datajob = _datajob(
        inlets=[KEPT, TMP_FILE],
        outlets=[OUTPUT],
        fine_grained_lineages=[
            # Mixed upstreams: the denied one is removed, the edge survives.
            _fgl([_field(KEPT, "a"), _field(TMP_FILE, "a")], [_field(OUTPUT, "a")]),
            # Only denied upstreams: the edge is dropped rather than left sourceless.
            _fgl([_field(TMP_FILE, "b")], [_field(OUTPUT, "b")]),
            # Denied downstream: the edge is dropped.
            _fgl([_field(KEPT, "c")], [_field(TMP_FILE, "c")]),
        ],
    )

    apply_dataset_filter(datajob, DENY)

    assert len(datajob.fine_grained_lineages) == 1
    assert datajob.fine_grained_lineages[0].upstreams == [_field(KEPT, "a")]
    assert datajob.fine_grained_lineages[0].downstreams == [_field(OUTPUT, "a")]


def test_allow_all_leaves_datajob_untouched() -> None:
    datajob = _datajob(inlets=[KEPT, TMP_FILE], outlets=[OUTPUT])

    apply_dataset_filter(
        datajob,
        DatasetFilter(
            pattern=AllowDenyPattern.allow_all(), bigquery_temp_table_dataset_prefix=""
        ),
    )

    assert datajob.inlets == [KEPT, TMP_FILE]
    assert datajob.outlets == [OUTPUT]


def test_bigquery_hidden_datasets_dropped_unless_prefix_cleared() -> None:
    hidden = [
        DatasetUrn("bigquery", "my_project._6f2a9c.anon"),
        DatasetUrn("bigquery", "my_project._script7a1.tmp"),
    ]
    # A table whose own name starts with "_" is not in a hidden dataset.
    named = DatasetUrn("bigquery", "my_project.my_dataset._staging")

    datajob = _datajob(inlets=[*hidden, named, KEPT], outlets=[OUTPUT])
    apply_dataset_filter(datajob, DENY)
    assert datajob.inlets == [named, KEPT]

    datajob = _datajob(inlets=[*hidden, KEPT], outlets=[OUTPUT])
    keep_hidden = DatasetFilter(
        pattern=AllowDenyPattern.allow_all(), bigquery_temp_table_dataset_prefix=""
    )
    apply_dataset_filter(datajob, keep_hidden)
    assert datajob.inlets == [*hidden, KEPT]
