from typing import List

import pytest

from datahub.api.entities.datajob import DataJob
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub_airflow_plugin._path_specs import DatasetPathMapper


def _mapper(*includes: str, **kwargs: object) -> DatasetPathMapper:
    return DatasetPathMapper(
        [PathSpec.model_validate({"include": i, **kwargs}) for i in includes]
    )


@pytest.mark.parametrize(
    ("include", "platform", "name", "expected"),
    [
        # A file inside a per-run folder collapses to the table folder.
        (
            "gs://my-bucket/{table}",
            "gcs",
            "my-bucket/events/run_123/part-0001.json",
            "my-bucket/events",
        ),
        # The table folder itself, with or without a trailing slash.
        ("gs://my-bucket/{table}", "gcs", "my-bucket/events", "my-bucket/events"),
        # A partition folder rather than a file, below a wildcard level.
        (
            "gs://my-bucket/raw/*/{table}/{partition_key[0]}={partition[0]}/*.parquet",
            "gcs",
            "my-bucket/raw/eu/orders/dt=2026-01-01",
            "my-bucket/raw/eu/orders",
        ),
        (
            "s3://my-bucket/{table}/*.csv",
            "s3",
            "my-bucket/sales/2026/a.csv",
            "my-bucket/sales",
        ),
        # Local paths keep their leading slash.
        (
            "/home/airflow/data/{table}",
            "file",
            "/home/airflow/data/exports/run_9/out.csv",
            "/home/airflow/data/exports",
        ),
    ],
)
def test_collapses_to_table_folder(
    include: str, platform: str, name: str, expected: str
) -> None:
    urn = _mapper(include).map_urn(DatasetUrn(platform, name, "PROD"))
    assert urn == DatasetUrn(platform, expected, "PROD")


@pytest.mark.parametrize(
    "name",
    [
        "other-bucket/events/x",  # different bucket
        "my-bucket",  # above the {table} level
        "my-bucket/_tmp/x",  # hidden folder
        "my-bucket/skip/x",  # excluded
        "my-bucket/scratch_1/x",  # denied by tables_filter_pattern
    ],
)
def test_leaves_non_matching_paths_alone(name: str) -> None:
    mapper = _mapper(
        "gs://my-bucket/{table}",
        exclude=["gs://my-bucket/skip/**"],
        tables_filter_pattern={"deny": ["scratch_.*"]},
    )
    urn = DatasetUrn("gcs", name, "PROD")
    assert mapper.map_urn(urn) == urn


def test_only_applies_to_matching_platform_and_keeps_env() -> None:
    mapper = _mapper("gs://my-bucket/{table}")
    table = DatasetUrn("bigquery", "my_project.my_dataset.events", "DEV")
    assert mapper.map_urn(table) == table
    assert mapper.map_urn(DatasetUrn("gcs", "my-bucket/t/run_1", "DEV")) == (
        DatasetUrn("gcs", "my-bucket/t", "DEV")
    )


def _field(dataset: DatasetUrn, column: str) -> str:
    return f"urn:li:schemaField:({dataset},{column})"


def test_apply_dedupes_collapsed_iolets_and_rewrites_column_lineage() -> None:
    run_files: List[DatasetUrn] = [
        DatasetUrn("gcs", f"my-bucket/events/run_{i}/part-0.json", "PROD")
        for i in range(3)
    ]
    output = DatasetUrn("bigquery", "my_project.my_dataset.daily", "PROD")
    datajob = DataJob(
        id="my_task",
        flow_urn=DataFlowUrn("airflow", "my_dag", "prod"),
        inlets=list(run_files),
        outlets=[output],
        fine_grained_lineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[_field(u, "a") for u in run_files],
                downstreams=[_field(output, "a")],
            )
        ],
    )

    _mapper("gs://my-bucket/{table}").apply(datajob)

    table = DatasetUrn("gcs", "my-bucket/events", "PROD")
    assert datajob.inlets == [table]
    assert datajob.outlets == [output]
    assert datajob.fine_grained_lineages[0].upstreams == [_field(table, "a")]
    assert datajob.fine_grained_lineages[0].downstreams == [_field(output, "a")]


@pytest.mark.parametrize(
    "include",
    [
        "gs://my-bucket/events/*.json",  # no {table}: nothing to collapse to
        "https://account.blob.core.windows.net/container/{table}",  # unsupported
    ],
)
def test_rejects_unusable_path_specs(include: str) -> None:
    with pytest.raises(ValueError):
        _mapper(include)
