from unittest.mock import MagicMock, patch

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)


@patch("datahub.ingestion.source.sql.postgres.source.create_engine")
def test_view_lineage_workunits_for_views_failed_parsing(
    create_engine_mock: MagicMock,
) -> None:
    config = PostgresConfig.model_validate(
        {
            "username": "user",
            "password": "password",
            "host_port": "host:5432",
            "database": "db1",
        }
    )
    source = PostgresSource(config, PipelineContext(run_id="test"))
    source.views_failed_parsing = {"db1.public.v1"}

    t1 = make_dataset_urn("postgres", "db1.public.t1")
    t2 = make_dataset_urn("postgres", "db1.public.t2")
    view_urn = make_dataset_urn("postgres", "db1.public.v1")

    with patch.object(
        source,
        "_get_view_lineage_elements",
        return_value={("v1", "public"): [t1, t2], ("v2", "public"): [t1]},
    ):
        workunits = list(source._get_view_lineage_workunits(MagicMock()))

    assert not source.report.failures
    assert len(workunits) == 1
    wu = workunits[0]
    assert wu.id == f"{view_urn}-upstreamLineage"
    assert wu.get_urn() == view_urn
    assert wu.get_aspect_of_type(UpstreamLineageClass) == UpstreamLineageClass(
        upstreams=[
            UpstreamClass(dataset=t1, type=DatasetLineageTypeClass.TRANSFORMED),
            UpstreamClass(dataset=t2, type=DatasetLineageTypeClass.TRANSFORMED),
        ]
    )
    assert wu.metadata.systemMetadata is None
