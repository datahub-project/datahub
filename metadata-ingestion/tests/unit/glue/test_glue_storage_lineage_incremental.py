from typing import Any, Dict, List
from unittest.mock import PropertyMock, patch

import pytest
from botocore.stub import Stubber

import datahub.metadata.schema_classes as models
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.run.pipeline_config import FlagsConfig
from datahub.ingestion.source.aws.glue import GlueSource, GlueSourceConfig
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage

LOCATION = "s3://my-bucket/my_db/events/"
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:glue,my_db.events,PROD)"

TABLE: Dict[str, Any] = {
    "Name": "events",
    "DatabaseName": "my_db",
    "CatalogId": "123412341234",
    "Owner": "owner",
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {"classification": "parquet"},
    "StorageDescriptor": {
        "Columns": [
            {"Name": "col_a", "Type": "string", "Parameters": {"k": "v"}},
            {"Name": "col_b", "Type": "int"},
        ],
        "Location": LOCATION,
    },
}


def _run(
    extract_column_parameters: bool, browse_paths_v2: bool
) -> List[MetadataWorkUnit]:
    source = GlueSource(
        ctx=PipelineContext(run_id="glue-storage-lineage-incremental"),
        config=GlueSourceConfig(
            aws_region="us-west-2",
            extract_transforms=False,
            use_s3_bucket_tags=False,
            use_s3_object_tags=False,
            emit_storage_lineage=True,
            include_column_lineage=True,
            incremental_properties=True,
            extract_column_parameters=extract_column_parameters,
        ),
    )
    with (
        patch.object(
            PipelineContext,
            "flags",
            new_callable=PropertyMock,
            return_value=FlagsConfig(generate_browse_path_v2=browse_paths_v2),
        ),
        Stubber(source.glue_client) as glue_stubber,
    ):
        glue_stubber.add_response(
            "get_databases",
            {"DatabaseList": [{"Name": "my_db", "CatalogId": "123412341234"}]},
            {},
        )
        glue_stubber.add_response(
            "get_tables", {"TableList": [TABLE]}, {"DatabaseName": "my_db"}
        )
        wus = list(source.get_workunits())

    assert not source.report.failures
    return wus


# Column parameters (other URNs between the dataset's work units) and browse paths v2 off change
# when AutoIncrementalPropertiesProcessor turns DatasetProperties into a PATCH; storage lineage
# must not depend on that timing.
@pytest.mark.parametrize(
    "extract_column_parameters, browse_paths_v2",
    [(False, True), (True, True), (False, False)],
    ids=["defaults", "column_parameters", "browse_paths_v2_off"],
)
def test_storage_lineage_with_incremental_properties(
    extract_column_parameters: bool, browse_paths_v2: bool
) -> None:
    wus = _run(extract_column_parameters, browse_paths_v2)

    lineages = [
        lineage
        for wu in wus
        if wu.get_urn() == DATASET_URN
        and (lineage := wu.get_aspect_of_type(models.UpstreamLineageClass))
    ]
    _assert_properties_patched(wus)
    assert len(lineages) == 1
    lineage = lineages[0]
    assert [u.dataset for u in lineage.upstreams] == [
        make_s3_urn_for_lineage(LOCATION, "PROD")
    ]
    assert lineage.fineGrainedLineages
    assert {
        d for fgl in lineage.fineGrainedLineages for d in (fgl.downstreams or [])
    } == {
        f"urn:li:schemaField:({DATASET_URN},col_a)",
        f"urn:li:schemaField:({DATASET_URN},col_b)",
    }


def _assert_properties_patched(wus: List[MetadataWorkUnit]) -> None:
    properties_writes = [
        wu.metadata
        for wu in wus
        if wu.get_urn() == DATASET_URN
        and not isinstance(wu.metadata, models.MetadataChangeEventClass)
        and wu.metadata.aspectName == "datasetProperties"
    ]
    assert len(properties_writes) == 1
    properties_patch = properties_writes[0]
    assert isinstance(properties_patch, models.MetadataChangeProposalClass)
    assert properties_patch.changeType == models.ChangeTypeClass.PATCH
    assert properties_patch.aspect is not None
    assert b"/customProperties/Location" in properties_patch.aspect.value
