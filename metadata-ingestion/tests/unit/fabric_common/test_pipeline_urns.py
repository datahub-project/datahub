"""The URN shapes shared by the Fabric pipeline connectors: a change here
silently detaches every activity DataJob and run from its pipeline."""

from datahub.ingestion.source.fabric.common.urn_generator import (
    make_activity_job_urn,
    make_pipeline_flow_urn,
)

WS = "00000000-0000-0000-0000-00000000000a"
PIPELINE = "00000000-0000-0000-0000-00000000000b"


def test_activity_job_urn_nests_the_pipeline_flow_urn() -> None:
    flow_urn = make_pipeline_flow_urn(
        WS, PIPELINE, platform="fabric-data-factory", env="PROD", platform_instance="eu"
    )
    job_urn = make_activity_job_urn("Copy data", flow_urn)

    # The platform instance is folded into the flow id, as DataFlowUrn does.
    assert flow_urn.flow_id == f"eu.{WS}.{PIPELINE}"
    assert flow_urn.cluster == "PROD"
    assert str(job_urn) == f"urn:li:dataJob:({flow_urn},Copy data)"
