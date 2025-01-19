import logging
import os
import tempfile
from random import randint

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DataPlatformInstancePropertiesClass,
    DataProcessInstanceKeyClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRunEventClass,
    MLHyperParamClass,
    MLMetricClass,
    MLTrainingRunPropertiesClass,
    SubTypesClass,
    TimeWindowSizeClass,
)
from tests.utils import (
    delete_urns_from_file,
    ingest_file_via_rest,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

# Generate unique DPI ID
dpi_id = f"test-pipeline-run-{randint(1000, 9999)}"
dpi_urn = f"urn:li:dataProcessInstance:{dpi_id}"


class FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="create_test_data"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event):
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self):
        self.sink.close()


def create_test_data(filename: str):
    mcps = [
        # Key aspect
        MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=dpi_urn,
            aspectName="dataProcessInstanceKey",
            aspect=DataProcessInstanceKeyClass(id=dpi_id),
        ),
        # Properties aspect
        MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=dpi_urn,
            aspectName="dataProcessInstanceProperties",
            aspect=DataProcessInstancePropertiesClass(
                name="Test Pipeline Run",
                type="BATCH_SCHEDULED",
                created=AuditStampClass(
                    time=1640692800000, actor="urn:li:corpuser:datahub"
                ),
            ),
        ),
        # Run Event aspect
        MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=dpi_urn,
            aspectName="dataProcessInstanceRunEvent",
            aspect=DataProcessInstanceRunEventClass(
                timestampMillis=1704067200000,
                eventGranularity=TimeWindowSizeClass(unit="WEEK", multiple=1),
                status="COMPLETE",
            ),
        ),
        # Platform Instance aspect
        MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=dpi_urn,
            aspectName="dataPlatformInstance",
            aspect=DataPlatformInstanceClass(
                platform="urn:li:dataPlatform:airflow",
                instance="urn:li:dataPlatformInstance:(urn:li:dataPlatform:airflow,1234567890)",
            ),
        ),
        MetadataChangeProposalWrapper(
            entityType="dataPlatformInstance",
            entityUrn="urn:li:dataPlatformInstance:(urn:li:dataPlatform:airflow,1234567890)",
            aspectName="dataPlatformInstanceProperties",
            aspect=DataPlatformInstancePropertiesClass(
                name="my process instance",
            ),
        ),
        # SubTypes aspect
        MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=dpi_urn,
            aspectName="subTypes",
            aspect=SubTypesClass(typeNames=["TEST", "BATCH_JOB"]),
        ),
        # Container aspect
        MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=dpi_urn,
            aspectName="container",
            aspect=ContainerClass(container="urn:li:container:testGroup1"),
        ),
        MetadataChangeProposalWrapper(
            entityType="container",
            entityUrn="urn:li:container:testGroup1",
            aspectName="containerProperties",
            aspect=ContainerPropertiesClass(name="testGroup1"),
        ),
        # ML Training Run Properties aspect
        MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=dpi_urn,
            aspectName="mlTrainingRunProperties",
            aspect=MLTrainingRunPropertiesClass(
                id="test-training-run-123",
                trainingMetrics=[
                    MLMetricClass(
                        name="accuracy",
                        description="accuracy of the model",
                        value="0.95",
                    ),
                    MLMetricClass(
                        name="loss",
                        description="accuracy loss of the model",
                        value="0.05",
                    ),
                ],
                hyperParams=[
                    MLHyperParamClass(
                        name="learningRate",
                        description="rate of learning",
                        value="0.001",
                    ),
                    MLHyperParamClass(
                        name="batchSize", description="size of the batch", value="32"
                    ),
                ],
                outputUrls=["s3://my-bucket/ml/output"],
            ),
        ),
    ]

    file_emitter = FileEmitter(filename)
    for mcp in mcps:
        file_emitter.emit(mcp)
    file_emitter.close()


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    new_file, filename = tempfile.mkstemp(suffix=".json")
    try:
        create_test_data(filename)
        print("ingesting data process instance test data")
        ingest_file_via_rest(auth_session, filename)
        wait_for_writes_to_sync()
        yield
        print("removing data process instance test data")
        delete_urns_from_file(graph_client, filename)
        wait_for_writes_to_sync()
    finally:
        os.remove(filename)


@pytest.mark.integration
def test_search_dpi(auth_session, ingest_cleanup_data):
    """Test DPI search and validation of returned fields using GraphQL."""

    json = {
        "query": """query scrollAcrossEntities($input: ScrollAcrossEntitiesInput!) {
            scrollAcrossEntities(input: $input) {
                nextScrollId
                count
                total
                searchResults {
                    entity {
                        ... on DataProcessInstance {
                            urn
                            properties {
                                name
                                externalUrl
                            }
                            dataPlatformInstance {
                                platform {
                                    urn
                                    name
                                }
                            }
                            subTypes {
                                typeNames
                            }
                            container {
                                urn
                            }
                            platform {
                                urn
                                name
                                properties {
                                    type
                                }
                            }
                            mlTrainingRunProperties {
                                id
                                trainingMetrics {
                                  name
                                  value
                                }
                                hyperParams {
                                  name
                                  value
                                }
                                outputUrls
                            }
                        }
                    }
                }
            }
        }""",
        "variables": {
            "input": {"types": ["DATA_PROCESS_INSTANCE"], "query": dpi_id, "count": 10}
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    # Basic response structure validation
    assert res_data, "Response should not be empty"
    assert "data" in res_data, "Response should contain 'data' field"
    print("RESPONSE DATA:" + str(res_data))
    assert "scrollAcrossEntities" in res_data["data"], (
        "Response should contain 'scrollAcrossEntities' field"
    )

    search_results = res_data["data"]["scrollAcrossEntities"]
    assert "searchResults" in search_results, (
        "Response should contain 'searchResults' field"
    )

    results = search_results["searchResults"]
    assert len(results) > 0, "Should find at least one result"

    # Find our test entity
    test_entity = None
    for result in results:
        if result["entity"]["urn"] == dpi_urn:
            test_entity = result["entity"]
            break

    assert test_entity is not None, f"Should find test entity with URN {dpi_urn}"

    # Validate fields
    props = test_entity["properties"]
    assert props["name"] == "Test Pipeline Run"

    platform_instance = test_entity["dataPlatformInstance"]
    assert platform_instance["platform"]["urn"] == "urn:li:dataPlatform:airflow"

    sub_types = test_entity["subTypes"]
    assert set(sub_types["typeNames"]) == {"TEST", "BATCH_JOB"}

    container = test_entity["container"]
    assert container["urn"] == "urn:li:container:testGroup1"

    ml_props = test_entity["mlTrainingRunProperties"]
    assert ml_props["id"] == "test-training-run-123"
    assert ml_props["trainingMetrics"][0] == {"name": "accuracy", "value": "0.95"}
    assert ml_props["trainingMetrics"][1] == {"name": "loss", "value": "0.05"}
    assert ml_props["hyperParams"][0] == {"name": "learningRate", "value": "0.001"}
    assert ml_props["hyperParams"][1] == {"name": "batchSize", "value": "32"}
    assert ml_props["outputUrls"][0] == "s3://my-bucket/ml/output"
