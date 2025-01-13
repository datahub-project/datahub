import logging
import os
import tempfile
import time
from random import randint
import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    MLMetricClass,
    MLHyperParamClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceKeyClass,
    MLTrainingRunPropertiesClass,
    AuditStampClass,
)
from tests.consistency_utils import wait_for_writes_to_sync

logger = logging.getLogger(__name__)


def create_sample_dpi():
    """Create a sample DataProcessInstance with realistic ML training properties"""
    # Generate timestamps
    current_time = int(time.time() * 1000)
    run_id = "run_abcde"
    dpi_urn = f"urn:li:dataProcessInstance:{run_id}"

    logger.info(f"Creating DPI with URN: {dpi_urn}")

    # Create key aspect
    dpi_key = DataProcessInstanceKeyClass(
        id=run_id
    )

    hyper_params = [
        MLHyperParamClass(
            name="alpha",
            value="0.05"
        ),
        MLHyperParamClass(
            name="beta",
            value="0.95"
        )
    ]

    metrics = [
        MLMetricClass(
            name="mse",
            value="0.05"
        ),
        MLMetricClass(
            name="r2",
            value="0.95"
        )
    ]

    # Create DPI properties
    dpi_props = DataProcessInstancePropertiesClass(
        name=f"Training {run_id}",
        type="BATCH_SCHEDULED",
        created=AuditStampClass(time=current_time, actor="urn:li:corpuser:datahub"),
        externalUrl="http://mlflow:5000",
        customProperties={
            "framework": "statsmodels",
            "python_version": "3.8",
        },
    )

    dpi_ml_props = MLTrainingRunPropertiesClass(
        hyperParams=hyper_params,
        trainingMetrics=metrics,
        outputUrls=["s3://my-bucket/ml/output"],
    )

    # Create the MCPs - one for the key, one for properties
    mcps = [
        # Key aspect
        MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            entityType="dataProcessInstance",
            aspectName="dataProcessInstanceKey",
            changeType="UPSERT",
            aspect=dpi_key
        ),
        # Properties aspect
        MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            entityType="dataProcessInstance",
            aspectName="dataProcessInstanceProperties",
            changeType="UPSERT",
            aspect=dpi_props
        ),
        MetadataChangeProposalWrapper(
            entityUrn=dpi_urn,
            entityType="dataProcessInstance",
            aspectName="mlTrainingRunProperties",
            changeType="UPSERT",
            aspect=dpi_ml_props
        )
    ]
    return mcps


@pytest.fixture(scope="module")
def ingest_cleanup_data(auth_session, graph_client, request):
    """Fixture to handle test data setup and cleanup"""
    try:
        logger.info("Starting DPI test data creation")
        mcps = create_sample_dpi()

        # Emit MCPs directly using graph client
        for mcp in mcps:
            logger.info(f"Emitting aspect: {mcp.aspect}")
            graph_client.emit(mcp)

        wait_for_writes_to_sync()

        # Verify entity exists
        dpi_urn = "urn:li:dataProcessInstance:run_abcde"
        logger.info(f"Verifying entity exists in graph... {dpi_urn}")

        # Try getting aspect
        dpi_props = graph_client.get_aspect(
            dpi_urn,
            DataProcessInstancePropertiesClass
        )
        dpi_key = graph_client.get_aspect(
            dpi_urn,
            DataProcessInstanceKeyClass
        )
        dpi_ml_props = graph_client.get_aspect(
            dpi_urn,
            MLTrainingRunPropertiesClass
        )

        logger.info(f"DPI properties from graph: {dpi_props}")
        logger.info(f"DPI key from graph: {dpi_key}")
        logger.info(f"DPI ML properties from graph: {dpi_ml_props}")

        yield

        logger.info("Cleaning up test data")
        graph_client.hard_delete_entity(dpi_urn)
        wait_for_writes_to_sync()

    except Exception as e:
        logger.error(f"Error in test setup/cleanup: {str(e)}")
        logger.error(f"Full exception: {e.__class__.__name__}")
        raise


def test_get_dpi(auth_session, ingest_cleanup_data):
    """Test getting a specific DPI entity"""
    logger.info("Starting DPI query test")

    json = {
        "query": """query dataProcessInstance($urn: String!) {
                dataProcessInstance(urn: $urn) {
                    urn
                    type
                    properties {
                        name
                        created {
                            time
                            actor
                        }
                        customProperties {
                            key
                            value
                        }
                        externalUrl
                    }
                    mlTrainingRunProperties {
                        hyperParams {
                            name
                            value
                        }
                        trainingMetrics {
                            name
                            value
                        }
                        outputUrls
                    }
                }
            }""",
        "variables": {
            "urn": "urn:li:dataProcessInstance:run_abcde"
        }
    }

    # Send GraphQL query
    logger.info("Sending GraphQL query")
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    logger.info(f"Response data: {res_data}")

    # Basic response structure validation
    assert res_data, "Response should not be empty"
    assert "data" in res_data, "Response should contain 'data' field"
    assert "dataProcessInstance" in res_data["data"], "Response should contain 'dataProcessInstance' field"

    dpi = res_data["data"]["dataProcessInstance"]
    assert dpi, "DPI should not be null"
    assert "urn" in dpi, "DPI should have URN"
    assert dpi["urn"] == "urn:li:dataProcessInstance:run_abcde", "URN should match expected value"

    # Validate properties if present
    if "properties" in dpi and dpi["properties"]:
        props = dpi["properties"]
        assert "name" in props, "Properties should contain name"
        assert "created" in props, "Properties should contain created"
        assert "customProperties" in props, "Properties should contain customProperties"

    if "mlTrainingRunProperties" in dpi and dpi["mlTrainingRunProperties"]:
        ml_props = dpi["mlTrainingRunProperties"]
        assert "hyperParams" in ml_props, "ML properties should contain hyperParams"
        assert "trainingMetrics" in ml_props, "ML properties should contain trainingMetrics"
        assert "outputUrls" in ml_props, "ML properties should contain outputUrls"