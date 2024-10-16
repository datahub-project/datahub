from unittest.mock import Mock, patch

import pytest
from dagster import (
    DagsterInstance,
    In,
    Out,
    SkipReason,
    build_run_status_sensor_context,
    build_sensor_context,
    job,
    op,
)
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstanceKey,
    InstanceRunResult,
)
from datahub.configuration.source_common import DEFAULT_ENV
from datahub.ingestion.graph.client import DatahubClientConfig

from datahub_dagster_plugin.client.dagster_generator import DatahubDagsterSourceConfig
from datahub_dagster_plugin.sensors.datahub_sensors import (
    DatahubSensors,
    make_datahub_sensor,
)


@patch("datahub.ingestion.graph.client.DataHubGraph", autospec=True)
@pytest.mark.skip(reason="disabling this test unti it will use proper golden files")
def test_datahub_sensor(mock_emit):
    instance = DagsterInstance.ephemeral()
    context = build_sensor_context(instance=instance)
    mock_emit.return_value = Mock()

    config = DatahubDagsterSourceConfig(
        datahub_client_config=DatahubClientConfig(
            server="http://localhost:8081",
        ),
        dagster_url="http://localhost:3000",
    )

    datahub_sensor = make_datahub_sensor(config)
    skip_reason = datahub_sensor(context)
    assert isinstance(skip_reason, SkipReason)


@patch("datahub_dagster_plugin.sensors.datahub_sensors.DatahubClient", autospec=True)
@pytest.mark.skip(reason="disabling this test unti it will use proper golden files")
def test_emit_metadata(mock_emit):
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    @op(
        out={
            "result": Out(
                metadata={
                    "datahub.outputs": [
                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableB,PROD)"
                    ]
                }
            )
        }
    )
    def extract():
        results = [1, 2, 3, 4]
        return results

    @op(
        ins={
            "data": In(
                metadata={
                    "datahub.inputs": [
                        "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableA,PROD)"
                    ]
                }
            )
        }
    )
    def transform(data):
        results = []
        for each in data:
            results.append(str(each))
        return results

    @job
    def etl():
        transform(extract())

    instance = DagsterInstance.ephemeral()
    result = etl.execute_in_process(instance=instance)

    # retrieve the DagsterRun
    dagster_run = result.dagster_run

    # retrieve a success event from the completed execution
    dagster_event = result.get_job_success_event()

    # create the context
    run_status_sensor_context = build_run_status_sensor_context(
        sensor_name="my_email_sensor",
        dagster_instance=instance,
        dagster_run=dagster_run,
        dagster_event=dagster_event,
    )

    DatahubSensors()._emit_metadata(run_status_sensor_context)

    expected_dataflow_urn = (
        f"urn:li:dataFlow:(dagster,{dagster_run.job_name},{DEFAULT_ENV})"
    )
    assert mock_emitter.method_calls[1][1][0].aspectName == "dataFlowInfo"
    assert mock_emitter.method_calls[1][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[2][1][0].aspectName == "ownership"
    assert mock_emitter.method_calls[2][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[3][1][0].aspectName == "globalTags"
    assert mock_emitter.method_calls[3][1][0].entityUrn == expected_dataflow_urn

    dpi_id = DataProcessInstanceKey(
        cluster=DEFAULT_ENV,
        orchestrator="dagster",
        id=dagster_run.run_id,
    ).guid()
    assert (
        mock_emitter.method_calls[7][1][0].aspectName == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[7][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[8][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[8][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[9][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[9][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[10][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[10][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[10][1][0].aspect.result.type
        == InstanceRunResult.SUCCESS
    )
    assert mock_emitter.method_calls[11][1][0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[11][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},extract)"
    )
    assert mock_emitter.method_calls[12][1][0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[12][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},extract)"
    )
    assert mock_emitter.method_calls[13][1][0].aspectName == "status"
    assert (
        mock_emitter.method_calls[13][1][0].entityUrn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableB,PROD)"
    )
    assert mock_emitter.method_calls[14][1][0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[14][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},extract)"
    )
    assert mock_emitter.method_calls[15][1][0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[15][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},extract)"
    )
    dpi_id = DataProcessInstanceKey(
        cluster=DEFAULT_ENV,
        orchestrator="dagster",
        id=f"{dagster_run.run_id}.extract",
    ).guid()
    assert (
        mock_emitter.method_calls[21][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[21][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[22][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[22][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert mock_emitter.method_calls[23][1][0].aspectName == "dataProcessInstanceOutput"
    assert (
        mock_emitter.method_calls[23][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert mock_emitter.method_calls[24][1][0].aspectName == "status"
    assert (
        mock_emitter.method_calls[24][1][0].entityUrn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableB,PROD)"
    )
    assert (
        mock_emitter.method_calls[25][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[25][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[26][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[26][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[26][1][0].aspect.result.type
        == InstanceRunResult.SUCCESS
    )
    assert mock_emitter.method_calls[27][1][0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[27][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},transform)"
    )
    assert mock_emitter.method_calls[28][1][0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[28][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},transform)"
    )
    assert mock_emitter.method_calls[29][1][0].aspectName == "status"
    assert (
        mock_emitter.method_calls[29][1][0].entityUrn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableA,PROD)"
    )
    assert mock_emitter.method_calls[30][1][0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[30][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},transform)"
    )
    assert mock_emitter.method_calls[31][1][0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[31][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},transform)"
    )
    dpi_id = DataProcessInstanceKey(
        cluster=DEFAULT_ENV,
        orchestrator="dagster",
        id=f"{dagster_run.run_id}.transform",
    ).guid()
    assert (
        mock_emitter.method_calls[37][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[37][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[38][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[38][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert mock_emitter.method_calls[39][1][0].aspectName == "dataProcessInstanceInput"
    assert (
        mock_emitter.method_calls[39][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert mock_emitter.method_calls[40][1][0].aspectName == "status"
    assert (
        mock_emitter.method_calls[40][1][0].entityUrn
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,tableA,PROD)"
    )
    assert (
        mock_emitter.method_calls[41][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[41][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[42][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[42][1][0].entityUrn
        == f"urn:li:dataProcessInstance:{dpi_id}"
    )
    assert (
        mock_emitter.method_calls[42][1][0].aspect.result.type
        == InstanceRunResult.SUCCESS
    )
