import asyncio
from unittest.mock import Mock, patch

from datahub.api.entities.datajob import DataJob
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub_provider.entities import Dataset

from prefect_datahub.datahub_emitter import DatahubEmitter


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_entities_to_urn_list(mock_emit):
    dataset_urn_list = DatahubEmitter()._entities_to_urn_list(
        [Dataset("snowflake", "mydb.schema.tableA")]
    )
    for dataset_urn in dataset_urn_list:
        assert isinstance(dataset_urn, DatasetUrn)


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_get_flow_run_graph(mock_emit, mock_prefect_client):
    graph_json = asyncio.run(
        DatahubEmitter()._get_flow_run_graph("c3b947e5-3fa1-4b46-a2e2-58d50c938f2e")
    )
    assert isinstance(graph_json, list)


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test__get_workspace(mock_emit, mock_prefect_cloud_client):
    workspace_name = DatahubEmitter()._get_workspace()
    assert workspace_name == "datahub"


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_add_task(mock_emit, mock_run_context):
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    datahub_emitter = DatahubEmitter()
    inputs = [Dataset("snowflake", "mydb.schema.tableA")]
    outputs = [Dataset("snowflake", "mydb.schema.tableC")]
    datahub_emitter.add_task(
        inputs=inputs,
        outputs=outputs,
    )

    task_run_ctx = mock_run_context[0]
    flow_run_ctx = mock_run_context[1]

    expected_datajob_urn = (
        f"urn:li:dataJob:(urn:li:dataFlow:"
        f"(prefect,{flow_run_ctx.flow.name},prod),{task_run_ctx.task.task_key})"
    )

    assert expected_datajob_urn in datahub_emitter.datajobs_to_emit.keys()
    actual_datajob = datahub_emitter.datajobs_to_emit[expected_datajob_urn]
    assert isinstance(actual_datajob, DataJob)
    assert str(actual_datajob.flow_urn) == "urn:li:dataFlow:(prefect,etl,prod)"
    assert actual_datajob.name == task_run_ctx.task.name
    assert actual_datajob.description == task_run_ctx.task.description
    assert actual_datajob.tags == task_run_ctx.task.tags
    assert (
        str(actual_datajob.inlets[0])
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableA,PROD)"
    )
    assert (
        str(actual_datajob.outlets[0])
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,mydb.schema.tableC,PROD)"
    )
    assert mock_emit.emit.call_count == 0


@patch("prefect_datahub.datahub_emitter.DatahubRestEmitter", autospec=True)
def test_emit_flow(
    mock_emit, mock_run_context, mock_prefect_client, mock_prefect_cloud_client
):
    mock_emitter = Mock()
    mock_emit.return_value = mock_emitter

    platform_instance = "datahub_workspace"

    datahub_emitter = DatahubEmitter(platform_instance=platform_instance)
    datahub_emitter.add_task()
    datahub_emitter.emit_flow()

    task_run_ctx = mock_run_context[0]
    flow_run_ctx = mock_run_context[1]

    expected_dataflow_urn = (
        f"urn:li:dataFlow:(prefect,{platform_instance}.{flow_run_ctx.flow.name},prod)"
    )

    assert mock_emitter.method_calls[1][1][0].aspectName == "dataFlowInfo"
    assert mock_emitter.method_calls[1][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[2][1][0].aspectName == "ownership"
    assert mock_emitter.method_calls[2][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[3][1][0].aspectName == "globalTags"
    assert mock_emitter.method_calls[3][1][0].entityUrn == expected_dataflow_urn
    assert mock_emitter.method_calls[4][1][0].aspectName == "browsePaths"
    assert mock_emitter.method_calls[4][1][0].entityUrn == expected_dataflow_urn
    assert (
        mock_emitter.method_calls[8][1][0].aspectName == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[8][1][0].entityUrn
        == "urn:li:dataProcessInstance:a95d24db6abd98384fc1d4c8540098a4"
    )
    assert (
        mock_emitter.method_calls[9][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[9][1][0].entityUrn
        == "urn:li:dataProcessInstance:a95d24db6abd98384fc1d4c8540098a4"
    )
    assert (
        mock_emitter.method_calls[10][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[10][1][0].entityUrn
        == "urn:li:dataProcessInstance:a95d24db6abd98384fc1d4c8540098a4"
    )
    assert mock_emitter.method_calls[11][1][0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[11][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[12][1][0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[12][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[13][1][0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[13][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[14][1][0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[14][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert mock_emitter.method_calls[15][1][0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[15][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.extract)"
    )
    assert (
        mock_emitter.method_calls[16][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[16][1][0].entityUrn
        == "urn:li:dataProcessInstance:bf5eab177af0097bbff6a41694f39af9"
    )
    assert (
        mock_emitter.method_calls[17][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[17][1][0].entityUrn
        == "urn:li:dataProcessInstance:bf5eab177af0097bbff6a41694f39af9"
    )
    assert (
        mock_emitter.method_calls[18][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[18][1][0].entityUrn
        == "urn:li:dataProcessInstance:bf5eab177af0097bbff6a41694f39af9"
    )
    assert (
        mock_emitter.method_calls[19][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[19][1][0].entityUrn
        == "urn:li:dataProcessInstance:bf5eab177af0097bbff6a41694f39af9"
    )
    assert mock_emitter.method_calls[20][1][0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[20][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[21][1][0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[21][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[22][1][0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[22][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[23][1][0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[23][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert mock_emitter.method_calls[24][1][0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[24][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.load)"
    )
    assert (
        mock_emitter.method_calls[25][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[25][1][0].entityUrn
        == "urn:li:dataProcessInstance:095673536b61e6f25c7691af0d2cc317"
    )
    assert (
        mock_emitter.method_calls[26][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[26][1][0].entityUrn
        == "urn:li:dataProcessInstance:095673536b61e6f25c7691af0d2cc317"
    )
    assert (
        mock_emitter.method_calls[27][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[27][1][0].entityUrn
        == "urn:li:dataProcessInstance:095673536b61e6f25c7691af0d2cc317"
    )
    assert (
        mock_emitter.method_calls[28][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[28][1][0].entityUrn
        == "urn:li:dataProcessInstance:095673536b61e6f25c7691af0d2cc317"
    )
    assert mock_emitter.method_calls[29][1][0].aspectName == "dataJobInfo"
    assert (
        mock_emitter.method_calls[29][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert mock_emitter.method_calls[30][1][0].aspectName == "dataJobInputOutput"
    assert (
        mock_emitter.method_calls[30][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert mock_emitter.method_calls[31][1][0].aspectName == "ownership"
    assert (
        mock_emitter.method_calls[31][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert mock_emitter.method_calls[32][1][0].aspectName == "globalTags"
    assert (
        mock_emitter.method_calls[32][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert (
        mock_emitter.method_calls[32][1][0].aspect.tags[0].tag
        == f"urn:li:tag:{task_run_ctx.task.tags[0]}"
    )
    assert mock_emitter.method_calls[33][1][0].aspectName == "browsePaths"
    assert (
        mock_emitter.method_calls[33][1][0].entityUrn
        == f"urn:li:dataJob:({expected_dataflow_urn},__main__.transform)"
    )
    assert (
        mock_emitter.method_calls[34][1][0].aspectName
        == "dataProcessInstanceProperties"
    )
    assert (
        mock_emitter.method_calls[34][1][0].entityUrn
        == "urn:li:dataProcessInstance:04ba0f8064b2c45f69da571c434f1c69"
    )
    assert (
        mock_emitter.method_calls[35][1][0].aspectName
        == "dataProcessInstanceRelationships"
    )
    assert (
        mock_emitter.method_calls[35][1][0].entityUrn
        == "urn:li:dataProcessInstance:04ba0f8064b2c45f69da571c434f1c69"
    )
    assert (
        mock_emitter.method_calls[36][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[36][1][0].entityUrn
        == "urn:li:dataProcessInstance:04ba0f8064b2c45f69da571c434f1c69"
    )
    assert (
        mock_emitter.method_calls[37][1][0].aspectName == "dataProcessInstanceRunEvent"
    )
    assert (
        mock_emitter.method_calls[37][1][0].entityUrn
        == "urn:li:dataProcessInstance:04ba0f8064b2c45f69da571c434f1c69"
    )
