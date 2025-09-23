import json
import urllib

import pytest
import tenacity

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.sink.file import FileSink
from datahub.metadata.com.linkedin.pegasus2avro.assertion import AssertionStdAggregation
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
    PartitionSpecClass,
    PartitionTypeClass,
)
from tests.utils import delete_urns_from_file, get_sleep_info, ingest_file_via_rest

restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}
sleep_sec, sleep_times = get_sleep_info()


def create_test_data(test_file):
    assertion_urn = "urn:li:assertion:2d3b06a6e77e1f24adc9860a05ea089b"
    dataset_urn = make_dataset_urn(platform="postgres", name="foo")
    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.DATASET,
        customProperties={"suite_name": "demo_suite"},
        datasetAssertion=DatasetAssertionInfoClass(
            fields=[make_schema_field_urn(dataset_urn, "col1")],
            dataset=dataset_urn,
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass.LESS_THAN,
            nativeType="column_value_is_less_than",
            aggregation=AssertionStdAggregation.IDENTITY,
            nativeParameters={"max_value": "99"},
        ),
    )
    # The assertion definition
    mcp1 = MetadataChangeProposalWrapper(
        entityType="assertion",
        changeType="UPSERT",
        entityUrn=assertion_urn,
        aspectName="assertionInfo",
        aspect=assertion_info,
    )
    timestamps = [
        1643794280350,
        1643794280352,
        1643794280354,
        1643880726872,
        1643880726874,
        1643880726875,
    ]
    # The assertion run event attached to the dataset
    mcp2 = MetadataChangeProposalWrapper(
        entityType="assertion",
        entityUrn=assertion_urn,
        changeType="UPSERT",
        aspectName="assertionRunEvent",
        aspect=AssertionRunEventClass(
            timestampMillis=timestamps[0],
            partitionSpec=PartitionSpecClass(
                partition="[{'country': 'IN'}]",
                type=PartitionTypeClass.PARTITION,
            ),
            messageId=str(timestamps[0]),
            assertionUrn=assertion_urn,
            asserteeUrn=dataset_urn,
            result=AssertionResultClass(
                type=AssertionResultTypeClass.SUCCESS,
                actualAggValue=90,
                externalUrl="http://example.com/uuid1",
            ),
            runId="uuid1",
            status=AssertionRunStatusClass.COMPLETE,
        ),
    )

    mcp3 = MetadataChangeProposalWrapper(
        entityType="assertion",
        entityUrn=assertion_urn,
        changeType="UPSERT",
        aspectName="assertionRunEvent",
        aspect=AssertionRunEventClass(
            timestampMillis=timestamps[1],
            partitionSpec=PartitionSpecClass(
                partition="[{'country': 'US'}]",
                type=PartitionTypeClass.PARTITION,
            ),
            messageId=str(timestamps[1]),
            assertionUrn=assertion_urn,
            asserteeUrn=dataset_urn,
            result=AssertionResultClass(
                type=AssertionResultTypeClass.FAILURE,
                actualAggValue=101,
                externalUrl="http://example.com/uuid1",
            ),
            runId="uuid1",
            status=AssertionRunStatusClass.COMPLETE,
        ),
    )
    # Result of evaluating this assertion on the whole dataset
    mcp4 = MetadataChangeProposalWrapper(
        entityType="assertion",
        entityUrn=assertion_urn,
        changeType="UPSERT",
        aspectName="assertionRunEvent",
        aspect=AssertionRunEventClass(
            timestampMillis=timestamps[2],
            partitionSpec=PartitionSpecClass(
                partition="FULL_TABLE_SNAPSHOT",
                type=PartitionTypeClass.FULL_TABLE,
            ),
            messageId=str(timestamps[2]),
            assertionUrn=assertion_urn,
            asserteeUrn=dataset_urn,
            result=AssertionResultClass(
                type=AssertionResultTypeClass.SUCCESS,
                actualAggValue=93,
                externalUrl="http://example.com/uuid1",
            ),
            runId="uuid1",
            status=AssertionRunStatusClass.COMPLETE,
        ),
    )

    mcp5 = MetadataChangeProposalWrapper(
        entityType="assertion",
        entityUrn=assertion_urn,
        changeType="UPSERT",
        aspectName="assertionRunEvent",
        aspect=AssertionRunEventClass(
            timestampMillis=timestamps[3],
            partitionSpec=PartitionSpecClass(
                partition="[{'country': 'IN'}]",
                type=PartitionTypeClass.PARTITION,
            ),
            messageId=str(timestamps[3]),
            assertionUrn=assertion_urn,
            asserteeUrn=dataset_urn,
            result=AssertionResultClass(
                type=AssertionResultTypeClass.SUCCESS,
                actualAggValue=90,
                externalUrl="http://example.com/uuid1",
            ),
            runId="uuid1",
            status=AssertionRunStatusClass.COMPLETE,
        ),
    )
    mcp6 = MetadataChangeProposalWrapper(
        entityType="assertion",
        entityUrn=assertion_urn,
        changeType="UPSERT",
        aspectName="assertionRunEvent",
        aspect=AssertionRunEventClass(
            timestampMillis=timestamps[4],
            partitionSpec=PartitionSpecClass(
                partition="[{'country': 'US'}]",
                type=PartitionTypeClass.PARTITION,
            ),
            messageId=str(timestamps[4]),
            assertionUrn=assertion_urn,
            asserteeUrn=dataset_urn,
            result=AssertionResultClass(
                type=AssertionResultTypeClass.FAILURE,
                actualAggValue=101,
                externalUrl="http://example.com/uuid1",
            ),
            runId="uuid1",
            status=AssertionRunStatusClass.COMPLETE,
        ),
    )

    # Result of evaluating this assertion on the whole dataset
    mcp7 = MetadataChangeProposalWrapper(
        entityType="assertion",
        entityUrn=assertion_urn,
        changeType="UPSERT",
        aspectName="assertionRunEvent",
        aspect=AssertionRunEventClass(
            timestampMillis=timestamps[5],
            partitionSpec=PartitionSpecClass(
                partition="FULL_TABLE_SNAPSHOT",
                type=PartitionTypeClass.FULL_TABLE,
            ),
            messageId=str(timestamps[5]),
            assertionUrn=assertion_urn,
            asserteeUrn=dataset_urn,
            result=AssertionResultClass(
                type=AssertionResultTypeClass.SUCCESS,
                actualAggValue=93,
                externalUrl="http://example.com/uuid1",
            ),
            runId="uuid1",
            status=AssertionRunStatusClass.COMPLETE,
        ),
    )

    fileSink: FileSink = FileSink.create(
        {"filename": test_file}, ctx=PipelineContext(run_id="test-file")
    )
    for mcp in [mcp1, mcp2, mcp3, mcp4, mcp5, mcp6, mcp7]:
        fileSink.write_record_async(
            RecordEnvelope(record=mcp, metadata={}), write_callback=NoopWriteCallback()
        )
    fileSink.close()


@pytest.fixture(scope="module")
def generate_test_data(graph_client, tmp_path_factory):
    """Generates metadata events data and stores into a test file"""
    print("generating assertions test data")
    dir_name = tmp_path_factory.mktemp("test_dq_events")
    file_name = dir_name / "test_dq_events.json"
    create_test_data(test_file=str(file_name))
    yield str(file_name)
    print("removing assertions test data")
    delete_urns_from_file(graph_client, str(file_name))


@pytest.fixture(scope="module")
def test_run_ingestion(auth_session, generate_test_data):
    ingest_file_via_rest(auth_session, generate_test_data)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _gms_get_latest_assertions_results_by_partition(auth_session):
    urn = make_dataset_urn("postgres", "foo")

    # Query
    # Given the dataset
    # show me latest assertion run events grouped-by date, partition, assertionId
    query = json.dumps(
        {
            "entityName": "assertion",
            "aspectName": "assertionRunEvent",
            "filter": {
                "or": [
                    {
                        "and": [
                            {
                                "field": "asserteeUrn",
                                "value": urn,
                                "condition": "EQUAL",
                            }
                        ]
                    }
                ]
            },
            "metrics": [{"fieldPath": "status", "aggregationType": "LATEST"}],
            "buckets": [
                {"key": "asserteeUrn", "type": "STRING_GROUPING_BUCKET"},
                {"key": "partitionSpec.partition", "type": "STRING_GROUPING_BUCKET"},
                {
                    "key": "timestampMillis",
                    "type": "DATE_GROUPING_BUCKET",
                    "timeWindowSize": {"multiple": 1, "unit": "DAY"},
                },
                {"key": "assertionUrn", "type": "STRING_GROUPING_BUCKET"},
            ],
        }
    )
    response = auth_session.post(
        f"{auth_session.gms_url()}/analytics?action=getTimeseriesStats",
        data=query,
        headers=restli_default_headers,
    )

    response.raise_for_status()
    data = response.json()

    assert data["value"]
    assert data["value"]["table"]
    assert sorted(data["value"]["table"]["columnNames"]) == [
        "asserteeUrn",
        "assertionUrn",
        "latest_status",
        "partitionSpec.partition",
        "timestampMillis",
    ]
    assert len(data["value"]["table"]["rows"]) == 6
    assert (
        data["value"]["table"]["rows"][0][
            data["value"]["table"]["columnNames"].index("asserteeUrn")
        ]
        == urn
    )


def test_gms_get_latest_assertions_results_by_partition(
    auth_session, test_run_ingestion
):
    _gms_get_latest_assertions_results_by_partition(auth_session)


def test_gms_get_assertions_on_dataset(auth_session, test_run_ingestion):
    """lists all assertion urns including those which may not have executed"""
    urn = make_dataset_urn("postgres", "foo")
    response = auth_session.get(
        f"{auth_session.gms_url()}/relationships?direction=INCOMING&urn={urllib.parse.quote(urn)}&types=Asserts"
    )

    response.raise_for_status()
    data = response.json()
    assert len(data["relationships"]) == 1


def test_gms_get_assertions_on_dataset_field(auth_session, test_run_ingestion):
    """lists all assertion urns including those which may not have executed"""
    dataset_urn = make_dataset_urn("postgres", "foo")
    field_urn = make_schema_field_urn(dataset_urn, "col1")
    response = auth_session.get(
        f"{auth_session.gms_url()}/relationships?direction=INCOMING&urn={urllib.parse.quote(field_urn)}&types=Asserts"
    )

    response.raise_for_status()
    data = response.json()
    assert len(data["relationships"]) == 1


def test_gms_get_assertion_info(auth_session, test_run_ingestion):
    assertion_urn = "urn:li:assertion:2d3b06a6e77e1f24adc9860a05ea089b"
    response = auth_session.get(
        f"{auth_session.gms_url()}/aspects/{urllib.parse.quote(assertion_urn)}\
            ?aspect=assertionInfo&version=0",
        headers=restli_default_headers,
    )

    response.raise_for_status()
    data = response.json()

    assert data["aspect"]
    assert data["aspect"]["com.linkedin.assertion.AssertionInfo"]
    assert data["aspect"]["com.linkedin.assertion.AssertionInfo"]["type"] == "DATASET"
    assert data["aspect"]["com.linkedin.assertion.AssertionInfo"]["datasetAssertion"][
        "scope"
    ]
