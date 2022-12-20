import typing
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.nifi import (
    NifiComponent,
    NifiFlow,
    NifiProcessGroup,
    NifiSource,
    NifiSourceConfig,
    NifiType,
)


@typing.no_type_check
def test_nifi_s3_provenance_event():
    config_dict = {
        "site_url": "http://localhost:8080",
    }
    nifi_config = NifiSourceConfig.parse_obj(config_dict)
    ctx = PipelineContext(run_id="test")

    with patch(
        "datahub.ingestion.source.nifi.NifiSource.fetch_provenance_events"
    ) as mock_provenance_events, patch(
        "datahub.ingestion.source.nifi.NifiSource.delete_provenance"
    ) as mock_delete_provenance:
        mocked_functions(mock_provenance_events, mock_delete_provenance, "puts3")

        nifi_source = NifiSource(nifi_config, ctx)

        nifi_source.nifi_flow = NifiFlow(
            version="1.15.0",
            clustered=False,
            root_process_group=NifiProcessGroup(
                id="803ebb92-017d-1000-2961-4bdaa27a3ba0",
                name="Standalone Flow",
                parent_group_id=None,
            ),
            components={
                "aed63edf-e660-3f29-b56b-192cf6286889": NifiComponent(
                    id="aed63edf-e660-3f29-b56b-192cf6286889",
                    name="PutS3Object",
                    type="org.apache.nifi.processors.aws.s3.PutS3Object",
                    parent_group_id="80404c81-017d-1000-e8e8-af7420af06c1",
                    nifi_type=NifiType.PROCESSOR,
                    comments="",
                    status=None,
                    inlets={},
                    outlets={},
                    config={},
                    target_uris=None,
                    last_event_time=None,
                )
            },
            remotely_accessible_ports={},
            connections=[],
            processGroups={
                "803ebb92-017d-1000-2961-4bdaa27a3ba0": NifiProcessGroup(
                    id="803ebb92-017d-1000-2961-4bdaa27a3ba0",
                    name="Standalone Flow",
                    parent_group_id=None,
                ),
                "80404c81-017d-1000-e8e8-af7420af06c1": NifiProcessGroup(
                    id="80404c81-017d-1000-e8e8-af7420af06c1",
                    name="Single_Site_S3_to_S3",
                    parent_group_id="803ebb92-017d-1000-2961-4bdaa27a3ba0",
                ),
            },
            remoteProcessGroups={},
            remote_ports={},
        )

        NifiSource.process_provenance_events(nifi_source)
        workunits = list(NifiSource.construct_workunits(nifi_source))

        # one aspect for dataflow and two aspects for datajob
        # and two aspects for dataset
        assert len(workunits) == 5
        assert workunits[0].metadata.entityType == "dataFlow"

        assert workunits[1].metadata.entityType == "dataset"
        assert workunits[2].metadata.entityType == "dataset"
        assert workunits[3].metadata.entityType == "dataJob"
        assert workunits[4].metadata.entityType == "dataJob"

        ioAspect = workunits[4].metadata.aspect
        assert ioAspect.outputDatasets == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,foo-nifi.tropical_data,PROD)"
        ]
        assert ioAspect.inputDatasets == []

    with patch(
        "datahub.ingestion.source.nifi.NifiSource.fetch_provenance_events"
    ) as mock_provenance_events, patch(
        "datahub.ingestion.source.nifi.NifiSource.delete_provenance"
    ) as mock_delete_provenance:
        mocked_functions(mock_provenance_events, mock_delete_provenance, "fetchs3")

        nifi_source = NifiSource(nifi_config, ctx)

        nifi_source.nifi_flow = NifiFlow(
            version="1.15.0",
            clustered=False,
            root_process_group=NifiProcessGroup(
                id="803ebb92-017d-1000-2961-4bdaa27a3ba0",
                name="Standalone Flow",
                parent_group_id=None,
            ),
            components={
                "91d59f03-1c2b-3f3f-48bc-f89296a328bd": NifiComponent(
                    id="91d59f03-1c2b-3f3f-48bc-f89296a328bd9",
                    name="FetchS3Object",
                    type="org.apache.nifi.processors.aws.s3.FetchS3Object",
                    parent_group_id="80404c81-017d-1000-e8e8-af7420af06c1",
                    nifi_type=NifiType.PROCESSOR,
                    comments="",
                    status=None,
                    inlets={},
                    outlets={},
                    config={},
                    target_uris=None,
                    last_event_time=None,
                )
            },
            remotely_accessible_ports={},
            connections=[],
            processGroups={
                "803ebb92-017d-1000-2961-4bdaa27a3ba0": NifiProcessGroup(
                    id="803ebb92-017d-1000-2961-4bdaa27a3ba0",
                    name="Standalone Flow",
                    parent_group_id=None,
                ),
                "80404c81-017d-1000-e8e8-af7420af06c1": NifiProcessGroup(
                    id="80404c81-017d-1000-e8e8-af7420af06c1",
                    name="Single_Site_S3_to_S3",
                    parent_group_id="803ebb92-017d-1000-2961-4bdaa27a3ba0",
                ),
            },
            remoteProcessGroups={},
            remote_ports={},
        )

        NifiSource.process_provenance_events(nifi_source)
        workunits = list(NifiSource.construct_workunits(nifi_source))

        # one aspect for dataflow and two aspects for datajob
        # and two aspects for dataset
        assert len(workunits) == 5
        assert workunits[0].metadata.entityType == "dataFlow"

        assert workunits[1].metadata.entityType == "dataset"
        assert workunits[2].metadata.entityType == "dataset"
        assert workunits[3].metadata.entityType == "dataJob"
        assert workunits[4].metadata.entityType == "dataJob"

        ioAspect = workunits[4].metadata.aspect
        assert ioAspect.outputDatasets == []
        assert ioAspect.inputDatasets == [
            "urn:li:dataset:(urn:li:dataPlatform:s3,enriched-topical-chat,PROD)"
        ]


def mocked_functions(mock_provenance_events, mock_delete_provenance, provenance_case):
    puts3_provenance_response = [
        {
            "id": "49",
            "eventId": 49,
            "eventTime": "12/08/2021 10:06:39.938 UTC",
            "eventType": "SEND",
            "flowFileUuid": "0b205834-5cac-4979-ad7a-e3db1cb17685",
            "groupId": "80404c81-017d-1000-e8e8-af7420af06c1",
            "componentId": "aed63edf-e660-3f29-b56b-192cf6286889",
            "componentType": "PutS3Object",
            "componentName": "PutS3Object",
            "attributes": [
                {
                    "name": "filename",
                    "value": "test_rare.json",
                    "previousValue": "test_rare.json",
                },
                {"name": "path", "value": "./", "previousValue": "./"},
                {
                    "name": "s3.bucket",
                    "value": "foo-nifi",
                    "previousValue": "enriched-topical-chat",
                },
                {"name": "s3.key", "value": "tropical_data/test_rare.json"},
            ],
            "transitUri": "https://foo-nifi.s3.amazonaws.com/tropical_data/test_rare.json",
        },
        {
            "id": "46",
            "eventId": 46,
            "eventTime": "12/08/2021 10:06:30.560 UTC",
            "eventType": "SEND",
            "flowFileUuid": "e8a6ad9a-1d02-4bf1-b104-dad1ff180ddd",
            "groupId": "80404c81-017d-1000-e8e8-af7420af06c1",
            "componentId": "aed63edf-e660-3f29-b56b-192cf6286889",
            "componentType": "PutS3Object",
            "componentName": "PutS3Object",
            "attributes": [
                {
                    "name": "filename",
                    "value": "test_freq.json",
                    "previousValue": "test_freq.json",
                },
                {"name": "path", "value": "./", "previousValue": "./"},
                {
                    "name": "s3.bucket",
                    "value": "foo-nifi",
                    "previousValue": "enriched-topical-chat",
                },
                {"name": "s3.key", "value": "tropical_data/test_freq.json"},
            ],
            "transitUri": "https://foo-nifi.s3.amazonaws.com/tropical_data/test_freq.json",
        },
    ]

    fetchs3_provenance_response = [
        {
            "id": "44",
            "eventId": 44,
            "eventTime": "12/08/2021 10:06:19.828 UTC",
            "eventType": "FETCH",
            "groupId": "80404c81-017d-1000-e8e8-af7420af06c1",
            "componentId": "91d59f03-1c2b-3f3f-48bc-f89296a328bd",
            "componentType": "FetchS3Object",
            "componentName": "FetchS3Object",
            "attributes": [
                {
                    "name": "filename",
                    "value": "test_rare.json",
                    "previousValue": "test_rare.json",
                },
                {"name": "path", "value": "./", "previousValue": "./"},
                {
                    "name": "s3.bucket",
                    "value": "enriched-topical-chat",
                    "previousValue": "enriched-topical-chat",
                },
            ],
            "transitUri": "http://enriched-topical-chat.amazonaws.com/test_rare.json",
        },
        {
            "id": "42",
            "eventId": 42,
            "eventTime": "12/08/2021 10:06:16.952 UTC",
            "eventType": "FETCH",
            "flowFileUuid": "e8a6ad9a-1d02-4bf1-b104-dad1ff180ddd",
            "groupId": "80404c81-017d-1000-e8e8-af7420af06c1",
            "componentId": "91d59f03-1c2b-3f3f-48bc-f89296a328bd",
            "componentType": "FetchS3Object",
            "componentName": "FetchS3Object",
            "attributes": [
                {
                    "name": "filename",
                    "value": "test_freq.json",
                    "previousValue": "test_freq.json",
                },
                {"name": "path", "value": "./", "previousValue": "./"},
                {
                    "name": "s3.bucket",
                    "value": "enriched-topical-chat",
                    "previousValue": "enriched-topical-chat",
                },
            ],
            "transitUri": "http://enriched-topical-chat.amazonaws.com/test_freq.json",
        },
    ]
    mock_delete_provenance.return_value = None
    if provenance_case == "fetchs3":
        mock_provenance_events.return_value = fetchs3_provenance_response
    else:
        mock_provenance_events.return_value = puts3_provenance_response
