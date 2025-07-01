import contextlib
import json
from datetime import datetime, timezone

import pytest
import requests
from freezegun import freeze_time

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

MOCK_GMS_ENDPOINT = "http://fakegmshost:8080"

FROZEN_TIME = 1618987484580
basicAuditStamp = models.AuditStampClass(
    time=1618987484580,
    actor="urn:li:corpuser:datahub",
    impersonator=None,
)


@pytest.mark.parametrize(
    "record,path,snapshot",
    [
        (
            # Simple test.
            models.MetadataChangeEventClass(
                proposedSnapshot=models.DatasetSnapshotClass(
                    urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,downstream,PROD)",
                    aspects=[
                        models.UpstreamLineageClass(
                            upstreams=[
                                models.UpstreamClass(
                                    auditStamp=basicAuditStamp,
                                    dataset="urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream1,PROD)",
                                    type="TRANSFORMED",
                                ),
                                models.UpstreamClass(
                                    auditStamp=basicAuditStamp,
                                    dataset="urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream2,PROD)",
                                    type="TRANSFORMED",
                                ),
                            ]
                        )
                    ],
                ),
            ),
            "/entities?action=ingest",
            {
                "entity": {
                    "value": {
                        "com.linkedin.metadata.snapshot.DatasetSnapshot": {
                            "urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,downstream,PROD)",
                            "aspects": [
                                {
                                    "com.linkedin.dataset.UpstreamLineage": {
                                        "upstreams": [
                                            {
                                                "auditStamp": {
                                                    "time": 1618987484580,
                                                    "actor": "urn:li:corpuser:datahub",
                                                },
                                                "dataset": "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream1,PROD)",
                                                "type": "TRANSFORMED",
                                            },
                                            {
                                                "auditStamp": {
                                                    "time": 1618987484580,
                                                    "actor": "urn:li:corpuser:datahub",
                                                },
                                                "dataset": "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream2,PROD)",
                                                "type": "TRANSFORMED",
                                            },
                                        ]
                                    }
                                }
                            ],
                        }
                    }
                },
                "systemMetadata": {
                    "lastObserved": FROZEN_TIME,
                    "lastRunId": "no-run-id-provided",
                    "properties": {
                        "clientId": "acryl-datahub",
                        "clientVersion": "1!0.0.0.dev0",
                    },
                    "runId": "no-run-id-provided",
                },
            },
        ),
        (
            # Verify the serialization behavior with chart type enums.
            models.MetadataChangeEventClass(
                proposedSnapshot=models.ChartSnapshotClass(
                    urn="urn:li:chart:(superset,227)",
                    aspects=[
                        models.ChartInfoClass(
                            title="Weekly Messages",
                            description="",
                            lastModified=models.ChangeAuditStampsClass(
                                created=basicAuditStamp,
                                lastModified=basicAuditStamp,
                            ),
                            type=models.ChartTypeClass.SCATTER,
                        ),
                    ],
                )
            ),
            "/entities?action=ingest",
            {
                "entity": {
                    "value": {
                        "com.linkedin.metadata.snapshot.ChartSnapshot": {
                            "urn": "urn:li:chart:(superset,227)",
                            "aspects": [
                                {
                                    "com.linkedin.chart.ChartInfo": {
                                        "customProperties": {},
                                        "title": "Weekly Messages",
                                        "description": "",
                                        "lastModified": {
                                            "created": {
                                                "time": 1618987484580,
                                                "actor": "urn:li:corpuser:datahub",
                                            },
                                            "lastModified": {
                                                "time": 1618987484580,
                                                "actor": "urn:li:corpuser:datahub",
                                            },
                                        },
                                        "type": "SCATTER",
                                    }
                                }
                            ],
                        }
                    }
                },
                "systemMetadata": {
                    "lastObserved": FROZEN_TIME,
                    "lastRunId": "no-run-id-provided",
                    "properties": {
                        "clientId": "acryl-datahub",
                        "clientVersion": "1!0.0.0.dev0",
                    },
                    "runId": "no-run-id-provided",
                },
            },
        ),
        (
            # Verify that DataJobInfo is serialized properly (particularly it's union type).
            models.MetadataChangeEventClass(
                proposedSnapshot=models.DataJobSnapshotClass(
                    urn="urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
                    aspects=[
                        models.DataJobInfoClass(
                            name="User Deletions",
                            description="Constructs the fct_users_deleted from logging_events",
                            type=models.AzkabanJobTypeClass.SQL,
                        )
                    ],
                )
            ),
            "/entities?action=ingest",
            {
                "entity": {
                    "value": {
                        "com.linkedin.metadata.snapshot.DataJobSnapshot": {
                            "urn": "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)",
                            "aspects": [
                                {
                                    "com.linkedin.datajob.DataJobInfo": {
                                        "customProperties": {},
                                        "name": "User Deletions",
                                        "description": "Constructs the fct_users_deleted from logging_events",
                                        "type": {"string": "SQL"},
                                    }
                                }
                            ],
                        }
                    }
                },
                "systemMetadata": {
                    "lastObserved": FROZEN_TIME,
                    "lastRunId": "no-run-id-provided",
                    "properties": {
                        "clientId": "acryl-datahub",
                        "clientVersion": "1!0.0.0.dev0",
                    },
                    "runId": "no-run-id-provided",
                },
            },
        ),
        (
            # Usage stats ingestion test.
            models.UsageAggregationClass(
                bucket=1623826800000,
                duration="DAY",
                resource="urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
                metrics=models.UsageAggregationMetricsClass(
                    uniqueUserCount=2,
                    users=[
                        models.UserUsageCountsClass(
                            user="urn:li:corpuser:jdoe",
                            count=5,
                        ),
                        models.UserUsageCountsClass(
                            user="urn:li:corpuser:unknown",
                            count=3,
                            userEmail="foo@example.com",
                        ),
                    ],
                    totalSqlQueries=1,
                    topSqlQueries=["SELECT * FROM foo"],
                ),
            ),
            "/usageStats?action=batchIngest",
            {
                "buckets": [
                    {
                        "bucket": 1623826800000,
                        "duration": "DAY",
                        "resource": "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)",
                        "metrics": {
                            "uniqueUserCount": 2,
                            "users": [
                                {"count": 5, "user": "urn:li:corpuser:jdoe"},
                                {
                                    "count": 3,
                                    "user": "urn:li:corpuser:unknown",
                                    "userEmail": "foo@example.com",
                                },
                            ],
                            "totalSqlQueries": 1,
                            "topSqlQueries": ["SELECT * FROM foo"],
                        },
                    }
                ]
            },
        ),
        (
            MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
                aspect=models.OwnershipClass(
                    owners=[
                        models.OwnerClass(
                            owner="urn:li:corpuser:fbar",
                            type=models.OwnershipTypeClass.DATAOWNER,
                        )
                    ],
                    lastModified=models.AuditStampClass(
                        time=0,
                        actor="urn:li:corpuser:fbar",
                    ),
                ),
            ),
            "/aspects?action=ingestProposal",
            {
                "async": "false",
                "proposal": {
                    "entityType": "dataset",
                    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
                    "changeType": "UPSERT",
                    "aspectName": "ownership",
                    "aspect": {
                        "value": '{"owners": [{"owner": "urn:li:corpuser:fbar", "type": "DATAOWNER"}], "ownerTypes": {}, "lastModified": {"time": 0, "actor": "urn:li:corpuser:fbar"}}',
                        "contentType": "application/json",
                    },
                    "systemMetadata": {
                        "lastObserved": FROZEN_TIME,
                        "lastRunId": "no-run-id-provided",
                        "properties": {
                            "clientId": "acryl-datahub",
                            "clientVersion": "1!0.0.0.dev0",
                        },
                        "runId": "no-run-id-provided",
                    },
                },
            },
        ),
    ],
)
@freeze_time(datetime.fromtimestamp(FROZEN_TIME / 1000, tz=timezone.utc))
def test_datahub_rest_emitter(requests_mock, record, path, snapshot):
    def match_request_text(request: requests.Request) -> bool:
        requested_snapshot = request.json()
        assert requested_snapshot == snapshot, (
            f"Expected snapshot to be {json.dumps(snapshot)}, got {json.dumps(requested_snapshot)}"
        )
        return True

    requests_mock.post(
        f"{MOCK_GMS_ENDPOINT}{path}",
        request_headers={"X-RestLi-Protocol-Version": "2.0.0"},
        additional_matcher=match_request_text,
    )

    with contextlib.ExitStack() as stack:
        if isinstance(record, models.UsageAggregationClass):
            stack.enter_context(
                pytest.warns(
                    DeprecationWarning,
                    match="Use emit with a datasetUsageStatistics aspect instead",
                )
            )

        # This test specifically exercises the restli emitter endpoints.
        # We have additional tests specifically for the OpenAPI emitter
        # and its request format.
        emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=False)
        stack.enter_context(emitter)
        emitter.emit(record)
