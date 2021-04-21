import json

import pytest
import requests

import datahub.metadata as models
from datahub.emitter.rest_emitter import DatahubRestEmitter

MOCK_GMS_ENDPOINT = "http://fakegmshost:8080"

basicAuditStamp = models.AuditStampClass(
    time=1618987484580,
    actor="urn:li:corpuser:datahub",
    impersonator=None,
)


@pytest.mark.parametrize(
    "mce,endpoint,snapshot",
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
            "datasets",
            json.loads(
                '{"snapshot": {"urn": "urn:li:dataset:(urn:li:dataPlatform:bigquery,downstream,PROD)", "aspects": [{"com.linkedin.dataset.UpstreamLineage": {"upstreams": [{"auditStamp": {"time": 1618987484580, "actor": "urn:li:corpuser:datahub"}, "dataset": "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream1,PROD)", "type": "TRANSFORMED"}, {"auditStamp": {"time": 1618987484580, "actor": "urn:li:corpuser:datahub"}, "dataset": "urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream2,PROD)", "type": "TRANSFORMED"}]}}]}}'  # noqa: E501
            ),
        ),
        (
            # Verify the behavior of the fieldDiscriminator for primitive enums.
            models.MetadataChangeEventClass(
                proposedSnapshot=models.MLModelSnapshotClass(
                    urn="urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)",
                    aspects=[
                        models.CostClass(
                            costType=models.CostTypeClass.ORG_COST_TYPE,
                            cost=models.CostCostClass(
                                fieldDiscriminator=models.CostCostDiscriminatorClass.costCode,
                                costCode="sampleCostCode",
                            ),
                        )
                    ],
                )
            ),
            "mlModels",
            {
                "snapshot": {
                    "aspects": [
                        {
                            "com.linkedin.common.Cost": {
                                "cost": {"costCode": "sampleCostCode"},
                                "costType": "ORG_COST_TYPE",
                            }
                        }
                    ],
                    "urn": "urn:li:mlModel:(urn:li:dataPlatform:science,scienceModel,PROD)",
                }
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
            "charts",
            json.loads(
                '{"snapshot": {"urn": "urn:li:chart:(superset,227)", "aspects": [{"com.linkedin.chart.ChartInfo": {"title": "Weekly Messages", "description": "", "lastModified": {"created": {"time": 1618987484580, "actor": "urn:li:corpuser:datahub"}, "lastModified": {"time": 1618987484580, "actor": "urn:li:corpuser:datahub"}}, "type": "SCATTER"}}]}}'  # noqa: E501
            ),
        ),
    ],
)
def test_datahub_rest_emitter(requests_mock, mce, endpoint, snapshot):
    def match_request_text(request: requests.Request) -> bool:
        requested_snapshot = request.json()
        assert (
            requested_snapshot == snapshot
        ), f"Expected snapshot to be {json.dumps(snapshot)}, got {json.dumps(requested_snapshot)}"
        return True

    requests_mock.post(
        f"{MOCK_GMS_ENDPOINT}/{endpoint}?action=ingest",
        request_headers={"X-RestLi-Protocol-Version": "2.0.0"},
        additional_matcher=match_request_text,
    )

    emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT)
    emitter.emit_mce(mce)
