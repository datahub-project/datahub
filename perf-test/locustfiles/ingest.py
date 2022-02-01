import json
import random

from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    BrowsePaths,
    Owner,
    Ownership,
    OwnershipType,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from locust import HttpUser, constant, task


class IngestUser(HttpUser):
    wait_time = constant(1)

    @task
    def ingest(self):
        proposed_snapshot = self._build_snapshot(random.randint(1, 100000))
        snapshot_fqn = (
            f"com.linkedin.metadata.snapshot.{proposed_snapshot.RECORD_SCHEMA.name}"
        )
        self.client.post(
            "/entities?action=ingest",
            json.dumps(
                {
                    "entity": {
                        "value": {
                            snapshot_fqn: pre_json_transform(proposed_snapshot.to_obj())
                        }
                    }
                }
            ),
        )

    def _build_snapshot(self, id: int):
        urn = self._build_urn(id)
        return DatasetSnapshot(
            urn,
            [
                self._build_properties(),
                self._build_ownership(id),
                self._build_browsepaths(id),
            ],
        )

    def _build_urn(self, id: int):
        return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,test_dataset_{id},PROD)"

    def _build_properties(self):
        return DatasetProperties(description="This is a great dataset")

    def _build_browsepaths(self, id: int):
        return BrowsePaths([f"/perf/testing/path/{id}"])

    def _build_ownership(self, id: int):
        return Ownership(
            [
                Owner(f"urn:li:corpuser:test_{id}", OwnershipType.DATAOWNER),
                Owner(f"urn:li:corpuser:common", OwnershipType.DATAOWNER),
            ]
        )
