import json
import random

from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    BrowsePaths,
    Owner,
    Ownership,
    OwnershipType,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    Upstream,
    UpstreamLineage
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from locust import HttpUser, constant, task
from threading import Lock, Thread

lock = Lock()
num_ingested = 0

class IngestUser(HttpUser):
    wait_time = constant(1)
    num_children = 1
    total = 100000
    platforms = ["snowflake", "bigquery", "redshift"]
    prefix = f"breadth{num_children}"

    @task
    def config(self):
        self.client.get("/config")

    @task
    def ingest(self):
        global num_ingested
        if num_ingested >= self.total:
            return
        lock.acquire()
        id = num_ingested
        num_ingested += 1
        lock.release()
        proposed_snapshot = self._build_snapshot(id)
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
                self._build_upstream(id),
                self._build_browsepaths(id),
            ],
        )

    def _build_urn(self, id: int):
        return f"urn:li:dataset:(urn:li:dataPlatform:{self.platforms[id % len(self.platforms)]},{self.prefix}_{id},PROD)"

    def _build_properties(self):
        return DatasetProperties(description="This is a great dataset")

    def _build_browsepaths(self, id: int):
        return BrowsePaths([f"/perf/{self.prefix}/path/{id}/group"])

    def _build_upstream(self, id: int):
        if id == 0:
            return UpstreamLineage([])
        parent_id = (id-1)//self.num_children
        return UpstreamLineage(
            [
                Upstream(
                    f"urn:li:dataset:(urn:li:dataPlatform:{self.platforms[parent_id % len(self.platforms)]},{self.prefix}_{parent_id},PROD)", 
                    DatasetLineageType.TRANSFORMED
                )
            ]
        )
