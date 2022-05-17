import json
import random

from datahub.emitter.serialization_helper import pre_json_transform
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    BrowsePaths,
    GlossaryTerms,
    GlossaryTermAssociation
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    Upstream,
    UpstreamLineage
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaMetadata,
    SchemaField,
    SchemaFieldDataType,
    FixedType,
    EditableSchemaMetadata,
    EditableSchemaFieldInfo,
    KeyValueSchema
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from locust import HttpUser, constant, task
from threading import Lock, Thread

lock = Lock()
num_ingested = 0


class IngestUser(HttpUser):
    wait_time = constant(1)
    num_children = 1
    total = 3000
    # platforms = ["snowflake", "bigquery", "redshift"]
    platforms = ["postgres"]
    # prefix = f"breadth{num_children}"
    prefix = "propogation_source"

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
                            snapshot_fqn: pre_json_transform(
                                proposed_snapshot.to_obj())
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
                # self._build_upstream(id),
                self._build_browsepaths(id),
                self._build_schema_metadata(id),
                self._build_editable_schema_metadata(id),
            ],
        )

    def _build_urn(self, id: int):
        return f"urn:li:dataset:(urn:li:dataPlatform:{self.platforms[0]},{self.prefix}_{id},PROD)"

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

    def _build_schema_metadata(self, id: int):
        schema = [SchemaField(f"dataset_{id}_field_{i}", SchemaFieldDataType(
            FixedType()), "int") for i in range(1, 10)]
        return SchemaMetadata(
            f"schema_{id}",
            f"urn:li:dataPlatform:{self.platforms[0]}",
            0,
            "random",
            KeyValueSchema("key", "value"),
            schema
        )

    def _build_editable_schema_metadata(self, id: int):
        schema = [EditableSchemaFieldInfo(f"dataset_{id}_field_{i}", glossaryTerms=GlossaryTerms([GlossaryTermAssociation(
            f"urn:li:glossaryTerm:term{i}")], AuditStamp(0, "urn:li:corpuser:datahub"))) for i in range(1, 10)]
        return EditableSchemaMetadata(schema)
