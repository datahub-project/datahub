"""Shared fixtures and builders for the BigQuery unit tests.

Raw audit-log builders produce the exact shapes GCP emits (legacy AuditData
entries, BigQueryAuditMetadata entries and exported audit rows) so the parser,
lineage and usage tests all exercise the same inputs. The source/table/config
factories patch only the google client constructors; everything the connector
does with their results runs for real.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, Optional, Sequence
from unittest.mock import NonCallableMock, create_autospec, patch

import pytest
from google.cloud.bigquery.table import Row

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.bigquery_v2.bigquery import BigqueryV2Source
from datahub.ingestion.source.bigquery_v2.bigquery_audit import (
    BigqueryTableIdentifier,
    BigQueryTableRef,
)
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.bigquery_platform_resource_helper import (
    BigQueryPlatformResourceHelper,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.bigquery_schema import (
    BigQuerySchemaApi,
    BigqueryTable,
)
from datahub.ingestion.source.bigquery_v2.bigquery_schema_gen import (
    BigQuerySchemaGenerator,
)
from datahub.ingestion.source.bigquery_v2.common import BigQueryIdentifierBuilder

PROJECT = "my-project"
DATASET = "my_dataset"
ACTOR = "user@example.com"
TS = datetime(2023, 1, 2, 12, 0, tzinfo=timezone.utc)
START_TIME = "2023-01-02T12:00:00Z"
END_TIME = "2023-01-02T12:00:01Z"

PROFILING_PROJECT = "test-project-123456"


def as_mock(obj: object) -> NonCallableMock:
    """Narrow a collaborator the test replaced with a mock, for mypy. Accepts
    the NonCallableMagicMock that create_autospec(..., instance=True) returns."""
    assert isinstance(obj, NonCallableMock)
    return obj


# ---------------------------------------------------------------------------
# Process-wide state isolation
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_process_wide_bigquery_state() -> Iterator[None]:
    """Two pieces of connector state live on classes rather than instances, so
    any test that builds a source or a BigQueryIdentifierBuilder would otherwise
    leak into whichever test runs next."""
    previous_suffix = BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX
    BigQueryPlatformResourceHelper.platform_resource_cache.clear()
    yield
    BigqueryTableIdentifier._BQ_SHARDED_TABLE_SUFFIX = previous_suffix
    BigQueryPlatformResourceHelper.platform_resource_cache.clear()


@pytest.fixture
def shard_suffix() -> str:
    """Pin the sharded-table suffix to its non-legacy value through the public
    config knob (the autouse fixture restores whatever was there before)."""
    BigQueryIdentifierBuilder(
        BigQueryV2Config(enable_legacy_sharded_table_support=False),
        BigQueryV2Report(),
    )
    return BigqueryTableIdentifier.DEFAULT_BQ_SHARDED_TABLE_SUFFIX


# ---------------------------------------------------------------------------
# Raw audit-log builders
# ---------------------------------------------------------------------------


@dataclass
class FakeLogEntry:
    """The subset of a google.cloud.logging LogEntry the parsers read."""

    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = TS


def table_ref(table: str, project: str = PROJECT, dataset: str = DATASET) -> str:
    return f"projects/{project}/datasets/{dataset}/tables/{table}"


def bq_ref(table: str) -> BigQueryTableRef:
    return BigQueryTableRef.from_string_name(table_ref(table))


def urn_for(table: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{PROJECT}.{DATASET}.{table},PROD)"


def _table_spec(table: str) -> Dict[str, str]:
    return {"projectId": PROJECT, "datasetId": DATASET, "tableId": table}


def v1_query_entry(
    query: str = "SELECT 1",
    statement_type: str = "SELECT",
    destination: Optional[str] = None,
    referenced_tables: Sequence[str] = (),
    referenced_views: Sequence[str] = (),
    job_id: str = "job_1",
    project: str = PROJECT,
) -> FakeLogEntry:
    """A legacy AuditData (serviceData.jobCompletedEvent) entry."""
    query_conf: Dict[str, Any] = {
        "query": query,
        "statementType": statement_type,
        "defaultDataset": {},
    }
    if destination:
        query_conf["destinationTable"] = _table_spec(destination)
    return FakeLogEntry(
        payload={
            "authenticationInfo": {"principalEmail": ACTOR},
            "serviceData": {
                "jobCompletedEvent": {
                    "job": {
                        "jobName": {"projectId": project, "jobId": job_id},
                        "jobConfiguration": {"query": query_conf},
                        "jobStatistics": {
                            "startTime": START_TIME,
                            "endTime": END_TIME,
                            "totalBilledBytes": "2048",
                            "queryOutputRowCount": "7",
                            "referencedTables": [
                                _table_spec(t) for t in referenced_tables
                            ],
                            "referencedViews": [
                                _table_spec(v) for v in referenced_views
                            ],
                        },
                    }
                }
            },
        }
    )


def v2_job_metadata(
    query: str = "SELECT 1",
    statement_type: str = "SELECT",
    destination: Optional[str] = None,
    referenced_tables: Sequence[str] = (),
    referenced_views: Sequence[str] = (),
    job_id: str = "job_1",
    project: str = PROJECT,
) -> Dict[str, Any]:
    """The BigQueryAuditMetadata `jobChange` payload shared by v2 log entries
    and exported audit rows."""
    return {
        "jobChange": {
            "job": {
                "jobName": f"projects/{project}/jobs/{job_id}",
                "jobConfig": {
                    "queryConfig": {
                        "query": query,
                        "statementType": statement_type,
                        "destinationTable": table_ref(destination)
                        if destination
                        else None,
                    }
                },
                "jobStats": {
                    "startTime": START_TIME,
                    "endTime": END_TIME,
                    "queryStats": {
                        "referencedTables": [table_ref(t) for t in referenced_tables],
                        "referencedViews": [table_ref(v) for v in referenced_views],
                        "outputRowCount": "7",
                        "totalBilledBytes": "2048",
                    },
                },
            }
        }
    }


def v2_query_entry(**kwargs: Any) -> FakeLogEntry:
    return FakeLogEntry(
        payload={
            "authenticationInfo": {"principalEmail": ACTOR},
            "metadata": v2_job_metadata(**kwargs),
        }
    )


def read_metadata(
    fields: Sequence[str] = ("id",),
    reason: str = "JOB",
    job_name: Optional[str] = f"projects/{PROJECT}/jobs/job_1",
) -> Dict[str, Any]:
    read_info: Dict[str, Any] = {"fields": list(fields), "reason": reason}
    if job_name:
        read_info["jobName"] = job_name
    return {"tableDataRead": read_info}


def read_entry(
    table: str,
    fields: Sequence[str] = ("id",),
    reason: str = "JOB",
    dataset: str = DATASET,
    timestamp: datetime = TS,
) -> FakeLogEntry:
    return FakeLogEntry(
        payload={
            "authenticationInfo": {"principalEmail": ACTOR},
            "resourceName": table_ref(table, dataset=dataset),
            "metadata": read_metadata(fields=fields, reason=reason),
        },
        timestamp=timestamp,
    )


def exported_row(metadata: Dict[str, Any], resource_name: Optional[str] = None) -> Row:
    """A row as returned by the exported-audit-log query templates."""
    proto_payload: Dict[str, Any] = {"authenticationInfo": {"principalEmail": ACTOR}}
    if resource_name:
        proto_payload["resourceName"] = resource_name
    return Row(
        (
            TS,
            "projects/p/logs/data_access",
            "insert-1",
            proto_payload,
            json.dumps(metadata),
        ),
        {"timestamp": 0, "logName": 1, "insertId": 2, "protoPayload": 3, "metadata": 4},
    )


# ---------------------------------------------------------------------------
# Config / table / source factories
# ---------------------------------------------------------------------------


def make_profiling_config(**profiling_overrides: Any) -> BigQueryV2Config:
    return BigQueryV2Config.model_validate(
        {
            "project_id": PROFILING_PROJECT,
            "profiling": {"enabled": True, **profiling_overrides},
        }
    )


def make_table(
    name: str = "test_table",
    rows_count: Optional[int] = 10000,
    external: bool = False,
    max_partition_id: Optional[str] = None,
    **kwargs: Any,
) -> BigqueryTable:
    now = datetime.now(timezone.utc)
    return BigqueryTable(
        name=name,
        comment=kwargs.pop("comment", None),
        rows_count=rows_count,
        size_in_bytes=kwargs.pop("size_in_bytes", 1_000_000 if rows_count else None),
        last_altered=kwargs.pop("last_altered", now - timedelta(days=1)),
        created=kwargs.pop("created", now - timedelta(days=30)),
        external=external,
        max_partition_id=max_partition_id,
        **kwargs,
    )


def make_source(
    ctx: Optional[PipelineContext] = None,
    project_id: str = PROJECT,
    **config_overrides: Any,
) -> BigqueryV2Source:
    """A real BigqueryV2Source whose google clients are never constructed."""
    with (
        patch.object(BigQueryV2Config, "get_bigquery_client"),
        patch.object(BigQueryV2Config, "get_projects_client"),
    ):
        config = BigQueryV2Config.model_validate(
            {"project_id": project_id, **config_overrides}
        )
        return BigqueryV2Source(
            config=config, ctx=ctx or PipelineContext(run_id="test")
        )


def make_schema_gen(
    project_id: str = PROJECT, **config_overrides: Any
) -> BigQuerySchemaGenerator:
    """The schema generator of a real source, with its BigQuerySchemaApi replaced
    by an autospecced mock so each test states exactly what the API returns."""
    schema_gen = make_source(
        project_id=project_id, **config_overrides
    ).bq_schema_extractor
    schema_gen.schema_api = create_autospec(BigQuerySchemaApi, instance=True)
    return schema_gen
