from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.workday.client import WorkdayClient
from datahub.ingestion.source.workday.models import (
    CustomReport,
    PrismBucket,
    PrismDataset,
    PrismDataSource,
    PrismField,
    PrismTable,
    WqlDataSource,
)
from datahub.testing import mce_helpers


def _pipeline_config(output_path: Path) -> Dict[str, Any]:
    return {
        "run_id": "workday-source-test",
        "source": {
            "type": "workday",
            "config": {
                "base_url": "https://wd2-impl-services1.workday.com",
                "tenant": "acme",
                "client_id": "client",
                "client_secret": "secret",
                "platform_instance": "prod",
                "env": "PROD",
                "extract_business_objects": True,
                "extract_custom_reports": True,
                "extract_buckets": True,
                "domain": {"HR": {"allow": [".*Headcount.*", "Worker"]}},
                "data_source_platform_mapping": {
                    "External Snowflake": {
                        "platform": "snowflake",
                        "dataset_name": "db.schema.headcount",
                    }
                },
                "stateful_ingestion": {"enabled": False},
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_path)}},
    }


# The list endpoints return summaries only; hydration (get_table/get_dataset)
# fills in schema fields, relationships, timestamps, row counts, DPL, and the
# per-field mappings that drive column-level lineage.
def _tables(_client: WorkdayClient) -> List[PrismTable]:
    return [
        PrismTable.model_validate(
            {"id": "tbl-1", "name": "Worker_Headcount", "sourceType": "Workday"}
        )
    ]


def _get_table(_client: WorkdayClient, table_id: str) -> Optional[PrismTable]:
    if table_id != "tbl-1":
        return None
    return PrismTable.model_validate(
        {
            "id": "tbl-1",
            "name": "Worker_Headcount",
            "description": "Headcount by organization",
            "sourceType": "Workday",
            "createdBy": "hr_analyst",
            "ownerGroup": "HR Data Stewards",
            "created": "2024-01-01T00:00:00Z",
            "lastRefreshed": "2024-06-01T12:30:00Z",
            "rows": 1500,
            "tags": ["Certified"],
            "glossaryTerms": [{"descriptor": "Headcount"}],
            "datasetId": "dset-1",
            "dataSources": ["ds-1", "ds-2"],
            "fields": [
                {
                    "name": "organization_id",
                    "type": "text",
                    "isPrimaryKey": True,
                    "length": 64,
                },
                {
                    "name": "headcount",
                    "type": "numeric",
                    "precision": 10,
                    "scale": 0,
                    "tags": ["PII"],
                },
                {"name": "as_of_date", "type": "date", "nullable": True},
            ],
        }
    )


def _datasets(_client: WorkdayClient) -> List[PrismDataset]:
    return [
        PrismDataset.model_validate(
            {"id": "dset-1", "name": "Headcount Pipeline", "sourceType": "Workday"}
        )
    ]


def _get_dataset(_client: WorkdayClient, dataset_id: str) -> Optional[PrismDataset]:
    if dataset_id != "dset-1":
        return None
    return PrismDataset.model_validate(
        {
            "id": "dset-1",
            "name": "Headcount Pipeline",
            "sourceType": "Workday",
            "created": "2023-12-01T00:00:00Z",
            "outputTableId": "tbl-1",
            "dataSources": ["ds-1"],
            "dpl": "FORMAT headcount AS integer\nGROUP BY organization_id",
            "fieldMappings": [
                {
                    "outputField": "headcount",
                    "sourceField": "worker_count",
                    "sourceObjectId": "ds-1",
                },
                {
                    "outputField": "organization_id",
                    "sourceField": "org",
                    "sourceObjectId": "ds-1",
                },
            ],
        }
    )


def _data_sources(_client: WorkdayClient) -> List[PrismDataSource]:
    return [
        PrismDataSource.model_validate(
            {"id": "ds-1", "name": "Headcount Report", "sourceType": "Workday"}
        ),
        PrismDataSource.model_validate(
            {"id": "ds-2", "name": "External Snowflake", "sourceType": "External"}
        ),
    ]


def _business_objects(_client: WorkdayClient) -> List[WqlDataSource]:
    return [
        WqlDataSource.model_validate(
            {
                "id": "bo-1",
                "descriptor": "Worker",
                "alias": "worker",
                "subjectArea": "Human Capital Management",
                "relatedBusinessObjects": [{"id": "bo-2"}],
                "fields": [
                    {"name": "worker_count", "type": "numeric"},
                    {"name": "org", "type": "text"},
                ],
            }
        ),
        WqlDataSource.model_validate(
            {
                "id": "bo-2",
                "descriptor": "Organization",
                "alias": "organization",
                "subjectArea": "Human Capital Management",
                "fields": [{"name": "org", "type": "text"}],
            }
        ),
    ]


def _get_business_object_fields(
    _client: WorkdayClient, data_source_id: str
) -> List[PrismField]:
    return []


def _custom_reports(_client: WorkdayClient) -> List[CustomReport]:
    return [
        CustomReport.model_validate(
            {
                "id": "rpt-1",
                "name": "Headcount Custom Report",
                "dataSource": "Worker",
                "subjectArea": "Human Capital Management",
                "enabledAsWebService": True,
                "outputFields": [
                    {"name": "org", "type": "text"},
                    {"name": "computed_ratio", "type": "numeric"},
                ],
            }
        )
    ]


def _buckets(_client: WorkdayClient) -> List[PrismBucket]:
    return [
        PrismBucket.model_validate(
            {
                "id": "bkt-1",
                "name": "Worker_Headcount Upload",
                "targetTableId": "tbl-1",
                "state": "Success",
            }
        )
    ]


@time_machine.travel("2024-07-01 00:00:00", tick=False)
def test_workday_ingestion(pytestconfig: Any, tmp_path: Path) -> None:
    output_path = tmp_path / "workday_mcps.json"
    golden_path = (
        pytestconfig.rootpath / "tests/integration/workday/workday_mcps_golden.json"
    )
    with (
        patch.object(WorkdayClient, "list_tables", _tables),
        patch.object(WorkdayClient, "get_table", _get_table),
        patch.object(WorkdayClient, "list_datasets", _datasets),
        patch.object(WorkdayClient, "get_dataset", _get_dataset),
        patch.object(WorkdayClient, "list_data_sources", _data_sources),
        patch.object(WorkdayClient, "list_buckets", _buckets),
        patch.object(WorkdayClient, "list_business_objects", _business_objects),
        patch.object(
            WorkdayClient, "get_business_object_fields", _get_business_object_fields
        ),
        patch.object(WorkdayClient, "list_custom_reports", _custom_reports),
        patch.object(WorkdayClient, "close", lambda self: None),
    ):
        pipeline = Pipeline.create(_pipeline_config(output_path))
        pipeline.run()
        pipeline.raise_from_status()

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(output_path),
        golden_path=str(golden_path),
    )
