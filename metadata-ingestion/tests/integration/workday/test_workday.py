from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.workday.client import WorkdayClient
from datahub.ingestion.source.workday.models import (
    PrismDataset,
    PrismDataSource,
    PrismTable,
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


def _tables(_client: WorkdayClient) -> List[PrismTable]:
    return [
        PrismTable.model_validate(
            {
                "id": "tbl-1",
                "name": "Worker_Headcount",
                "description": "Headcount by organization",
                "sourceType": "Workday",
                "createdBy": "hr_analyst",
                "datasetId": "dset-1",
                "dataSources": ["ds-1", "ds-2"],
                "fields": [
                    {"name": "organization_id", "type": "text", "isPrimaryKey": True},
                    {"name": "headcount", "type": "numeric"},
                    {"name": "as_of_date", "type": "date"},
                ],
            }
        )
    ]


def _datasets(_client: WorkdayClient) -> List[PrismDataset]:
    return [
        PrismDataset.model_validate(
            {
                "id": "dset-1",
                "name": "Headcount Pipeline",
                "sourceType": "Workday",
                "outputTableId": "tbl-1",
                "dataSources": ["ds-1"],
            }
        )
    ]


def _data_sources(_client: WorkdayClient) -> List[PrismDataSource]:
    return [
        PrismDataSource.model_validate(
            {"id": "ds-1", "name": "Headcount Report", "sourceType": "Workday"}
        ),
        PrismDataSource.model_validate(
            {"id": "ds-2", "name": "External Snowflake", "sourceType": "External"}
        ),
    ]


def test_workday_ingestion(pytestconfig: Any, tmp_path: Path) -> None:
    output_path = tmp_path / "workday_mcps.json"
    golden_path = (
        pytestconfig.rootpath / "tests/integration/workday/workday_mcps_golden.json"
    )
    with (
        patch.object(WorkdayClient, "list_tables", _tables),
        patch.object(WorkdayClient, "list_datasets", _datasets),
        patch.object(WorkdayClient, "list_data_sources", _data_sources),
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
