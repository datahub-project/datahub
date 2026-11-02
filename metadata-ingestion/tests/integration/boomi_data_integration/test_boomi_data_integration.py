import json
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests_mock
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.boomi_data_integration.report import (
    BoomiDataIntegrationSourceReport,
)
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2026-10-01 12:00:00"
ACCOUNT_ID = "acct-0001"
BASE_URL = f"https://api.eu-west-1.rivery.io/v1/accounts/{ACCOUNT_ID}"
GOLDEN_DIR = Path(__file__).parent / "golden"

ENV_PROD = "env-prod"
ENV_DEV = "env-dev"
ENV_LOCKED = "env-locked"


def _environment(
    cross_id: str, name: str, is_default: bool = False
) -> Dict[str, object]:
    return {
        "_id": f"oid-{cross_id}",
        "account": ACCOUNT_ID,
        "cross_id": cross_id,
        "environment_name": name,
        "variables": {},
        "is_default": is_default,
        "is_deleted": False,
    }


def _summary(
    river_id: str,
    name: str,
    river_type: str,
    group_id: str,
    group_name: str,
    datasource_id: str,
    is_api_v2: bool = True,
    description: Optional[str] = None,
    schedulers: Optional[List[str]] = None,
) -> Dict[str, object]:
    return {
        "river_cross_id": river_id,
        "name": name,
        "river_type": river_type,
        "river_status": "active",
        "group_id": group_id,
        "group_name": group_name,
        "datasource_id": datasource_id,
        "description": description,
        "river_schedulers": schedulers or [],
        "last_user_name_modified": "Data Engineer",
        "is_api_v2": is_api_v2,
        "created_at": "2026-03-01T09:00:00Z",
        "last_updated_at": "2026-09-15T09:00:00Z",
    }


def _river(
    summary: Dict[str, object],
    properties: Dict[str, object],
    description: str = "",
    schedulers: Optional[List[Dict[str, object]]] = None,
) -> Dict[str, object]:
    return {
        "cross_id": summary["river_cross_id"],
        "account_id": ACCOUNT_ID,
        "kind": "main_river",
        "type": summary["river_type"],
        "name": summary["name"],
        "environment_id": "unused",
        "environment_name": "unused",
        "group_id": summary["group_id"],
        "group_name": summary["group_name"],
        "metadata": {
            "description": description,
            "river_status": "active",
            "created_by": "Data Engineer",
            "last_updated_by": "Data Engineer",
            "created_at": "2026-03-01T09:00:00Z",
            "last_updated_at": "2026-09-20T09:00:00Z",
            "is_deleted": False,
        },
        "properties": properties,
        "schedulers": schedulers or [],
    }


def _table(name: str, selected: bool, target_table: Optional[str] = None) -> Dict:
    return {
        "run_type_and_datasource": "multi_tables",
        "details": {
            "name": name,
            "is_selected": selected,
            "target_table": target_table,
            "extract_method": "incremental",
        },
    }


def _sql_step(
    step_id: str,
    step_type: str,
    target: Dict[str, object],
    connection_id: str = "conn-wh",
) -> Dict[str, object]:
    return {
        "type": step_type,
        "name": f"Step {step_id}",
        "step_id": step_id,
        "is_enabled": True,
        "connection_id": connection_id,
        "sql_query": "SELECT 1",
        "target_settings": target,
    }


# Production: Sales and Finance groups, rivers spread over two pages.
S_ORDERS = _summary(
    "r-orders", "Orders to Snowflake", "source_to_target", "g-sales", "Sales", "mysql"
)
S_EVENTS = _summary(
    "r-events",
    "Events to BigQuery",
    "source_to_target",
    "g-sales",
    "Sales",
    "postgresql",
)
S_UI = _summary(
    "r-ui-made",
    "UI Made Salesforce Load",
    "source_to_target",
    "g-sales",
    "Sales",
    "salesforce",
    is_api_v2=False,
    description="Built in the console.",
    schedulers=["0 6 * * *"],
)
S_LOGIC = _summary(
    "r-nightly", "Nightly Build", "logic", "g-finance", "Finance", "logic"
)
S_ACTIONS = _summary(
    "r-actions", "Call Payments API", "actions", "g-finance", "Finance", "actions"
)
S_PREFIXED = _summary(
    "r-prefixed", "Prefixed Load", "source_to_target", "g-finance", "Finance", "mssql"
)

# Development: one group; one river denied by river_pattern.
S_PG = _summary(
    "r-dev-pg",
    "Leads to Postgres",
    "source_to_target",
    "g-sandbox",
    "Sandbox",
    "hubspot",
)
S_DBX = _summary(
    "r-dev-dbx",
    "Ops to Databricks",
    "source_to_target",
    "g-sandbox",
    "Sandbox",
    "oracle",
)
S_S3 = _summary(
    "r-dev-s3", "Archive to S3", "source_to_target", "g-sandbox", "Sandbox", "mysql"
)
S_TMP = _summary(
    "r-tmp-scratch", "tmp_scratch", "source_to_target", "g-sandbox", "Sandbox", "mysql"
)

RIVERS: Dict[str, Dict[str, object]] = {
    "r-orders": _river(
        S_ORDERS,
        {
            "properties_type": "source_to_target",
            "source": {
                "name": "mysql",
                "connection_id": "conn-mysql",
                "run_type": "multi_tables",
            },
            "target": {
                "name": "snowflake",
                "connection_id": "conn-sf",
                "database_name": "ANALYTICS",
                "schema_name": "RAW",
                "loading_method": "merge",
                "target_prefix": "",
            },
            "schemas": [
                {
                    "name": "shop",
                    "tables": [
                        _table("orders", True),
                        _table("order_items", True, target_table="ORDER_LINES"),
                        _table("audit_log", False),
                    ],
                }
            ],
        },
        description="Copies the shop schema into Snowflake.",
        schedulers=[
            {"cron_expression": "0 */2 * * *", "is_enabled": True},
            {"cron_expression": "0 0 1 * *", "is_enabled": False},
        ],
    ),
    "r-events": _river(
        S_EVENTS,
        {
            "source": {
                "name": "postgresql",
                "connection_id": "conn-pg-app",
                "run_type": "custom_query",
                "custom_query_source_settings": {"query": "SELECT * FROM events"},
            },
            "target": {
                "name": "bigquery",
                "connection_id": "conn-bq",
                "dataset_id": "raw_events",
                "table_name": "app_events",
                "loading_method": "append",
            },
        },
    ),
    "r-nightly": _river(
        S_LOGIC,
        {
            "properties_type": "logic",
            "logic_steps": [
                {
                    "type": "run_once",
                    "name": "Load",
                    "container_id": "c-load",
                    "is_parallel": False,
                    "steps": [
                        {
                            "type": "river",
                            "name": "Run orders river",
                            "step_id": "s-run-orders",
                            "river_id": "r-orders",
                        },
                        _sql_step(
                            "s-build-daily",
                            "snowflake_sql_query",
                            {
                                "target_type": "table",
                                "database_name": "ANALYTICS",
                                "schema_name": "MART",
                                "table_name": "DAILY_ORDERS",
                                "loading_mode": "overwrite",
                            },
                        ),
                    ],
                },
                {
                    "type": "run_once",
                    "name": "Side tasks",
                    "container_id": "c-side",
                    "is_parallel": True,
                    "steps": [
                        {
                            "type": "logicode",
                            "name": "Score customers",
                            "step_id": "s-score",
                            "file_id": "file-1",
                            "logicode_size": "small",
                        },
                        {
                            "type": "redshift_sql_script",
                            "name": "Vacuum",
                            "step_id": "s-vacuum",
                            "connection_id": "conn-rs",
                            "sql_query": "VACUUM",
                        },
                        {
                            "type": "action",
                            "name": "Disabled ping",
                            "step_id": "s-disabled",
                            "action_id": "act-1",
                            "is_enabled": False,
                        },
                    ],
                },
                {
                    "type": "condition",
                    "name": "Weekday?",
                    "container_id": "c-cond",
                    "steps": [
                        {
                            "condition_name": "weekday",
                            "is_else": False,
                            "action": "run",
                            "step": {
                                "type": "action",
                                "name": "Notify",
                                "step_id": "s-notify",
                                "action_id": "act-2",
                            },
                        },
                        {
                            "condition_name": "else",
                            "is_else": True,
                            "action": "run",
                            "step": _sql_step(
                                "s-weekly",
                                "bigquery_sql_query",
                                {
                                    "target_type": "table",
                                    "dataset_id": "reporting",
                                    "table_name": "weekly_summary",
                                },
                                connection_id="conn-bq",
                            ),
                        },
                    ],
                },
                {
                    "type": "loop",
                    "name": "Per region",
                    "container_id": "c-loop",
                    "loop_over_value": "{regions}",
                    "loop_over_variable_name": ["region"],
                    "steps": [
                        _sql_step(
                            "s-region",
                            "databricks_sql_query",
                            {
                                "target_type": "table",
                                "catalog": "main",
                                "schema_name": "gold",
                                "table_name": "region_totals",
                            },
                            connection_id="conn-dbx",
                        ),
                        _sql_step(
                            "s-to-variable",
                            "postgres_sql_query",
                            {
                                "target_type": "variable",
                                "variable_name": "row_count",
                                "variable_type": "integer",
                            },
                        ),
                    ],
                },
            ],
        },
        description="Builds the daily marts.",
    ),
    "r-actions": _river(S_ACTIONS, {"properties_type": "actions"}),
    "r-prefixed": _river(
        S_PREFIXED,
        {
            "source": {
                "name": "mssql",
                "connection_id": "conn-erp",
                "run_type": "multi_tables",
            },
            "target": {
                "name": "redshift",
                "connection_id": "conn-rs",
                "schema_name": "staging",
                "target_prefix": "erp_",
                "loading_method": "merge",
            },
            "schemas": [{"name": "dbo", "tables": [_table("invoices", True)]}],
        },
    ),
    "r-dev-pg": _river(
        S_PG,
        {
            "source": {
                "name": "hubspot",
                "connection_id": "conn-hubspot",
                "run_type": "predefined_report",
            },
            "target": {
                "name": "postgres_rds",
                "connection_id": "conn-pg-dw",
                "schema_name": "crm",
                "table_name": "Leads",
                "loading_method": "overwrite",
            },
        },
    ),
    "r-dev-dbx": _river(
        S_DBX,
        {
            "source": {
                "name": "oracle",
                "connection_id": "conn-ora",
                "run_type": "multi_tables",
            },
            "target": {
                "name": "databricks",
                "connection_id": "conn-dbx",
                "catalog_name": "dev",
                "schema_name": "ops",
                "loading_method": "merge",
            },
            "schemas": [
                {
                    "name": "OPS",
                    "tables": [_table("TICKETS", True), _table("AGENTS", True)],
                }
            ],
        },
    ),
    "r-dev-s3": _river(
        S_S3,
        {
            "source": {"name": "mysql", "connection_id": "conn-mysql"},
            "target": {
                "name": "s3",
                "connection_id": "conn-s3",
                "bucket_name": "archive",
                "path": "mysql/",
                "partitioned_kind": "none",
            },
        },
    ),
}

PROD_PAGE_1 = [S_ORDERS, S_EVENTS, S_UI]
PROD_PAGE_2 = [S_LOGIC, S_ACTIONS, S_PREFIXED]
DEV_RIVERS = [S_PG, S_DBX, S_S3, S_TMP]


def _page(
    items: List[Dict[str, object]], environment_id: str, next_page: Optional[str]
) -> Dict[str, object]:
    return {
        "items": items,
        "next_page": next_page,
        "current_page_size": len(items),
        "account_id": ACCOUNT_ID,
        "environment_id": environment_id,
    }


def _register_mocks(
    mock: requests_mock.Mocker, prod_page_2: List[Dict[str, object]] = PROD_PAGE_2
) -> None:
    mock.get(
        f"{BASE_URL}/environments/{ENV_PROD}",
        json=_environment(ENV_PROD, "Production", is_default=True),
    )
    mock.get(
        f"{BASE_URL}/environments/{ENV_DEV}", json=_environment(ENV_DEV, "Development")
    )
    mock.get(f"{BASE_URL}/environments/{ENV_LOCKED}", status_code=403, text="denied")
    mock.get(
        f"{BASE_URL}/environments/{ENV_PROD}/rivers?page=1",
        json=_page(PROD_PAGE_1, ENV_PROD, next_page="page-2"),
    )
    mock.get(
        f"{BASE_URL}/environments/{ENV_PROD}/rivers?page=2",
        json=_page(prod_page_2, ENV_PROD, next_page=None),
    )
    mock.get(
        f"{BASE_URL}/environments/{ENV_DEV}/rivers?page=1",
        json=_page(DEV_RIVERS, ENV_DEV, next_page=None),
    )
    for environment_id, summaries in (
        (ENV_PROD, PROD_PAGE_1 + PROD_PAGE_2),
        (ENV_DEV, DEV_RIVERS),
    ):
        for summary in summaries:
            river_id = str(summary["river_cross_id"])
            url = f"{BASE_URL}/environments/{environment_id}/rivers/{river_id}"
            if river_id in RIVERS:
                mock.get(url, json=RIVERS[river_id])
            else:
                mock.get(
                    url,
                    status_code=400,
                    json={"detail": "River was not created via the API"},
                )


def _recipe(output_path: Path) -> Dict[str, object]:
    return {
        "run_id": "boomi-di-test",
        "source": {
            "type": "boomi-data-integration",
            "config": {
                "account_id": ACCOUNT_ID,
                "region": "eu",
                "environment_tokens": {
                    ENV_PROD: "token-prod",
                    ENV_DEV: "token-dev",
                    ENV_LOCKED: "token-locked",
                },
                "river_pattern": {"deny": ["^tmp_.*"]},
                "connection_to_platform": {
                    "conn-bq": {"platform": "bigquery", "database": "my-gcp-project"},
                    "conn-pg-dw": {
                        "platform": "postgres",
                        "platform_instance": "dw",
                        "database": "warehouse",
                        "convert_urns_to_lowercase": False,
                    },
                },
                "max_requests_per_second": 50,
            },
        },
        "sink": {"type": "file", "config": {"filename": str(output_path)}},
    }


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_data_integration_ingestion(
    pytestconfig: pytest.Config, tmp_path: Path
) -> None:
    output_path = tmp_path / "boomi_di_mces.json"
    with requests_mock.Mocker() as mock:
        _register_mocks(mock)
        pipeline = Pipeline.create(_recipe(output_path))
        pipeline.run()
        pipeline.raise_from_status()

        dev_calls = [
            r for r in mock.request_history if f"/environments/{ENV_DEV}" in r.url
        ]
        assert {r.headers["Authorization"] for r in dev_calls} == {"Bearer token-dev"}

    report = pipeline.source.get_report()
    assert isinstance(report, BoomiDataIntegrationSourceReport)
    assert report.environments_emitted == 2
    assert list(report.environments_inaccessible) == [ENV_LOCKED]
    assert report.rivers_emitted == 9
    assert list(report.rivers_filtered) == ["tmp_scratch"]
    assert report.rivers_without_definition == 1
    assert report.logic_steps_emitted == 8
    assert report.logic_steps_disabled == 1
    assert report.targets_object_storage_skipped == 1
    assert len(report.targets_prefix_skipped) == 1
    assert [w.title for w in report.warnings] == [
        "Environment not accessible",
        "River definitions unavailable",
    ]

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=GOLDEN_DIR / "boomi_data_integration_mces_golden.json",
    )


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_boomi_data_integration_stateful_removes_deleted_river(
    tmp_path: Path, mock_datahub_graph: MagicMock
) -> None:
    recipe = _recipe(tmp_path / "run1.json")
    source_config = recipe["source"]["config"]  # type: ignore[index]
    source_config["stateful_ingestion"] = {
        "enabled": True,
        "remove_stale_metadata": True,
        "fail_safe_threshold": 100.0,
        "state_provider": {
            "type": "datahub",
            "config": {"datahub_api": {"server": "http://localhost:8080"}},
        },
    }
    recipe["pipeline_name"] = "boomi_di_stateful_test"

    with (
        patch(
            "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
            mock_datahub_graph,
        ) as mock_checkpoint,
        requests_mock.Mocker() as mock,
    ):
        mock_checkpoint.return_value = mock_datahub_graph
        _register_mocks(mock)
        pipeline_run1 = run_and_get_pipeline(recipe)
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline_run1)

        # The actions river is deleted between runs.
        _register_mocks(
            mock,
            prod_page_2=[r for r in PROD_PAGE_2 if r["river_cross_id"] != "r-actions"],
        )
        recipe["sink"] = {
            "type": "file",
            "config": {"filename": str(tmp_path / "run2.json")},
        }
        pipeline_run2 = run_and_get_pipeline(recipe)
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline_run2)

    assert checkpoint1 and checkpoint1.state
    assert checkpoint2 and checkpoint2.state
    validate_all_providers_have_committed_successfully(pipeline_run1, 1)
    validate_all_providers_have_committed_successfully(pipeline_run2, 1)
    removed = set(
        checkpoint1.state.get_urns_not_in(
            type="dataFlow", other_checkpoint_state=checkpoint2.state
        )
    ) | set(
        checkpoint1.state.get_urns_not_in(
            type="dataJob", other_checkpoint_state=checkpoint2.state
        )
    )
    assert removed == {
        "urn:li:dataFlow:(boomi-data-integration,r-actions,PROD)",
        "urn:li:dataJob:(urn:li:dataFlow:(boomi-data-integration,r-actions,PROD),run)",
    }
    soft_deleted = {
        mcp["entityUrn"]
        for mcp in json.loads((tmp_path / "run2.json").read_text())
        if mcp.get("aspectName") == "status" and mcp["aspect"]["json"]["removed"]
    }
    assert soft_deleted == removed
