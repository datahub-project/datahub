#!/usr/bin/env python3
"""Probe MicroStrategy REST APIs used by PR 16992.

This is a research script for connector development. It reads the ``mstr`` block
from ``~/.datahubenv`` by default, calls the REST endpoints used by the
MicroStrategy connector in datahub-project/datahub#16992, and writes a sanitized
JSON report. It never writes credentials, auth tokens, connection strings, or
raw SQL. Warehouse table names parsed from SQL are replaced with stable fake
sales/orders table names.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any, Iterable, Mapping


REDACTED_KEYS = {
    "authtoken",
    "connectionstring",
    "cookie",
    "password",
    "secret",
    "sessionid",
    "token",
    "x-mstr-authtoken",
}

SQL_KEYS = {"sql", "sqlStatement", "statement"}

FAKE_TABLES = [
    "SALES_DB.SALES.fact_sales",
    "SALES_DB.SALES.dim_customer",
    "SALES_DB.SALES.dim_product",
    "SALES_DB.SALES.dim_store",
    "SALES_DB.SALES.dim_calendar",
    "SALES_DB.SALES.sales_plan",
    "SALES_DB.SALES.sales_region",
    "SALES_DB.ORDERS.fact_orders",
    "SALES_DB.ORDERS.order_items",
    "SALES_DB.ORDERS.order_status",
    "SALES_DB.ORDERS.fulfillment",
    "SALES_DB.ORDERS.returns",
]

PR16992_API_CALLS = [
    "POST /api/auth/login",
    "POST /api/auth/logout",
    "GET /api/projects",
    "GET /api/v2/projects/{project_id}",
    "GET /api/v2/projects/{project_id}/folders",
    "GET /api/v2/folders/{folder_id}",
    "GET /api/v2/users",
    "GET /api/v2/users/{user_id}",
    "GET /api/datasources",
    "GET /api/projects/{project_id}/datasources",
    "GET /api/searches/results?type={object_type}",
    "GET /api/objects/{object_id}?type={object_type}",
    "GET /api/v2/tables/{table_id}",
    "GET /api/model/tables",
    "GET /api/model/tables/{model_table_id}",
    "GET /api/datasources/{datasource_id}",
    "GET /api/datasources/connections/{connection_id}",
    "GET /api/model/attributes/{attribute_id}",
    "GET /api/model/metrics/{metric_id}",
    "GET /api/v2/cubes/{cube_id}",
    "GET /api/v2/cubes/{cube_id}/sqlView",
    "GET /api/v2/reports/{report_id}",
    "POST /api/v2/reports/{report_id}/instances?executionStage=resolve_prompts",
    "GET /api/v2/reports/{report_id}/instances/{instance_id}/sqlView",
    "DELETE /api/v2/reports/{report_id}/instances/{instance_id}",
    "GET /api/documents/{document_id}/definition",
    "GET /api/v2/dossiers/{dossier_id}/definition",
    "POST /api/documents/{document_id}/instances",
    "POST /api/dossiers/{dossier_id}/instances",
    "GET /api/dossiers/{dossier_id}/instances/{instance_id}/datasets/sqlView",
    "DELETE /api/dossiers/{dossier_id}/instances/{instance_id}",
    "DELETE /api/documents/{document_id}/instances/{instance_id} (cleanup fallback)",
    "GET /api/v2/projects/{project_id}/datasets",
    "GET /api/v2/datasets/{dataset_id}",
]


@dataclass
class ApiResponse:
    ok: bool
    method: str
    path: str
    status: int | None
    body: Any = None
    error: str | None = None


class MicroStrategyProbeClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: int,
        username: str,
        password: str,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.username = username
        self.password = password
        self.auth_token: str | None = None
        self.cookie_jar = CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookie_jar)
        )

    def login(self) -> ApiResponse:
        response = self.request(
            "POST",
            "/api/auth/login",
            body={
                "loginMode": 1,
                "username": self.username,
                "password": self.password,
            },
            include_auth=False,
            expected_empty=True,
        )
        if response.ok and isinstance(response.body, dict):
            headers = response.body.get("headers", {})
            if isinstance(headers, dict):
                self.auth_token = next(
                    (
                        str(value)
                        for key, value in headers.items()
                        if str(key).lower() == "x-mstr-authtoken"
                    ),
                    None,
                )
        return response

    def logout(self) -> ApiResponse:
        return self.request(
            "POST",
            "/api/auth/logout",
            expected_empty=True,
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        project_id: str | None = None,
        query: Mapping[str, Any] | None = None,
        body: Mapping[str, Any] | None = None,
        include_auth: bool = True,
        expected_empty: bool = False,
        timeout: int | None = None,
    ) -> ApiResponse:
        url_path = path
        if query:
            separator = "&" if "?" in url_path else "?"
            url_path = f"{url_path}{separator}{urllib.parse.urlencode(query, doseq=True)}"

        headers = {"Accept": "application/json"}
        data: bytes | None = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if include_auth and self.auth_token:
            headers["X-MSTR-AuthToken"] = self.auth_token
        if project_id:
            headers["X-MSTR-ProjectID"] = project_id

        request = urllib.request.Request(
            f"{self.base_url}{url_path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with self.opener.open(request, timeout=timeout or self.timeout) as response:
                status = response.status
                response_headers = dict(response.headers.items())
                payload = response.read()
                if expected_empty or not payload:
                    return ApiResponse(
                        ok=True,
                        method=method,
                        path=url_path,
                        status=status,
                        body={"headers": response_headers},
                    )
                parsed = json.loads(payload.decode("utf-8"))
                return ApiResponse(
                    ok=True,
                    method=method,
                    path=url_path,
                    status=status,
                    body=parsed,
                )
        except urllib.error.HTTPError as error:
            payload = error.read().decode("utf-8", errors="replace")
            return ApiResponse(
                ok=False,
                method=method,
                path=url_path,
                status=error.code,
                error=payload[:1000],
            )
        except Exception as error:
            return ApiResponse(
                ok=False,
                method=method,
                path=url_path,
                status=None,
                error=str(error),
            )


def main() -> int:
    args = parse_args()
    credentials = load_mstr_credentials(args.env_file)
    client = MicroStrategyProbeClient(
        credentials["base_url"],
        timeout=args.timeout,
        username=credentials["username"],
        password=credentials["password"],
    )
    sanitizer = Sanitizer(sample_size=args.sample_size)
    report: dict[str, Any] = {
        "generatedAt": int(time.time()),
        "source": "datahub-project/datahub#16992",
        "apiInventory": PR16992_API_CALLS,
        "target": {
            "dashboardNameHash": hash_value(args.dashboard_name)
            if args.dashboard_name
            else None,
            "projectNameHash": hash_value(args.project_name)
            if args.project_name
            else None,
        },
        "endpoints": [],
        "lineageFindings": {},
        "metadataMapping": metadata_mapping_summary(),
    }
    metadata_payloads_for_id_collection: list[Any] = []

    try:
        add_result(report, "auth.login", client.login(), sanitizer)
        if not client.auth_token:
            raise RuntimeError("Login did not return X-MSTR-AuthToken")

        projects = call(
            report,
            "projects.list",
            client,
            sanitizer,
            "GET",
            "/api/projects",
        )
        project = select_project(projects, args.project_name)
        if not project:
            raise RuntimeError("No project was found")
        project_id = str(project.get("id") or project.get("projectId"))
        report["target"]["selectedProjectIdHash"] = hash_value(project_id)

        call(
            report,
            "projects.get",
            client,
            sanitizer,
            "GET",
            f"/api/v2/projects/{project_id}",
        )
        folders = call(
            report,
            "folders.list",
            client,
            sanitizer,
            "GET",
            f"/api/v2/projects/{project_id}/folders",
        )
        folder = first_row(folders)
        if folder:
            folder_id = str(folder.get("id") or folder.get("objectId"))
            call(
                report,
                "folders.get",
                client,
                sanitizer,
                "GET",
                f"/api/v2/folders/{folder_id}",
            )

        users = call(report, "users.list", client, sanitizer, "GET", "/api/v2/users")
        user = first_row(users)
        if user:
            user_id = str(user.get("id") or user.get("objectId"))
            call(report, "users.get", client, sanitizer, "GET", f"/api/v2/users/{user_id}")

        datasources = call(
            report,
            "datasources.list",
            client,
            sanitizer,
            "GET",
            "/api/datasources",
        )
        project_datasources = call(
            report,
            "datasources.project_list",
            client,
            sanitizer,
            "GET",
            f"/api/projects/{project_id}/datasources",
        )
        source_candidates = rows_from_payload(project_datasources) or rows_from_payload(
            datasources
        )
        datasource = first_real_datasource(source_candidates)
        if datasource:
            datasource_id = str(datasource.get("id") or datasource.get("objectId"))
            call(
                report,
                "datasources.get",
                client,
                sanitizer,
                "GET",
                f"/api/datasources/{datasource_id}",
                project_id=project_id,
            )
            connection_id = datasource_connection_id(datasource)
            if connection_id:
                connection_body = call(
                    report,
                    "datasource_connections.get",
                    client,
                    sanitizer,
                    "GET",
                    f"/api/datasources/connections/{connection_id}",
                )
                report["lineageFindings"]["datasourceConnection"] = (
                    summarize_connection_string(connection_body)
                )

        dashboards = call(
            report,
            "search.type_55_dashboards",
            client,
            sanitizer,
            "GET",
            "/api/searches/results",
            project_id=project_id,
            query={"type": 55, "offset": 0, "limit": args.search_limit},
        )
        dashboard = select_dashboard(dashboards, args.dashboard_name)
        if dashboard:
            dashboard_id = str(dashboard.get("id") or dashboard.get("objectId"))
            dashboard_subtype = str(dashboard.get("subtype") or "")
            report["target"]["selectedDashboardIdHash"] = hash_value(dashboard_id)
            call(
                report,
                "objects.get_dashboard",
                client,
                sanitizer,
                "GET",
                f"/api/objects/{dashboard_id}",
                project_id=project_id,
                query={"type": 55},
            )
            if dashboard_subtype == "14081":
                document_definition = call(
                    report,
                    "documents.definition",
                    client,
                    sanitizer,
                    "GET",
                    f"/api/documents/{dashboard_id}/definition",
                    project_id=project_id,
                )
                metadata_payloads_for_id_collection.append(document_definition)
                probe_document_sql(
                    report,
                    client,
                    sanitizer,
                    project_id=project_id,
                    document_id=dashboard_id,
                    is_legacy=True,
                    timeout=args.sql_timeout,
                )
            else:
                dossier_definition = call(
                    report,
                    "dossiers.definition",
                    client,
                    sanitizer,
                    "GET",
                    f"/api/v2/dossiers/{dashboard_id}/definition",
                    project_id=project_id,
                )
                metadata_payloads_for_id_collection.append(dossier_definition)
                probe_document_sql(
                    report,
                    client,
                    sanitizer,
                    project_id=project_id,
                    document_id=dashboard_id,
                    is_legacy=False,
                    timeout=args.sql_timeout,
                )

        reports = call(
            report,
            "search.type_3_reports_and_cubes",
            client,
            sanitizer,
            "GET",
            "/api/searches/results",
            project_id=project_id,
            query={"type": 3, "offset": 0, "limit": args.search_limit},
        )
        report_objects = rows_from_payload(reports)
        report_obj = first_by_subtype(report_objects, deny_subtypes={"776"})
        cube_obj = first_by_subtype(report_objects, allow_subtypes={"776"})

        configured_cube_search = call(
            report,
            "search.configured_cube_type",
            client,
            sanitizer,
            "GET",
            "/api/searches/results",
            project_id=project_id,
            query={"type": args.cube_search_type, "offset": 0, "limit": args.search_limit},
        )
        if not cube_obj:
            cube_obj = first_row(configured_cube_search)

        warehouse_tables = call(
            report,
            "search.type_53_warehouse_tables",
            client,
            sanitizer,
            "GET",
            "/api/searches/results",
            project_id=project_id,
            query={"type": 53, "offset": 0, "limit": min(args.search_limit, 5)},
        )
        warehouse_table = first_row(warehouse_tables)
        if warehouse_table:
            table_id = str(warehouse_table.get("id") or warehouse_table.get("objectId"))
            call(
                report,
                "tables.v2_get",
                client,
                sanitizer,
                "GET",
                f"/api/v2/tables/{table_id}",
                project_id=project_id,
            )

        model_tables = call(
            report,
            "model_tables.list",
            client,
            sanitizer,
            "GET",
            "/api/model/tables",
            project_id=project_id,
            query={"limit": 5, "offset": 0},
        )
        model_table = first_row(model_tables, nested_key="tables")
        if model_table:
            model_table_id = nested_object_id(model_table)
            if model_table_id:
                call(
                    report,
                    "model_tables.get",
                    client,
                    sanitizer,
                    "GET",
                    f"/api/model/tables/{model_table_id}",
                    project_id=project_id,
                )

        if report_obj:
            report_payload = call(
                report,
                "reports.get",
                client,
                sanitizer,
                "GET",
                f"/api/v2/reports/{str(report_obj.get('id') or report_obj.get('objectId'))}",
                project_id=project_id,
            )
            metadata_payloads_for_id_collection.append(report_payload)
            probe_report_sql(
                report,
                client,
                sanitizer,
                project_id=project_id,
                report_id=str(report_obj.get("id") or report_obj.get("objectId")),
                timeout=args.sql_timeout,
            )
        if cube_obj:
            cube_id = str(cube_obj.get("id") or cube_obj.get("objectId"))
            cube_payload = call(
                report,
                "cubes.get",
                client,
                sanitizer,
                "GET",
                f"/api/v2/cubes/{cube_id}",
                project_id=project_id,
            )
            metadata_payloads_for_id_collection.append(cube_payload)
            cube_sql = call(
                report,
                "cubes.sql_view",
                client,
                sanitizer,
                "GET",
                f"/api/v2/cubes/{cube_id}/sqlView",
                project_id=project_id,
                timeout=args.sql_timeout,
            )
            report["lineageFindings"]["cubeSqlView"] = summarize_sql_payload(
                cube_sql, sanitizer
            )

        datasets = call(
            report,
            "datasets.list",
            client,
            sanitizer,
            "GET",
            f"/api/v2/projects/{project_id}/datasets",
        )
        dataset = first_row(datasets)
        if dataset:
            dataset_id = str(dataset.get("id") or dataset.get("objectId"))
            dataset_payload = call(
                report,
                "datasets.get",
                client,
                sanitizer,
                "GET",
                f"/api/v2/datasets/{dataset_id}",
                project_id=project_id,
            )
            metadata_payloads_for_id_collection.append(dataset_payload)

        ids = collect_metric_attribute_ids(
            [
                dashboards,
                reports,
                configured_cube_search,
                *metadata_payloads_for_id_collection,
            ]
        )
        if ids["attributes"]:
            call(
                report,
                "model_attributes.get",
                client,
                sanitizer,
                "GET",
                f"/api/model/attributes/{ids['attributes'][0]}",
                project_id=project_id,
            )
        if ids["metrics"]:
            call(
                report,
                "model_metrics.get",
                client,
                sanitizer,
                "GET",
                f"/api/model/metrics/{ids['metrics'][0]}",
                project_id=project_id,
            )

    finally:
        add_result(report, "auth.logout", client.logout(), sanitizer)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(str(args.output))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path.home() / ".datahubenv",
        help="Path to a YAML-like file containing mstr.base_url, username, password.",
    )
    parser.add_argument("--project-name", default=None)
    parser.add_argument("--dashboard-name", default=None)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sql-timeout", type=int, default=180)
    parser.add_argument("--sample-size", type=int, default=3)
    parser.add_argument("--search-limit", type=int, default=25)
    parser.add_argument("--cube-search-type", type=int, default=776)
    return parser.parse_args()


def _clean_scalar(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and (value[0], value[-1]) in {("'", "'"), ('"', '"')}:
        return value[1:-1]
    return value


def load_mstr_credentials(path: Path) -> dict[str, str]:
    creds: dict[str, str] = {}
    in_mstr = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not raw_line.startswith((" ", "\t")) and stripped.endswith(":"):
            in_mstr = stripped[:-1] == "mstr"
            continue
        if in_mstr and ":" in stripped:
            key, value = stripped.split(":", 1)
            if key in {"base_url", "username", "password"}:
                creds[key] = _clean_scalar(value)

    missing = {"base_url", "username", "password"} - set(creds)
    if missing:
        raise ValueError(f"Missing mstr fields in {path}: {sorted(missing)}")
    return creds


class Sanitizer:
    def __init__(self, *, sample_size: int) -> None:
        self.sample_size = sample_size
        self._fake_table_by_real: dict[str, str] = {}
        self._fake_name_by_real: dict[str, str] = {}

    def payload_summary(self, payload: Any) -> dict[str, Any]:
        rows = rows_from_payload(payload)
        if rows:
            return {
                "shape": "collection",
                "count": len(rows),
                "sample": [self.sanitize(row) for row in rows[: self.sample_size]],
            }
        if isinstance(payload, dict):
            return {
                "shape": "object",
                "keys": sorted(str(key) for key in payload.keys()),
                "sample": self.sanitize(payload),
            }
        if isinstance(payload, list):
            return {
                "shape": "collection",
                "count": len(payload),
                "sample": [self.sanitize(row) for row in payload[: self.sample_size]],
            }
        return {"shape": type(payload).__name__}

    def sanitize(self, value: Any, *, key: str = "") -> Any:
        lowered_key = key.lower()
        if lowered_key in REDACTED_KEYS or any(
            token in lowered_key for token in ("password", "secret", "token", "cookie")
        ):
            return "<redacted>"
        if key in SQL_KEYS:
            sql = str(value or "")
            return {
                "sqlStatementLength": len(sql),
                "parsedWarehouseTables": self.fake_tables(extract_tables_from_sql(sql)),
            }
        if isinstance(value, dict):
            return {
                str(child_key): self.sanitize(child_value, key=str(child_key))
                for child_key, child_value in value.items()
            }
        if isinstance(value, list):
            return [
                self.sanitize(item, key=key)
                for item in value[: self.sample_size]
            ]
        if isinstance(value, str):
            return self.sanitize_string(value)
        return value

    def fake_tables(self, tables: Iterable[str]) -> list[str]:
        fake: list[str] = []
        for table in sorted(set(tables)):
            if table not in self._fake_table_by_real:
                self._fake_table_by_real[table] = FAKE_TABLES[
                    len(self._fake_table_by_real) % len(FAKE_TABLES)
                ]
            fake.append(self._fake_table_by_real[table])
        return fake

    def sanitize_string(self, value: str) -> str:
        if re.fullmatch(r"[A-Fa-f0-9]{32}", value):
            return f"id_{hash_value(value)[:12]}"
        if looks_like_sensitive_connection_string(value):
            return "<redacted_connection_string>"
        if looks_like_sensitive_warehouse_name(value):
            if value not in self._fake_name_by_real:
                self._fake_name_by_real[value] = FAKE_TABLES[
                    len(self._fake_name_by_real) % len(FAKE_TABLES)
                ]
            return self._fake_name_by_real[value]
        return value


def looks_like_sensitive_connection_string(value: str) -> bool:
    lowered = value.lower()
    return any(
        token in lowered
        for token in (
            "jdbc:",
            "odbc;",
            "private_key",
            "snowflakecomputing.com",
            "password=",
            "authenticator=",
        )
    )


def looks_like_sensitive_warehouse_name(value: str) -> bool:
    lowered = value.lower()
    if any(token in lowered for token in ("jdbc", "odbc", "snowflake")):
        return True
    if re.fullmatch(r"[A-Z][A-Z0-9$#]*(?:_[A-Z0-9$#]+)+", value):
        return value.startswith(
            (
                "DIM_",
                "FACT_",
                "FCT_",
                "MERCH_",
                "ORG_",
                "W_",
                "WC_",
            )
        )
    return False


def hash_value(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def add_result(
    report: dict[str, Any],
    name: str,
    response: ApiResponse,
    sanitizer: Sanitizer,
) -> None:
    entry: dict[str, Any] = {
        "name": name,
        "method": response.method,
        "path": sanitize_path(response.path),
        "ok": response.ok,
        "status": response.status,
    }
    if response.ok:
        entry.update(sanitizer.payload_summary(response.body))
    else:
        entry["error"] = sanitizer.sanitize(response.error or "")
    report["endpoints"].append(entry)


def call(
    report: dict[str, Any],
    name: str,
    client: MicroStrategyProbeClient,
    sanitizer: Sanitizer,
    method: str,
    path: str,
    *,
    project_id: str | None = None,
    query: Mapping[str, Any] | None = None,
    body: Mapping[str, Any] | None = None,
    timeout: int | None = None,
) -> Any:
    response = client.request(
        method,
        path,
        project_id=project_id,
        query=query,
        body=body,
        timeout=timeout,
    )
    add_result(report, name, response, sanitizer)
    return response.body if response.ok else None


def sanitize_path(path: str) -> str:
    path = re.sub(r"/[A-Fa-f0-9]{32}(?=/|\?|$)", "/{id}", path)
    path = re.sub(r"searchId=[A-Fa-f0-9-]+", "searchId={id}", path)
    return path


def rows_from_payload(payload: Any, *, nested_key: str | None = None) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    if nested_key and isinstance(payload.get(nested_key), list):
        return [row for row in payload[nested_key] if isinstance(row, dict)]
    for key in (
        "result",
        "results",
        "objects",
        "items",
        "datasets",
        "datasources",
        "folders",
        "tables",
        "users",
    ):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            nested = value.get("items") or value.get("objects") or value.get("result")
            if isinstance(nested, list):
                return [row for row in nested if isinstance(row, dict)]
    return []


def first_row(payload: Any, *, nested_key: str | None = None) -> dict[str, Any] | None:
    rows = rows_from_payload(payload, nested_key=nested_key)
    return rows[0] if rows else None


def select_project(payload: Any, project_name: str | None) -> dict[str, Any] | None:
    rows = rows_from_payload(payload)
    if not project_name:
        return rows[0] if rows else None
    for row in rows:
        if row.get("name") == project_name:
            return row
    return None


def select_dashboard(payload: Any, dashboard_name: str | None) -> dict[str, Any] | None:
    rows = rows_from_payload(payload)
    if dashboard_name:
        for row in rows:
            if (row.get("name") or row.get("title")) == dashboard_name:
                return row
    return rows[0] if rows else None


def first_by_subtype(
    rows: list[dict[str, Any]],
    *,
    allow_subtypes: set[str] | None = None,
    deny_subtypes: set[str] | None = None,
) -> dict[str, Any] | None:
    for row in rows:
        subtype = str(row.get("subtype") or "")
        if allow_subtypes and subtype not in allow_subtypes:
            continue
        if deny_subtypes and subtype in deny_subtypes:
            continue
        return row
    return None


def first_real_datasource(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in rows:
        database = row.get("database")
        if isinstance(database, dict) and database.get("type") not in {
            None,
            "",
            "url_auth",
        }:
            return row
    return rows[0] if rows else None


def datasource_connection_id(datasource: Mapping[str, Any]) -> str | None:
    database = datasource.get("database")
    connection = datasource.get("connection")
    if isinstance(database, dict) and isinstance(database.get("connection"), dict):
        connection = database["connection"]
    if isinstance(connection, dict):
        value = connection.get("id") or connection.get("objectId")
        return str(value) if value else None
    return None


def nested_object_id(value: Mapping[str, Any]) -> str | None:
    info = value.get("information")
    if isinstance(info, dict):
        object_id = info.get("objectId") or info.get("id")
        if object_id:
            return str(object_id)
    object_id = value.get("id") or value.get("objectId")
    return str(object_id) if object_id else None


def probe_document_sql(
    report: dict[str, Any],
    client: MicroStrategyProbeClient,
    sanitizer: Sanitizer,
    *,
    project_id: str,
    document_id: str,
    is_legacy: bool,
    timeout: int,
) -> None:
    create_path = (
        f"/api/documents/{document_id}/instances"
        if is_legacy
        else f"/api/dossiers/{document_id}/instances"
    )
    create_response = client.request(
        "POST",
        create_path,
        project_id=project_id,
        body={},
        timeout=timeout,
    )
    add_result(report, "documents_or_dossiers.instances.create", create_response, sanitizer)
    instance_id = extract_instance_id(create_response.body)
    if not create_response.ok or not instance_id:
        return
    try:
        sql_response = client.request(
            "GET",
            f"/api/dossiers/{document_id}/instances/{instance_id}/datasets/sqlView",
            project_id=project_id,
            timeout=timeout,
        )
        add_result(report, "documents_or_dossiers.datasets_sql_view", sql_response, sanitizer)
        if sql_response.ok:
            report["lineageFindings"]["documentDatasetSqlView"] = summarize_sql_payload(
                sql_response.body,
                sanitizer,
            )
    finally:
        delete_response = client.request(
            "DELETE",
            f"/api/dossiers/{document_id}/instances/{instance_id}",
            project_id=project_id,
            expected_empty=True,
        )
        add_result(
            report,
            "documents_or_dossiers.instances.delete",
            delete_response,
            sanitizer,
        )
        if is_legacy and delete_response.status in {404, 405}:
            fallback_response = client.request(
                "DELETE",
                f"/api/documents/{document_id}/instances/{instance_id}",
                project_id=project_id,
                expected_empty=True,
            )
            add_result(
                report,
                "documents.instances.delete_fallback",
                fallback_response,
                sanitizer,
            )


def probe_report_sql(
    report: dict[str, Any],
    client: MicroStrategyProbeClient,
    sanitizer: Sanitizer,
    *,
    project_id: str,
    report_id: str,
    timeout: int,
) -> None:
    create_response = client.request(
        "POST",
        f"/api/v2/reports/{report_id}/instances",
        project_id=project_id,
        query={"executionStage": "resolve_prompts"},
        body={},
        timeout=timeout,
    )
    add_result(report, "reports.instances.create", create_response, sanitizer)
    instance_id = extract_instance_id(create_response.body)
    if not create_response.ok or not instance_id:
        return
    try:
        sql_response = client.request(
            "GET",
            f"/api/v2/reports/{report_id}/instances/{instance_id}/sqlView",
            project_id=project_id,
            timeout=timeout,
        )
        add_result(report, "reports.instances.sql_view", sql_response, sanitizer)
        if sql_response.ok:
            report["lineageFindings"]["reportSqlView"] = summarize_sql_payload(
                sql_response.body,
                sanitizer,
            )
    finally:
        delete_response = client.request(
            "DELETE",
            f"/api/v2/reports/{report_id}/instances/{instance_id}",
            project_id=project_id,
            expected_empty=True,
        )
        add_result(report, "reports.instances.delete", delete_response, sanitizer)


def extract_instance_id(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("mid", "instanceId", "instanceID", "id"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    result = payload.get("result")
    if isinstance(result, dict):
        return extract_instance_id(result)
    return None


def extract_tables_from_sql(sql: str) -> list[str]:
    if not isinstance(sql, str) or not sql.strip():
        return []
    pattern = (
        r"(?:from|join)\s+"
        r"(?:"
        r'"([\w$#]+)"\."([\w$#]+)"'
        r"|`([\w$#]+)`\.`([\w$#]+)`"
        r"|([\w$#]+)\.([\w$#]+)"
        r'|"([\w$#]+)"'
        r"|`([\w$#]+)`"
        r"|([\w$#]+)"
        r")"
    )
    keywords = {
        "as",
        "cross",
        "delete",
        "full",
        "group",
        "having",
        "inner",
        "into",
        "join",
        "left",
        "on",
        "order",
        "outer",
        "right",
        "select",
        "set",
        "update",
        "where",
        "with",
    }
    volatile_pattern = re.compile(r"^T[A-Z0-9]{10,}$")
    tables: set[str] = set()
    for match in re.findall(pattern, sql, re.IGNORECASE):
        if match[0] and match[1]:
            tables.add(f"{match[0]}.{match[1]}")
        elif match[2] and match[3]:
            tables.add(f"{match[2]}.{match[3]}")
        elif match[4] and match[5]:
            if match[4].lower() not in keywords and match[5].lower() not in keywords:
                tables.add(f"{match[4]}.{match[5]}")
        else:
            bare = match[6] or match[7] or match[8]
            if (
                bare
                and bare.lower() not in keywords
                and not volatile_pattern.match(bare.upper())
            ):
                tables.add(bare)
    return sorted(tables)


def summarize_sql_payload(payload: Any, sanitizer: Sanitizer) -> dict[str, Any]:
    rows = rows_from_payload(payload)
    if not rows and isinstance(payload, dict):
        rows = [payload]
    dataset_summaries: list[dict[str, Any]] = []
    unique_tables: set[str] = set()
    for row in rows:
        sql = str(row.get("sqlStatement") or row.get("sql") or "")
        tables = extract_tables_from_sql(sql)
        fake_tables = sanitizer.fake_tables(tables)
        unique_tables.update(fake_tables)
        dataset_summaries.append(
            {
                "idHash": hash_value(str(row.get("id") or row.get("objectId") or "")),
                "nameHash": hash_value(str(row.get("name") or "")),
                "sqlLength": len(sql),
                "warehouseTables": fake_tables,
            }
        )
    return {
        "sqlEntryCount": len(dataset_summaries),
        "uniqueWarehouseTableCount": len(unique_tables),
        "uniqueWarehouseTables": sorted(unique_tables),
        "entries": dataset_summaries,
    }


def summarize_connection_string(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"connectionStringPresent": False}
    conn = str(payload.get("connectionString") or "")
    return {
        "connectionStringPresent": bool(conn),
        "databasePresent": bool(parse_connection_string_param(conn, "DATABASE", "databaseName", "db", "catalog")),
        "schemaPresent": bool(parse_connection_string_param(conn, "schema", "currentSchema", "CURRENT_SCHEMA", "searchpath")),
    }


def parse_connection_string_param(conn_str: str, *param_names: str) -> str | None:
    if not conn_str:
        return None
    alt = "|".join(re.escape(name) for name in param_names)
    match = re.search(rf"(?:{alt})\s*=\s*([^;&,\s}}]+)", conn_str, re.IGNORECASE)
    return match.group(1) if match else None


def collect_metric_attribute_ids(payloads: Iterable[Any]) -> dict[str, list[str]]:
    ids: dict[str, list[str]] = {"attributes": [], "metrics": []}

    def visit(value: Any, parent_key: str = "") -> None:
        if isinstance(value, dict):
            object_type = str(value.get("type") or value.get("objectType") or "").lower()
            object_id = value.get("id") or value.get("objectId")
            if object_id == "00000000000000000000000000000000":
                object_id = None
            parent = parent_key.lower()
            if object_id and (
                "attribute" in object_type
                or object_type == "12"
                or parent in {"attribute", "attributes"}
            ):
                ids["attributes"].append(str(object_id))
            if object_id and (
                "metric" in object_type
                or object_type == "4"
                or parent in {"metric", "metrics"}
            ):
                ids["metrics"].append(str(object_id))
            for child_key, child in value.items():
                visit(child, str(child_key))
        elif isinstance(value, list):
            for child in value:
                visit(child, parent_key)

    for payload in payloads:
        visit(payload)
    return {key: sorted(set(value)) for key, value in ids.items()}


def metadata_mapping_summary() -> dict[str, Any]:
    return {
        "project": {
            "api": ["GET /api/projects", "GET /api/v2/projects/{project_id}"],
            "datahub": ["container", "containerProperties", "subTypes"],
        },
        "folder": {
            "api": ["GET /api/v2/projects/{project_id}/folders", "GET /api/v2/folders/{folder_id}"],
            "datahub": ["container", "containerProperties", "container parent edges"],
        },
        "dashboard_or_document": {
            "api": [
                "GET /api/searches/results?type=55",
                "GET /api/documents/{document_id}/definition",
                "GET /api/v2/dossiers/{dossier_id}/definition",
            ],
            "datahub": ["dashboard", "dashboardInfo", "ownership", "subTypes"],
        },
        "visualization": {
            "api": ["dashboard/dossier definitions", "runtime instance visualization endpoints"],
            "datahub": ["chart", "chartInfo", "chartInfo.inputs", "subTypes"],
        },
        "embedded_dataset": {
            "api": ["definition datasets[]", "GET /api/dossiers/{id}/instances/{iid}/datasets/sqlView"],
            "datahub": ["dataset", "schemaMetadata", "datasetProperties", "upstreamLineage"],
        },
        "report": {
            "api": ["GET /api/searches/results?type=3", "GET /api/v2/reports/{report_id}", "report sqlView instance APIs"],
            "datahub": ["chart", "chartInfo", "chartInfo.inputs"],
        },
        "intelligent_cube": {
            "api": ["GET /api/searches/results?type=776", "GET /api/v2/cubes/{cube_id}", "GET /api/v2/cubes/{cube_id}/sqlView"],
            "datahub": ["dataset", "schemaMetadata", "viewProperties", "upstreamLineage"],
        },
        "metric_attribute_fact": {
            "api": ["GET /api/model/metrics/{metric_id}", "GET /api/model/attributes/{attribute_id}"],
            "datahub": ["schemaField", "globalTags", "glossaryTerms", "jsonProps"],
        },
        "warehouse_source": {
            "api": [
                "GET /api/datasources",
                "GET /api/projects/{project_id}/datasources",
                "GET /api/datasources/{datasource_id}",
                "GET /api/datasources/connections/{connection_id}",
            ],
            "datahub": ["customProperties", "dataPlatformInstance", "upstream dataset URN construction"],
        },
    }


if __name__ == "__main__":
    sys.exit(main())
