import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Set

import requests

from datahub.ingestion.source.microstrategy.config import (
    MicroStrategyConfig,
    MicroStrategyGuestAuth,
    MicroStrategyPasswordAuth,
)
from datahub.ingestion.source.microstrategy.constants import (
    MSTR_LOGIN_MODE_GUEST,
    MSTR_LOGIN_MODE_STANDARD,
)
from datahub.ingestion.source.microstrategy.models import (
    Datasource,
    DatasourceConnection,
    MSTRObject,
    Project,
)
from datahub.ingestion.source.microstrategy.report import MicroStrategyReport

logger = logging.getLogger(__name__)


class MicroStrategyAPIError(RuntimeError):
    pass


class MicroStrategyClient:
    def __init__(self, config: MicroStrategyConfig, report: MicroStrategyReport):
        self.config = config
        self.report = report
        self.base_url = config.base_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def login(self) -> None:
        auth = self.config.auth
        payload: Dict[str, Any]
        if isinstance(auth, MicroStrategyGuestAuth):
            payload = {"loginMode": MSTR_LOGIN_MODE_GUEST}
        elif isinstance(auth, MicroStrategyPasswordAuth):
            payload = {
                "loginMode": MSTR_LOGIN_MODE_STANDARD,
                "username": auth.username,
                "password": auth.password.get_secret_value(),
            }
        else:
            raise MicroStrategyAPIError(f"Unsupported auth config {type(auth)}")

        response = self._request("POST", "/api/auth/login", json=payload)
        token = response.headers.get("X-MSTR-AuthToken")
        if not token:
            raise MicroStrategyAPIError("MicroStrategy login did not return auth token")
        self.session.headers.update({"X-MSTR-AuthToken": token})

    def close(self) -> None:
        try:
            self._request("POST", "/api/auth/logout")
        except Exception:
            logger.debug("MicroStrategy logout failed", exc_info=True)
        self.session.close()

    def list_projects(self) -> List[Project]:
        payload = self._get_json("/api/projects")
        projects: Any
        if isinstance(payload, list):
            projects = payload
        elif isinstance(payload, dict):
            projects = payload.get("projects") or payload.get("result") or []
        else:
            projects = []
        return [
            Project.model_validate(project)
            for project in projects
            if isinstance(project, dict)
        ]

    def list_datasources(self, project_id: str) -> List[Datasource]:
        payload = self._get_json("/api/datasources", project_id=project_id)
        datasources = self._extract_list(payload, "datasources")
        return [
            Datasource.model_validate(datasource)
            for datasource in datasources
            if isinstance(datasource, dict)
        ]

    def list_project_datasources(self, project_id: str) -> List[Datasource]:
        payload = self._get_json(
            f"/api/projects/{project_id}/datasources",
            project_id=project_id,
        )
        datasources = self._extract_list(payload, "datasources")
        return [
            Datasource.model_validate(datasource)
            for datasource in datasources
            if isinstance(datasource, dict)
        ]

    def get_datasource(self, project_id: str, datasource_id: str) -> Datasource:
        payload = self._get_json(
            f"/api/datasources/{datasource_id}",
            project_id=project_id,
        )
        result = payload.get("result") if isinstance(payload, dict) else None
        return Datasource.model_validate(result if isinstance(result, dict) else payload)

    def list_datasource_connections(
        self, project_id: str
    ) -> List[DatasourceConnection]:
        payload = self._get_json("/api/datasources/connections", project_id=project_id)
        connections = self._extract_list(payload, "connections")
        return [
            DatasourceConnection.model_validate(connection)
            for connection in connections
            if isinstance(connection, dict)
        ]

    def get_datasource_connection(
        self, connection_id: str, project_id: Optional[str] = None
    ) -> DatasourceConnection:
        payload = self._get_json(
            f"/api/datasources/connections/{connection_id}",
            project_id=project_id,
        )
        result = payload.get("result") if isinstance(payload, dict) else None
        return DatasourceConnection.model_validate(
            result if isinstance(result, dict) else payload
        )

    def search_dashboards(self, project_id: str) -> Iterable[MSTRObject]:
        for item in self._metadata_search(
            project_id=project_id,
            type_filter="55",
        ):
            yield MSTRObject.model_validate(item)

    def search_objects(
        self,
        project_id: str,
        type_filter: str,
        limit: Optional[int] = None,
    ) -> List[MSTRObject]:
        results: List[MSTRObject] = []
        for item in self._metadata_search(project_id=project_id, type_filter=type_filter):
            results.append(MSTRObject.model_validate(item))
            if limit is not None and len(results) >= limit:
                break
        return results

    def get_object_dependencies(
        self,
        project_id: str,
        object_id: str,
        object_type: str,
    ) -> List[MSTRObject]:
        create_response = self._get_json(
            "/api/metadataSearches/results",
            project_id=project_id,
            params={
                "domain": 2,
                "usedByObject": f"{object_id};{object_type}",
                "usedByRecursive": "false",
            },
            method="POST",
        )
        search_id = self._extract_search_id(create_response)
        if not search_id:
            raise MicroStrategyAPIError(
                "MicroStrategy metadata search response did not include search id "
                f"for object_id={object_id}, object_type={object_type}"
            )
        result = self._get_json(
            "/api/metadataSearches/results",
            project_id=project_id,
            params={"searchId": search_id, "offset": 0, "limit": -1},
        )
        return [
            MSTRObject.model_validate(item)
            for item in self._extract_search_results(result)
        ]

    def get_metric_model(self, project_id: str, metric_id: str) -> Dict[str, Any]:
        return self._get_json(
            f"/api/model/metrics/{metric_id}",
            project_id=project_id,
            params={"showExpressionAs": "tokens"},
        )

    def get_model_table(self, project_id: str, table_id: str) -> Dict[str, Any]:
        return self._get_json(
            f"/api/model/tables/{table_id}",
            project_id=project_id,
        )

    def list_model_tables(
        self,
        project_id: str,
        limit: int = 1,
        offset: int = 0,
        fields: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if fields:
            params["fields"] = fields
        return self._get_json(
            "/api/model/tables",
            project_id=project_id,
            params=params,
        )

    def get_dossier_definition(self, project_id: str, dossier_id: str) -> Dict[str, Any]:
        return self._get_json(
            f"/api/v2/dossiers/{dossier_id}/definition",
            project_id=project_id,
        )

    def get_document_definition(self, project_id: str, document_id: str) -> Dict[str, Any]:
        return self._get_json(
            f"/api/documents/{document_id}/definition",
            project_id=project_id,
        )

    def create_dossier_instance(self, project_id: str, dossier_id: str) -> str:
        response = self._get_json(
            f"/api/dossiers/{dossier_id}/instances",
            project_id=project_id,
            method="POST",
            json={},
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
        )
        instance_id = self._extract_instance_id(response)
        if not instance_id:
            raise MicroStrategyAPIError(
                "MicroStrategy dossier instance response did not include "
                f"an instance id for {dossier_id}"
        )
        return instance_id

    def create_document_instance(self, project_id: str, document_id: str) -> str:
        response = self._get_json(
            f"/api/documents/{document_id}/instances",
            project_id=project_id,
            method="POST",
            json={},
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
        )
        instance_id = self._extract_instance_id(response)
        if not instance_id:
            raise MicroStrategyAPIError(
                "MicroStrategy document instance response did not include "
                f"an instance id for {document_id}"
            )
        return instance_id

    def get_dossier_datasets_sql(
        self,
        project_id: str,
        dossier_id: str,
        instance_id: str,
    ) -> List[Dict[str, Any]]:
        payload = self._get_json(
            f"/api/dossiers/{dossier_id}/instances/{instance_id}/datasets/sqlView",
            project_id=project_id,
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
        )
        datasets = self._extract_list(payload, "datasets")
        if datasets:
            return [dataset for dataset in datasets if isinstance(dataset, dict)]
        if isinstance(payload, dict) and any(
            key in payload for key in ("sql", "sqlStatement", "statement")
        ):
            return [payload]
        return []

    def get_cube_sql_view(self, project_id: str, cube_id: str) -> Dict[str, Any]:
        return self._get_json(
            f"/api/v2/cubes/{cube_id}/sqlView",
            project_id=project_id,
            timeout_seconds=self.config.warehouse_lineage_sql_timeout_seconds,
        )

    def delete_dossier_instance(
        self,
        project_id: str,
        dossier_id: str,
        instance_id: str,
    ) -> bool:
        response = self._request(
            "DELETE",
            f"/api/dossiers/{dossier_id}/instances/{instance_id}",
            project_id=project_id,
            expected_statuses={200, 202, 204, 404, 405},
        )
        return response.status_code not in {404, 405}

    def delete_document_instance(
        self,
        project_id: str,
        document_id: str,
        instance_id: str,
    ) -> bool:
        response = self._request(
            "DELETE",
            f"/api/documents/{document_id}/instances/{instance_id}",
            project_id=project_id,
            expected_statuses={200, 202, 204, 404},
        )
        return response.status_code != 404

    def get_dossier_visualization(
        self,
        project_id: str,
        dossier_id: str,
        instance_id: str,
        chapter_key: str,
        visualization_key: str,
    ) -> Dict[str, Any]:
        return self._get_json(
            (
                f"/api/v2/dossiers/{dossier_id}/instances/{instance_id}"
                f"/chapters/{chapter_key}/visualizations/{visualization_key}"
            ),
            project_id=project_id,
        )

    def _metadata_search(
        self,
        project_id: str,
        type_filter: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        offset = 0
        while True:
            payload: Dict[str, Any] = {
                "limit": self.config.page_size,
                "offset": offset,
                "showHidden": self.config.include_hidden,
            }
            if type_filter:
                payload["type"] = type_filter

            result = self._get_json(
                "/api/searches/results",
                project_id=project_id,
                params=payload,
            )
            items = self._extract_search_results(result)
            if not items:
                break
            yield from items
            if len(items) < self.config.page_size:
                break
            offset += len(items)

    @staticmethod
    def _extract_search_results(response: Any) -> List[Dict[str, Any]]:
        if isinstance(response, list):
            return [item for item in response if isinstance(item, dict)]
        if not isinstance(response, dict):
            return []
        for key in ("result", "results", "objects", "items"):
            value = response.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                nested = value.get("items") or value.get("objects")
                if isinstance(nested, list):
                    return [item for item in nested if isinstance(item, dict)]
        return []

    @staticmethod
    def _extract_search_id(response: Any) -> Optional[str]:
        if not isinstance(response, dict):
            return None
        for key in ("id", "searchId"):
            value = response.get(key)
            if isinstance(value, str) and value:
                return value
        result = response.get("result")
        if isinstance(result, dict):
            return MicroStrategyClient._extract_search_id(result)
        return None

    @staticmethod
    def _extract_list(response: Any, key: str) -> List[Any]:
        if isinstance(response, list):
            return response
        if not isinstance(response, dict):
            return []
        value = response.get(key)
        if isinstance(value, list):
            return value
        result = response.get("result")
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            nested = result.get(key) or result.get("items")
            if isinstance(nested, list):
                return nested
        items = response.get("items")
        if isinstance(items, list):
            return items
        return []

    def _get_json(
        self,
        path: str,
        project_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        json: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        response = self._request(
            method,
            path,
            project_id=project_id,
            params=params,
            json=json,
            timeout_seconds=timeout_seconds,
        )
        if not response.content:
            return {}
        value = response.json()
        return value if isinstance(value, dict) else {"result": value}

    @staticmethod
    def _extract_instance_id(response: Any) -> Optional[str]:
        if not isinstance(response, dict):
            return None
        for key in ("instanceId", "instanceID", "mid", "id"):
            value = response.get(key)
            if isinstance(value, str) and value:
                return value
        result = response.get("result")
        if isinstance(result, dict):
            return MicroStrategyClient._extract_instance_id(result)
        return None

    def _request(
        self,
        method: str,
        path: str,
        project_id: Optional[str] = None,
        expected_statuses: Optional[Set[int]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        headers = kwargs.pop("headers", {})
        if project_id:
            headers["X-MSTR-ProjectID"] = project_id
        timeout_seconds = kwargs.pop("timeout_seconds", None)

        attempts = self.config.max_retries + 1
        last_error: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=timeout_seconds or self.config.timeout_seconds,
                    verify=self.config.verify_ssl,
                    **kwargs,
                )
                if expected_statuses and response.status_code in expected_statuses:
                    return response
                if response.status_code >= 500 and attempt < attempts - 1:
                    time.sleep(2**attempt)
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException as error:
                last_error = error
                if attempt < attempts - 1:
                    time.sleep(2**attempt)
                    continue
                self.report.report_api_error()
                raise MicroStrategyAPIError(
                    f"MicroStrategy API request failed: {method} {path}: {error}"
                ) from error

        raise MicroStrategyAPIError(
            f"MicroStrategy API request failed: {method} {path}: {last_error}"
        )
