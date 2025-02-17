import base64
import json
import logging
import re
from time import sleep
from typing import Dict, Iterable, List, Optional

import requests

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.source.azure.azure_common import AzureConnectionConfig
from datahub.ingestion.source.ms_fabric.fabric_utils import set_session
from datahub.ingestion.source.ms_fabric.lineage_state import DatasetLineageState
from datahub.ingestion.source.ms_fabric.reporting import AzureFabricSourceReport
from datahub.ingestion.source.ms_fabric.types import Workspace
from datahub.metadata.schema_classes import (
    GenericAspectClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


class ReportDefinition:
    def __init__(self, id: str, name: str, workspace_id: str):
        self.id = id
        self.name = name
        self.workspace_id = workspace_id
        self.columns: List[str] = []
        self.semantic_model_refs: List[Dict[str, str]] = []
        self.display_name: Optional[str] = None
        self.dataset_id: Optional[str] = None
        self.workspace_name: Optional[str] = None


class ReportManager:
    def __init__(
        self,
        azure_config: AzureConnectionConfig,
        workspaces: List[Workspace],
        ctx: PipelineContext,
        report: AzureFabricSourceReport,
        lineage_state: DatasetLineageState,
    ):
        self.azure_config = azure_config
        self.fabric_session = set_session(requests.Session(), azure_config)
        self.workspaces = workspaces
        self.ctx = ctx
        self.report = report
        self.lineage_state = lineage_state
        self.report_map: Dict[str, Dict[str, ReportDefinition]] = {}

    def _get_items_for_workspace(
        self,
        workspace_id: str,
        item_type: str = "Report",
        continuation_token: Optional[str] = None,
    ) -> List[Dict]:
        items = []
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
        params = {"type": item_type}

        if continuation_token:
            params["continuationToken"] = continuation_token

        try:
            response = self.fabric_session.get(url, params=params)
            response.raise_for_status()
            response_data = response.json()

            if "value" in response_data:
                items.extend(response_data["value"])

            if "continuationToken" in response_data:
                items.extend(
                    self._get_items_for_workspace(
                        workspace_id, item_type, response_data["continuationToken"]
                    )
                )
        except Exception as e:
            self.report.report_warning(
                "item_extraction",
                f"Failed to extract items for workspace {workspace_id}: {str(e)}",
            )

        return items

    def _extract_semantic_model_ref(
        self, report_data: Dict
    ) -> Optional[Dict[str, str]]:
        try:
            dataset_ref = report_data.get("datasetReference", {})
            if "byConnection" in dataset_ref:
                conn = dataset_ref["byConnection"]
                conn_str = conn.get("connectionString", "")
                model_id = conn.get("pbiModelDatabaseName")

                workspace_match = re.search(r'myorg/([^"]+)"', conn_str)
                workspace_name = workspace_match.group(1) if workspace_match else None

                model_match = re.search(r'Initial Catalog="([^"]+)"', conn_str)
                model_name = model_match.group(1) if model_match else None

                if workspace_name and model_name and model_id:
                    return {
                        "workspace_name": workspace_name,
                        "model_name": model_name,
                        "model_id": model_id,
                    }

        except Exception as e:
            logger.error(f"Failed to extract semantic model reference: {e}")
        return None

    def _extract_report_metadata(self, platform_data: Dict) -> Optional[str]:
        try:
            metadata = platform_data.get("metadata", {})
            return metadata.get("displayName")
        except Exception as e:
            logger.error(f"Failed to extract report metadata: {e}")
        return None

    def _get_report_definition(self, workspace_id: str, item_id: str) -> Optional[Dict]:
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}/getDefinition"
        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                response = self.fabric_session.post(
                    url, headers=self.fabric_session.headers
                )

                if response.status_code == 429:
                    sleep(retry_delay * (attempt + 1))
                    continue

                if response.status_code == 202:
                    # Get operation ID
                    operation_id = None
                    if "x-ms-operation-id" in response.headers:
                        operation_id = response.headers["x-ms-operation-id"]
                    elif "Location" in response.headers:
                        operation_id = response.headers["Location"].split("/")[-1]

                    if not operation_id:
                        logger.error("No operation ID found in response headers")
                        return None

                    # Poll for completion
                    status_url = (
                        f"https://api.fabric.microsoft.com/v1/operations/{operation_id}"
                    )
                    max_polls = 10
                    poll_delay = 1

                    for _ in range(max_polls):
                        sleep(poll_delay)
                        status_response = self.fabric_session.get(
                            status_url, headers=self.fabric_session.headers
                        )

                        if status_response.status_code == 429:
                            sleep(retry_delay)
                            continue

                        status_response.raise_for_status()
                        status = status_response.json().get("status")

                        if status == "Succeeded":
                            results_url = f"https://api.fabric.microsoft.com/v1/operations/{operation_id}/result"
                            result_response = self.fabric_session.get(
                                results_url, headers=self.fabric_session.headers
                            )
                            result_response.raise_for_status()
                            data = result_response.json()

                            if "definition" not in data:
                                logger.error(f"No definition in response: {data}")
                                return None

                            definition = data["definition"]
                            if "parts" not in definition:
                                return None

                            report_def = {}
                            for part in definition["parts"]:
                                if "payload" not in part:
                                    continue

                                payload = base64.b64decode(part["payload"])
                                if not payload:
                                    continue

                                if part["path"] == "definition.pbir":
                                    report_def["report_data"] = json.loads(payload)
                                elif part["path"] == ".platform":
                                    report_def["platform_data"] = json.loads(payload)

                            return report_def
                        elif status == "Failed":
                            logger.error(f"Operation failed: {status_response.json()}")
                            return None

                    logger.error(
                        "Exceeded maximum polls waiting for operation completion"
                    )
                    return None

                response.raise_for_status()
                return response.json()

            except Exception as e:
                if attempt < max_retries - 1:
                    sleep(retry_delay * (attempt + 1))
                    continue
                logger.error(f"Failed to get report definition: {str(e)}")
                return None

        return None

    def _populate_report_map(self):
        for workspace in self.workspaces:
            self.report_map[workspace.id] = {}
            reports = self._get_items_for_workspace(workspace.id, "Report")

            for report in reports:
                report_def = ReportDefinition(
                    id=report["id"],
                    name=report.get("displayName", ""),
                    workspace_id=workspace.id,
                )
                report_def.workspace_name = workspace.display_name

                definition = self._get_report_definition(workspace.id, report["id"])
                if definition:
                    if "report_data" in definition:
                        model_ref = self._extract_semantic_model_ref(
                            definition["report_data"]
                        )
                        if model_ref:
                            report_def.dataset_id = model_ref["model_id"]
                            report_def.semantic_model_refs.append(model_ref)

                    if "platform_data" in definition:
                        display_name = self._extract_report_metadata(
                            definition["platform_data"]
                        )
                        if display_name:
                            report_def.display_name = display_name

                self.report_map[workspace.id][report["id"]] = report_def

    def get_report_wus(self) -> Iterable[WorkUnit]:
        self._populate_report_map()

        for workspace_id, reports in self.report_map.items():
            for report_id, report in reports.items():
                yield from self._process_report(workspace_id, report_id, report)

    def _process_report(
        self, workspace_id: str, report_id: str, report: ReportDefinition
    ) -> Iterable[WorkUnit]:
        report_urn = f"urn:li:dashboard:(powerbi,reports.{report_id})"

        # Register the report
        self.lineage_state.register_dataset(
            table_name=report.name,
            urn=report_urn,
            columns=report.columns,
            platform="powerbi-report",
        )

        if report.semantic_model_refs:
            for model_ref in report.semantic_model_refs:
                semantic_model_key = (
                    f"{model_ref['workspace_name']}.{model_ref['model_name']}"
                )

                semantic_models = self.lineage_state.get_upstream_datasets(
                    table_name=semantic_model_key, platform="semantic-model"
                )

                if not semantic_models:
                    continue

                semantic_model = semantic_models[0]

                # Find dashboard and charts
                dashboard_entries = self.lineage_state.get_upstream_datasets(
                    table_name=report_id, platform="powerbi-dashboard"
                )

                if not dashboard_entries:
                    continue

                dashboard = dashboard_entries[0]
                charts = dashboard.get("additional_info", {}).get("charts", [])

                # Create inputEdges from semantic model tables to charts
                for chart_urn in charts:
                    table_urns = (
                        semantic_model.get("additional_info", {})
                        .get("tables", {})
                        .values()
                    )

                    if not table_urns:
                        continue

                    input_edges = {
                        "inputEdges": [
                            {"source": table_urn, "target": chart_urn, "type": "USES"}
                            for table_urn in table_urns
                        ]
                    }

                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=chart_urn,
                        aspect=GenericAspectClass(
                            value=json.dumps(input_edges).encode(),
                            contentType="application/json",
                        ),
                    )

                    yield mcp.as_workunit()

                # Create upstream lineage for the report
                upstream_lineage = UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(dataset=semantic_model["urn"], type="TRANSFORMED")
                    ]
                )

                yield MetadataChangeProposalWrapper(
                    entityUrn=report_urn, aspect=upstream_lineage
                ).as_workunit()
