"""REST API client for Microsoft Fabric Data Factory items."""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.core_client import FabricCoreClient
from datahub.ingestion.source.fabric.common.models import FabricJobInstance
from datahub.ingestion.source.fabric.common.report import FabricClientReport
from datahub.ingestion.source.fabric.data_factory.models import PipelineActivity

logger = logging.getLogger(__name__)


class FabricDataFactoryClient(FabricCoreClient):
    """Client for Microsoft Fabric Data Factory.

    Inherits workspace and item listing from FabricCoreClient.
    Use list_workspaces() and list_items(workspace_id, item_type) directly.

    Supported item types: "DataPipeline", "CopyJob", "Dataflow"
    """

    def __init__(
        self,
        auth_helper: FabricAuthHelper,
        timeout: int = 30,
        report: Optional[FabricClientReport] = None,
    ):
        super().__init__(auth_helper, timeout, report)

    def get_pipeline_runs(
        self,
        workspace_id: str,
        pipeline_id: str,
        lookback_window_start: datetime,
    ) -> List[FabricJobInstance]:
        """Fetch pipeline job instances within the lookback window.

        Uses the Core API's list_item_job_instances() which returns runs
        in descending order by startTimeUtc (newest first).

        Stops consuming the lazy iterator as soon as a run's startTimeUtc
        falls before the lookback_window_start, avoiding unnecessary pages.

        API limitation: Returns at most 100 recently completed runs
        plus all currently active runs.

        Args:
            workspace_id: Workspace GUID
            pipeline_id: Data Pipeline item GUID
            lookback_window_start: Cutoff datetime (UTC). Runs older than
                this are excluded and iteration stops.

        Returns:
            List of FabricJobInstance within the lookback window.
        """
        logger.debug(
            f"Fetching pipeline runs for {pipeline_id} "
            f"(lookback since {lookback_window_start.isoformat()})"
        )

        runs: List[FabricJobInstance] = []
        for job_instance in self.list_item_job_instances(workspace_id, pipeline_id):
            if job_instance.start_time_utc:
                run_start = datetime.fromisoformat(
                    job_instance.start_time_utc.replace("Z", "+00:00")
                )
                # Fabric API returns UTC timestamps without offset suffix
                if run_start.tzinfo is None:
                    run_start = run_start.replace(tzinfo=timezone.utc)
                if run_start < lookback_window_start:
                    logger.debug(
                        f"Reached lookback boundary for pipeline {pipeline_id}, "
                        f"stopping after {len(runs)} run(s)"
                    )
                    break

            runs.append(job_instance)

        return runs

    def get_pipeline_activities(
        self,
        workspace_id: str,
        pipeline_id: str,
    ) -> List[PipelineActivity]:
        """Fetch pipeline definition and parse activities from it.

        Uses the Core API's get_item_definition() to retrieve the pipeline
        definition, then extracts activities from the pipeline-content.json part.
        Malformed activities (missing name/type) are skipped.

        Args:
            workspace_id: Workspace GUID
            pipeline_id: Data Pipeline item GUID

        Returns:
            List of PipelineActivity parsed from the definition.
        """
        parts = self.get_item_definition(workspace_id, pipeline_id)

        for part in parts:
            if part.get("path") == "pipeline-content.json":
                content = part.get("content", {})
                if not isinstance(content, dict):
                    break
                raw_activities = content.get("properties", {}).get("activities", [])
                activities: List[PipelineActivity] = []
                for activity_dict in raw_activities:
                    if not isinstance(activity_dict, dict):
                        continue
                    try:
                        activities.append(PipelineActivity.from_dict(activity_dict))
                    except KeyError as e:
                        logger.debug(
                            f"Skipping malformed activity in pipeline "
                            f"{pipeline_id}: missing required field {e}"
                        )
                return activities

        logger.warning(
            f"No pipeline-content.json found in definition for pipeline {pipeline_id}"
        )
        return []
