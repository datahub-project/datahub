from datetime import datetime
from typing import Iterable, Optional, Tuple

import requests

from datahub.ingestion.api.source import (
    SourceReport,
)
from datahub.ingestion.source.snaplogic.snaplogic_config import SnaplogicConfig
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)


class SnaplogicLineageExtractor:
    """
    A class to interact with the SnapLogic API.
    """

    def __init__(
        self,
        config: SnaplogicConfig,
        redundant_run_skip_handler: Optional[RedundantLineageRunSkipHandler],
        report: SourceReport,
    ):
        self.config = config
        self.report = report
        self.redundant_run_skip_handler = redundant_run_skip_handler
        self.start_time, self.end_time = self._get_time_window()

    def get_lineages(self) -> Iterable[dict]:
        """Generator function that yields lineage records one at a time as they are fetched."""
        page = 0
        has_more = True
        records_processed = 0

        try:
            while has_more:
                params = {
                    "format": "OPENLINEAGE",
                    "start_ts": str(int(self.start_time.timestamp() * 1000)),
                    "end_ts": str(int(self.end_time.timestamp() * 1000)),
                    "page": str(page),
                }

                self.report.info(
                    message=f"Fetching lineage data - page: {page}, start_ts: {self.start_time}, end_ts: {self.end_time}",
                    title="Lineage Fetch",
                )
                headers = {"User-Agent": "datahub-connector/1.0"}
                response = requests.get(
                    url=f"{self.config.base_url}/api/1/rest/public/catalog/{self.config.org_name}/lineage",
                    params=params,
                    headers=headers,
                    auth=(
                        self.config.username,
                        self.config.password.get_secret_value(),
                    ),
                )
                response.raise_for_status()

                data = response.json()
                content = data["content"]

                # Yield records one at a time
                for record in content:
                    records_processed += 1
                    yield record

                # Check if we need to fetch more pages
                has_more = (
                    len(content) >= 20
                )  # If we got full page size, there might be more
                page += 1

            self.report.info(
                message=f"Completed fetching lineage data. Total records processed: {records_processed}",
                title="Lineage Fetch Complete",
            )

        except Exception as e:
            self.report.report_failure(
                message="Error fetching lineage data",
                exc=e,
                title="Lineage Fetch Error",
            )
            raise

    def _get_time_window(self) -> Tuple[datetime, datetime]:
        if self.redundant_run_skip_handler:
            return self.redundant_run_skip_handler.suggest_run_time_window(
                self.config.start_time, self.config.end_time
            )
        else:
            return self.config.start_time, self.config.end_time

    def update_stats(self):
        if self.redundant_run_skip_handler:
            # Update the checkpoint state for this run.
            self.redundant_run_skip_handler.update_state(
                self.config.start_time,
                self.config.end_time,
            )

    def report_status(self, step: str, status: bool) -> None:
        if self.redundant_run_skip_handler:
            self.redundant_run_skip_handler.report_current_run_status(step, status)
