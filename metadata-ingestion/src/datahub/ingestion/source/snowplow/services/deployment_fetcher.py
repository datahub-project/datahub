"""
Deployment fetching service for Snowplow connector.

Handles fetching deployment history for data structures from BDP API,
supporting both parallel and sequential fetching strategies.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from datahub.ingestion.source.snowplow.models.snowplow_models import (
    DataStructure,
    DataStructureDeployment,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.snowplow_client import SnowplowBDPClient
    from datahub.ingestion.source.snowplow.snowplow_report import SnowplowSourceReport

logger = logging.getLogger(__name__)


class DeploymentFetcher:
    """
    Service for fetching deployment history from Snowplow BDP.

    Supports parallel fetching for improved performance when processing
    large numbers of schemas.
    """

    def __init__(
        self,
        bdp_client: Optional["SnowplowBDPClient"],
        enable_parallel: bool = True,
        max_workers: int = 5,
        report: Optional["SnowplowSourceReport"] = None,
    ) -> None:
        """
        Initialize deployment fetcher.

        Args:
            bdp_client: BDP API client instance
            enable_parallel: Whether to use parallel fetching
            max_workers: Maximum concurrent API calls for parallel fetching
            report: Optional report for tracking fetch failures
        """
        self.bdp_client = bdp_client
        self.enable_parallel = enable_parallel
        self.max_workers = max_workers
        self.report = report

    def fetch_deployments(self, data_structures: List[DataStructure]) -> None:
        """
        Fetch deployment history for all data structures.

        Mutates the data_structures list in place, adding deployments to each.

        Args:
            data_structures: List of data structures to fetch deployments for
        """
        if not self.bdp_client:
            logger.warning("No BDP client configured, skipping deployment fetching")
            return

        # Filter to schemas that have a hash (needed for API call)
        schemas_needing_deployments = [ds for ds in data_structures if ds.hash]

        if not schemas_needing_deployments:
            logger.debug("No schemas need deployment fetching")
            return

        logger.info(
            f"Fetching deployment history for {len(schemas_needing_deployments)} schemas"
        )

        if self.enable_parallel and len(schemas_needing_deployments) > 1:
            self._fetch_parallel(schemas_needing_deployments)
        else:
            self._fetch_sequential(schemas_needing_deployments)

    def _fetch_parallel(self, schemas: List[DataStructure]) -> None:
        """
        Fetch deployments in parallel using ThreadPoolExecutor.

        Uses a two-phase approach to avoid race conditions:
        1. Fetch all deployments concurrently, collecting results in a map
        2. Apply results to data structures in single thread

        Args:
            schemas: List of data structures to fetch deployments for
        """
        logger.info(
            f"Fetching deployments in parallel (max_workers={self.max_workers}) "
            f"for {len(schemas)} schemas"
        )

        def fetch_one(
            ds: DataStructure,
        ) -> Tuple[
            str, str, Optional[List[DataStructureDeployment]], Optional[Exception]
        ]:
            """
            Fetch deployments for a single schema without mutating.

            Returns:
                Tuple of (vendor, name, deployments, error)
            """
            try:
                if self.bdp_client and ds.hash:
                    deployments = self.bdp_client.get_data_structure_deployments(
                        ds.hash
                    )
                    logger.debug(
                        f"Fetched {len(deployments) if deployments else 0} deployments "
                        f"for {ds.vendor}/{ds.name}"
                    )
                    return ds.vendor or "", ds.name or "", deployments, None
                return ds.vendor or "", ds.name or "", None, None
            except Exception as e:
                return ds.vendor or "", ds.name or "", None, e

        # Phase 1: Fetch all deployments concurrently
        deployment_map: Dict[str, Optional[List[DataStructureDeployment]]] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(fetch_one, ds) for ds in schemas]

            failed_schemas: List[str] = []
            for future in as_completed(futures):
                vendor, name, deployments, error = future.result()
                schema_key = f"{vendor}/{name}"
                if error:
                    failed_schemas.append(schema_key)
                    logger.warning(
                        f"Failed to fetch deployments for {schema_key}: {error}"
                    )
                    if self.report:
                        self.report.report_deployment_fetch_failure(
                            schema_key, str(error)
                        )
                else:
                    deployment_map[schema_key] = deployments

            if failed_schemas:
                logger.warning(
                    f"Failed to fetch deployments for {len(failed_schemas)} schema(s). "
                    f"These schemas will be extracted without deployment history."
                )

        # Phase 2: Apply results to schemas in single thread
        for ds in schemas:
            schema_key = f"{ds.vendor}/{ds.name}"
            if schema_key in deployment_map:
                deployments = deployment_map[schema_key]
                if deployments is not None:
                    ds.deployments = deployments

        logger.info(
            f"Completed parallel deployment fetching for {len(schemas)} schemas "
            f"({len(deployment_map)} successful)"
        )

    def _fetch_sequential(self, schemas: List[DataStructure]) -> None:
        """
        Fetch deployments sequentially.

        Args:
            schemas: List of data structures to fetch deployments for
        """
        if not self.bdp_client:
            return

        for ds in schemas:
            try:
                if ds.hash:
                    deployments = self.bdp_client.get_data_structure_deployments(
                        ds.hash
                    )
                    ds.deployments = deployments
                    logger.debug(
                        f"Fetched {len(deployments)} deployments for {ds.vendor}/{ds.name}"
                    )
            except Exception as e:
                schema_key = f"{ds.vendor}/{ds.name}"
                logger.warning(f"Failed to fetch deployments for {schema_key}: {e}")
                if self.report:
                    self.report.report_deployment_fetch_failure(schema_key, str(e))
