import json
import logging
from typing import Dict, List

from acryl.executor.request.execution_request import ExecutionRequest

from datahub_executor.common.client.fetcher.ingestion.types import IngestionSource
from datahub_executor.common.constants import RUN_INGEST_TASK_NAME
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import CronSchedule, ExecutionRequestSchedule

logger = logging.getLogger(__name__)


def graphql_to_ingestion_sources(
    graphql_ingestion_sources: List[Dict],
) -> List[IngestionSource]:
    logger.debug(
        f"Converting GraphQL ingestion sources to IngestionSource {graphql_ingestion_sources}"
    )
    ingestion_sources = []
    for ingestion_source in graphql_ingestion_sources:
        try:
            METRIC("INGESTION_FETCHER_ITEMS_MAPPED").inc()
            if (
                "urn" in ingestion_source
                and "urn:li:dataHubIngestionSource:cli-" in ingestion_source["urn"]
            ):
                # Skip CLI ingestion runs
                continue
            # Simply parse to our Pydantic models using the raw GraphQL Response.
            ingestion_sources.append(IngestionSource.model_validate(ingestion_source))
        except Exception:
            METRIC("INGESTION_FETCHER_ITEMS_ERRORED", exception="exception").inc()
            logger.exception(
                f"Failed to convert GraphQL IngestionSource object to Python object. {ingestion_source}"
            )
    logger.debug(
        f"Finished converting GraphQL ingestion_source to IngestionSource {ingestion_sources}"
    )
    return ingestion_sources


def ingestion_sources_to_execution_requests(
    ingestion_sources: List[IngestionSource], default_cli_version: str
) -> List[ExecutionRequest]:
    execution_requests = []

    for ingestion_source in ingestion_sources:
        try:
            recipe = json.loads(ingestion_source.config.recipe)
            if "pipeline_name" not in recipe or recipe["pipeline_name"] == "":
                recipe["pipeline_name"] = ingestion_source.urn

            execution_request = ExecutionRequest(
                executor_id=ingestion_source.config.executor_id,
                exec_id=ingestion_source.urn,
                name=RUN_INGEST_TASK_NAME,
                args={
                    "urn": ingestion_source.urn,
                    "recipe": json.dumps(recipe),
                    "version": (
                        ingestion_source.config.version
                        if ingestion_source.config.version
                        else default_cli_version
                    ),
                    "debug_mode": ingestion_source.config.debug_mode,
                    **ingestion_source.config.extra_args,
                },
            )
            if ingestion_source.schedule:
                execution_requests.append(
                    ExecutionRequestSchedule(
                        execution_request=execution_request,
                        schedule=CronSchedule(
                            cron=ingestion_source.schedule.interval,
                            timezone=ingestion_source.schedule.timezone,
                        ),
                    )
                )
        except Exception as e:
            METRIC("INGESTION_FETCHER_ITEMS_ERRORED", exception="exception").inc()
            logger.warning(f"Exception while fetching ingestion sources: {e}")

    return execution_requests
