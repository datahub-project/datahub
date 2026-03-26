"""Azure Data Factory REST API client wrapper.

This module provides a typed client for interacting with the Azure Data Factory
REST API. It handles authentication, pagination, and error handling.

API Documentation: https://learn.microsoft.com/en-us/rest/api/datafactory/
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

from azure.core.credentials import TokenCredential
from azure.core.exceptions import HttpResponseError
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityRun,
    ActivityRunsQueryResponse,
    DataFlowResource,
    DatasetResource,
    Factory,
    LinkedServiceResource,
    PipelineResource,
    PipelineRun,
    PipelineRunsQueryResponse,
    RunFilterParameters,
    TriggerResource,
)

logger = logging.getLogger(__name__)

# Maximum retention period for activity run queries (Azure limit)
MAX_ACTIVITY_RUN_RETENTION_DAYS = 90


class AzureDataFactoryClient:
    """Client for Azure Data Factory REST API.

    Uses the Azure SDK (azure-mgmt-datafactory) directly for type safety and
    automatic pagination handling. Returns SDK model objects.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/
    """

    def __init__(
        self,
        credential: TokenCredential,
        subscription_id: str,
    ) -> None:
        """Initialize the ADF client.

        Args:
            credential: Azure credential for authentication (from AzureCredentialConfig)
            subscription_id: Azure subscription ID containing Data Factories
        """
        self.subscription_id = subscription_id
        self._client = DataFactoryManagementClient(
            credential=credential,
            subscription_id=subscription_id,
        )

    def get_factories(
        self,
        resource_group: Optional[str] = None,
    ) -> Iterator[Factory]:
        """List all Data Factories.

        API Reference:
        - By subscription: https://learn.microsoft.com/en-us/rest/api/datafactory/factories/list
        - By resource group: https://learn.microsoft.com/en-us/rest/api/datafactory/factories/list-by-resource-group

        Args:
            resource_group: Optional resource group name to filter factories

        Yields:
            Factory objects (SDK model)
        """
        try:
            if resource_group:
                factories_response = self._client.factories.list_by_resource_group(
                    resource_group_name=resource_group
                )
            else:
                factories_response = self._client.factories.list()

            yield from factories_response

        except HttpResponseError as e:
            logger.error(f"Failed to list factories: {e.message}")
            raise

    def get_factory(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Factory:
        """Get a specific Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/factories/get

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Returns:
            Factory object (SDK model)
        """
        factory = self._client.factories.get(
            resource_group_name=resource_group,
            factory_name=factory_name,
        )
        if factory is None:
            raise ValueError(f"Factory not found: {factory_name}")
        return factory

    def get_pipelines(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[PipelineResource]:
        """List all pipelines in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            PipelineResource objects (SDK model)
        """
        try:
            pipelines_response = self._client.pipelines.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )
            yield from pipelines_response

        except HttpResponseError as e:
            logger.error(
                f"Failed to list pipelines for factory {factory_name}: {e.message}"
            )
            raise

    def get_pipeline(
        self,
        resource_group: str,
        factory_name: str,
        pipeline_name: str,
    ) -> PipelineResource:
        """Get a specific pipeline.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/get

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name
            pipeline_name: Pipeline name

        Returns:
            PipelineResource object (SDK model)
        """
        pipeline = self._client.pipelines.get(
            resource_group_name=resource_group,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
        )
        if pipeline is None:
            raise ValueError(f"Pipeline not found: {pipeline_name}")
        return pipeline

    def get_datasets(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[DatasetResource]:
        """List all datasets in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/datasets/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            DatasetResource objects (SDK model)
        """
        try:
            datasets_response = self._client.datasets.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )
            yield from datasets_response

        except HttpResponseError as e:
            logger.error(
                f"Failed to list datasets for factory {factory_name}: {e.message}"
            )
            raise

    def get_linked_services(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[LinkedServiceResource]:
        """List all linked services in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/linked-services/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            LinkedServiceResource objects (SDK model)
        """
        try:
            linked_services_response = self._client.linked_services.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )
            yield from linked_services_response

        except HttpResponseError as e:
            logger.error(
                f"Failed to list linked services for factory {factory_name}: {e.message}"
            )
            raise

    def get_data_flows(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[DataFlowResource]:
        """List all data flows in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/data-flows/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            DataFlowResource objects (SDK model)
        """
        try:
            data_flows_response = self._client.data_flows.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )
            yield from data_flows_response

        except HttpResponseError as e:
            logger.error(
                f"Failed to list data flows for factory {factory_name}: {e.message}"
            )
            raise

    def get_triggers(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[TriggerResource]:
        """List all triggers in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/triggers/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            TriggerResource objects (SDK model)
        """
        try:
            triggers_response = self._client.triggers.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )
            yield from triggers_response

        except HttpResponseError as e:
            logger.error(
                f"Failed to list triggers for factory {factory_name}: {e.message}"
            )
            raise

    def get_pipeline_runs(
        self,
        resource_group: str,
        factory_name: str,
        days: int = 7,
    ) -> Iterator[PipelineRun]:
        """Query pipeline runs for a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/pipeline-runs/query-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name
            days: Number of days of history to fetch

        Yields:
            PipelineRun objects (SDK model)
        """
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=days)

            filter_params = RunFilterParameters(
                last_updated_after=start_time,
                last_updated_before=end_time,
            )

            response: PipelineRunsQueryResponse = (
                self._client.pipeline_runs.query_by_factory(
                    resource_group_name=resource_group,
                    factory_name=factory_name,
                    filter_parameters=filter_params,
                )
            )

            yield from response.value or []

            # Handle pagination via continuation token
            while response.continuation_token:
                filter_params.continuation_token = response.continuation_token
                response = self._client.pipeline_runs.query_by_factory(
                    resource_group_name=resource_group,
                    factory_name=factory_name,
                    filter_parameters=filter_params,
                )
                yield from response.value or []

        except HttpResponseError as e:
            logger.error(
                f"Failed to query pipeline runs for factory {factory_name}: {e.message}"
            )
            raise

    def get_activity_runs(
        self,
        resource_group: str,
        factory_name: str,
        run_id: str,
    ) -> Iterator[ActivityRun]:
        """Query activity runs for a pipeline run.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/activity-runs/query-by-pipeline-run

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name
            run_id: Pipeline run ID

        Yields:
            ActivityRun objects (SDK model)
        """
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=MAX_ACTIVITY_RUN_RETENTION_DAYS)

            filter_params = RunFilterParameters(
                last_updated_after=start_time,
                last_updated_before=end_time,
            )

            response: ActivityRunsQueryResponse = (
                self._client.activity_runs.query_by_pipeline_run(
                    resource_group_name=resource_group,
                    factory_name=factory_name,
                    run_id=run_id,
                    filter_parameters=filter_params,
                )
            )

            yield from response.value or []

            # Handle pagination via continuation token
            while response.continuation_token:
                filter_params.continuation_token = response.continuation_token
                response = self._client.activity_runs.query_by_pipeline_run(
                    resource_group_name=resource_group,
                    factory_name=factory_name,
                    run_id=run_id,
                    filter_parameters=filter_params,
                )
                yield from response.value or []

        except HttpResponseError as e:
            logger.error(
                f"Failed to query activity runs for pipeline run {run_id}: {e.message}"
            )
            raise

    def close(self) -> None:
        """Close the client and release resources."""
        self._client.close()
