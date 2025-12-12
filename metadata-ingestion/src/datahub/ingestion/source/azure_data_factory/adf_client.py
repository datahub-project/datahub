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
    ActivityRunsQueryResponse,
    PipelineRunsQueryResponse,
    RunFilterParameters,
)

from datahub.ingestion.source.azure_data_factory.adf_models import (
    ActivityRun,
    DataFlow,
    Dataset,
    Factory,
    LinkedService,
    Pipeline,
    PipelineRun,
    Trigger,
)

logger = logging.getLogger(__name__)

# Maximum retention period for activity run queries (Azure limit)
MAX_ACTIVITY_RUN_RETENTION_DAYS = 90


class AzureDataFactoryClient:
    """Client for Azure Data Factory REST API.

    Uses the Azure SDK (azure-mgmt-datafactory) for type safety and
    automatic pagination handling.

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
            Factory objects
        """
        try:
            if resource_group:
                # GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories
                # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/factories/list-by-resource-group
                factories_response = self._client.factories.list_by_resource_group(
                    resource_group_name=resource_group
                )
            else:
                # GET /subscriptions/{sub}/providers/Microsoft.DataFactory/factories
                # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/factories/list
                factories_response = self._client.factories.list()

            for factory in factories_response:
                yield Factory.model_validate(factory.as_dict())

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
            Factory object
        """
        # GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}
        # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/factories/get
        factory = self._client.factories.get(
            resource_group_name=resource_group,
            factory_name=factory_name,
        )
        if factory is None:
            raise ValueError(f"Factory not found: {factory_name}")
        return Factory.model_validate(factory.as_dict())

    def get_pipelines(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[Pipeline]:
        """List all pipelines in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            Pipeline objects with activities
        """
        try:
            # GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines
            # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/list-by-factory
            pipelines_response = self._client.pipelines.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )

            for pipeline in pipelines_response:
                yield Pipeline.model_validate(pipeline.as_dict())

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
    ) -> Pipeline:
        """Get a specific pipeline.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/get

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name
            pipeline_name: Pipeline name

        Returns:
            Pipeline object with activities
        """
        # GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelines/{pipelineName}
        # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/get
        pipeline = self._client.pipelines.get(
            resource_group_name=resource_group,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
        )
        if pipeline is None:
            raise ValueError(f"Pipeline not found: {pipeline_name}")
        return Pipeline.model_validate(pipeline.as_dict())

    def get_datasets(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[Dataset]:
        """List all datasets in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/datasets/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            Dataset objects
        """
        try:
            # GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}/datasets
            # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/datasets/list-by-factory
            datasets_response = self._client.datasets.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )

            for dataset in datasets_response:
                yield Dataset.model_validate(dataset.as_dict())

        except HttpResponseError as e:
            logger.error(
                f"Failed to list datasets for factory {factory_name}: {e.message}"
            )
            raise

    def get_linked_services(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[LinkedService]:
        """List all linked services in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/linked-services/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            LinkedService objects
        """
        try:
            # GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}/linkedservices
            # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/linked-services/list-by-factory
            linked_services_response = self._client.linked_services.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )

            for linked_service in linked_services_response:
                yield LinkedService.model_validate(linked_service.as_dict())

        except HttpResponseError as e:
            logger.error(
                f"Failed to list linked services for factory {factory_name}: {e.message}"
            )
            raise

    def get_data_flows(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[DataFlow]:
        """List all data flows in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/data-flows/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            DataFlow objects
        """
        try:
            # GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}/dataflows
            # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/data-flows/list-by-factory
            data_flows_response = self._client.data_flows.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )

            for data_flow in data_flows_response:
                yield DataFlow.model_validate(data_flow.as_dict())

        except HttpResponseError as e:
            logger.error(
                f"Failed to list data flows for factory {factory_name}: {e.message}"
            )
            raise

    def get_triggers(
        self,
        resource_group: str,
        factory_name: str,
    ) -> Iterator[Trigger]:
        """List all triggers in a Data Factory.

        API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/triggers/list-by-factory

        Args:
            resource_group: Resource group name
            factory_name: Data Factory name

        Yields:
            Trigger objects
        """
        try:
            # GET /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}/triggers
            # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/triggers/list-by-factory
            triggers_response = self._client.triggers.list_by_factory(
                resource_group_name=resource_group,
                factory_name=factory_name,
            )

            for trigger in triggers_response:
                yield Trigger.model_validate(trigger.as_dict())

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
            PipelineRun objects
        """
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=days)

            # POST /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}/queryPipelineRuns
            # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/pipeline-runs/query-by-factory
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

            for run in response.value or []:
                yield PipelineRun.model_validate(run.as_dict())

            # Handle pagination via continuation token
            while response.continuation_token:
                filter_params.continuation_token = response.continuation_token
                response = self._client.pipeline_runs.query_by_factory(
                    resource_group_name=resource_group,
                    factory_name=factory_name,
                    filter_parameters=filter_params,
                )
                for run in response.value or []:
                    yield PipelineRun.model_validate(run.as_dict())

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
            ActivityRun objects
        """
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=MAX_ACTIVITY_RUN_RETENTION_DAYS)

            # POST /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{factoryName}/pipelineruns/{runId}/queryActivityruns
            # Docs: https://learn.microsoft.com/en-us/rest/api/datafactory/activity-runs/query-by-pipeline-run
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

            for run in response.value or []:
                yield ActivityRun.model_validate(run.as_dict())

            # Handle pagination via continuation token
            while response.continuation_token:
                filter_params.continuation_token = response.continuation_token
                response = self._client.activity_runs.query_by_pipeline_run(
                    resource_group_name=resource_group,
                    factory_name=factory_name,
                    run_id=run_id,
                    filter_parameters=filter_params,
                )
                for run in response.value or []:
                    yield ActivityRun.model_validate(run.as_dict())

        except HttpResponseError as e:
            logger.error(
                f"Failed to query activity runs for pipeline run {run_id}: {e.message}"
            )
            raise

    def close(self) -> None:
        """Close the client and release resources."""
        self._client.close()
