"""Shared builders for the Azure Data Factory unit tests.

azure-mgmt-datafactory models are built the way the SDK deserializer builds
them (`hydrate` sets the readonly attributes their constructors refuse), and
`FakeAdfClient` serves them through the `AzureDataFactoryClient` surface the
source calls. `ingest` runs a source the way `Pipeline.run` does, under
ingestion attribution, so SDK entities put descriptions where a real run does.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, TypeVar, cast
from unittest.mock import patch

from azure.mgmt.datafactory import models as adf

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure_data_factory.adf_client import (
    AzureDataFactoryClient,
)
from datahub.ingestion.source.azure_data_factory.adf_config import (
    AzureDataFactoryConfig,
)
from datahub.ingestion.source.azure_data_factory.adf_source import (
    AzureDataFactorySource,
)
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.sdk._attribution import KnownAttribution, change_default_attribution

SUB = "00000000-0000-0000-0000-000000000000"
RG = "my-rg"
FACTORY = "my-factory"

ADF_CLIENT_CLASS = (
    "datahub.ingestion.source.azure_data_factory.adf_source.AzureDataFactoryClient"
)

M = TypeVar("M")


# --- azure-mgmt-datafactory models -----------------------------------------


def raw(value: object) -> Any:
    """ARM returns many properties as untyped JSON (a connection string, a
    table name, a header arrives as a plain string), so the SDK's declared
    types (MutableMapping, Key Vault references) don't describe what a fixture
    must imitate."""
    return value


def hydrate(model: M, **readonly: Any) -> M:
    """Populate readonly SDK attributes (name, id, run fields) the way the
    SDK deserializer does, since their constructors don't accept them."""
    for attribute, value in readonly.items():
        setattr(model, attribute, value)
    return model


def factory_id(name: str, rg: str = RG) -> str:
    return (
        f"/subscriptions/{SUB}/resourceGroups/{rg}"
        f"/providers/Microsoft.DataFactory/factories/{name}"
    )


def adf_factory(name: Optional[str] = FACTORY, rg: str = RG) -> adf.Factory:
    return hydrate(
        adf.Factory(location="westeurope", tags={"team": "data"}),
        name=name,
        id=factory_id(name, rg) if name else None,
        provisioning_state="Succeeded",
    )


def ls_ref(name: str) -> adf.LinkedServiceReference:
    return adf.LinkedServiceReference(
        type="LinkedServiceReference", reference_name=name
    )


def ds_ref(name: str, **parameters: Any) -> adf.DatasetReference:
    return adf.DatasetReference(
        type="DatasetReference", reference_name=name, parameters=parameters or None
    )


def pipeline_ref(name: str) -> adf.PipelineReference:
    return adf.PipelineReference(type="PipelineReference", reference_name=name)


def dataset_resource(name: str, properties: adf.Dataset) -> adf.DatasetResource:
    return hydrate(adf.DatasetResource(properties=properties), name=name)


def linked_service_resource(
    name: str, properties: adf.LinkedService
) -> adf.LinkedServiceResource:
    return hydrate(adf.LinkedServiceResource(properties=properties), name=name)


def pipeline_resource(
    name: Optional[str], activities: List[adf.Activity], **kwargs: Any
) -> adf.PipelineResource:
    return hydrate(
        adf.PipelineResource(activities=activities, **kwargs),
        name=name,
        id=f"{factory_id(FACTORY)}/pipelines/{name}",
    )


def pipeline_run(
    pipeline: Optional[str],
    run_id: Optional[str],
    status: str = "Succeeded",
    parameters: Optional[Dict[str, str]] = None,
    ended: bool = True,
) -> adf.PipelineRun:
    invoked_by = hydrate(
        adf.PipelineRunInvokedBy(),
        name="DailyTrigger",
        invoked_by_type="ScheduleTrigger",
    )
    return hydrate(
        adf.PipelineRun(),
        pipeline_name=pipeline,
        run_id=run_id,
        status=status,
        message="m" * 600,
        parameters=parameters,
        run_start=datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc),
        run_end=datetime(2024, 1, 1, 9, 0, tzinfo=timezone.utc) if ended else None,
        invoked_by=invoked_by,
    )


def activity_run(
    activity: Optional[str],
    run_id: str,
    pipeline: str = "MainPipeline",
    pipeline_run_id: str = "run-1",
    activity_type: str = "Copy",
    status: str = "Succeeded",
    run_input: Optional[Dict[str, Any]] = None,
    error: Optional[Dict[str, Any]] = None,
) -> adf.ActivityRun:
    return hydrate(
        adf.ActivityRun(),
        activity_name=activity,
        activity_run_id=run_id,
        pipeline_name=pipeline,
        pipeline_run_id=pipeline_run_id,
        activity_type=activity_type,
        status=status,
        duration_in_ms=1200,
        activity_run_start=datetime(2024, 1, 1, 8, 1, tzinfo=timezone.utc),
        activity_run_end=datetime(2024, 1, 1, 8, 2, tzinfo=timezone.utc),
        input=run_input,
        error=error,
    )


# --- the client the source reads through -------------------------------------


class FakeAdfClient:
    """One factory's resources, served through the AzureDataFactoryClient
    surface the source calls. A method named in `errors` raises instead."""

    def __init__(self, factories: Optional[List[adf.Factory]] = None) -> None:
        self.factories = factories if factories is not None else [adf_factory()]
        self.datasets: List[adf.DatasetResource] = []
        self.linked_services: List[adf.LinkedServiceResource] = []
        self.global_parameters: List[adf.GlobalParameterResource] = []
        self.triggers: List[adf.TriggerResource] = []
        self.data_flows: List[adf.DataFlowResource] = []
        self.pipelines: List[adf.PipelineResource] = []
        self.pipeline_runs: List[adf.PipelineRun] = []
        self.activity_runs: Dict[str, List[adf.ActivityRun]] = {}
        self.errors: Dict[str, Exception] = {}
        self.run_queries: List[str] = []
        self.closed = False

    def _serve(self, method: str, items: List[Any]) -> Iterator[Any]:
        if method in self.errors:
            raise self.errors[method]
        yield from items

    def get_factories(
        self, resource_group: Optional[str] = None
    ) -> Iterator[adf.Factory]:
        return self._serve("get_factories", self.factories)

    def get_datasets(
        self, resource_group: str, factory_name: str
    ) -> Iterator[adf.DatasetResource]:
        return self._serve("get_datasets", self.datasets)

    def get_linked_services(
        self, resource_group: str, factory_name: str
    ) -> Iterator[adf.LinkedServiceResource]:
        return self._serve("get_linked_services", self.linked_services)

    def get_global_parameters(
        self, resource_group: str, factory_name: str
    ) -> Iterator[adf.GlobalParameterResource]:
        return self._serve("get_global_parameters", self.global_parameters)

    def get_triggers(
        self, resource_group: str, factory_name: str
    ) -> Iterator[adf.TriggerResource]:
        return self._serve("get_triggers", self.triggers)

    def get_data_flows(
        self, resource_group: str, factory_name: str
    ) -> Iterator[adf.DataFlowResource]:
        return self._serve("get_data_flows", self.data_flows)

    def get_pipelines(
        self, resource_group: str, factory_name: str
    ) -> Iterator[adf.PipelineResource]:
        return self._serve("get_pipelines", self.pipelines)

    def get_pipeline_runs(
        self, resource_group: str, factory_name: str, days: int = 7
    ) -> Iterator[adf.PipelineRun]:
        return self._serve("get_pipeline_runs", self.pipeline_runs)

    def get_activity_runs(
        self, resource_group: str, factory_name: str, run_id: str
    ) -> Iterator[adf.ActivityRun]:
        self.run_queries.append(run_id)
        return self._serve("get_activity_runs", self.activity_runs.get(run_id, []))

    def close(self) -> None:
        self.closed = True


# --- sources -------------------------------------------------------------------


def build_adf_source(
    client: Optional[FakeAdfClient] = None, **recipe: Any
) -> AzureDataFactorySource:
    """A source over `client`, or over an autospecced AzureDataFactoryClient
    when none is given (for tests that drive one method directly)."""
    config = AzureDataFactoryConfig.model_validate(
        {
            "subscription_id": SUB,
            "credential": {"authentication_method": "cli"},
            **recipe,
        }
    )
    with patch(ADF_CLIENT_CLASS, autospec=True):
        source = AzureDataFactorySource(config, PipelineContext(run_id="adf-test"))
    if client is not None:
        source.client = cast(AzureDataFactoryClient, client)
    return source


def ingest(source: AzureDataFactorySource) -> List[MetadataWorkUnit]:
    with change_default_attribution(KnownAttribution.INGESTION):
        return list(source.get_workunits_internal())


# --- what DataHub already knows --------------------------------------------------


def schema_metadata(
    columns: List[str], platform: str = "urn:li:dataPlatform:mssql"
) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="t",
        platform=platform,
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=column,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="varchar",
            )
            for column in columns
        ],
    )
