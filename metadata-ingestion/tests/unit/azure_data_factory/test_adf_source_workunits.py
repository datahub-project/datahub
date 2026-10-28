"""AzureDataFactorySource end to end over a fake ADF client: what one factory's
pipelines, activities and execution history become as workunits, and how a
failing Azure call degrades."""

from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Type, TypeVar, cast
from unittest.mock import MagicMock, patch

from azure.mgmt.datafactory import models as adf

from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    ContainerPropertiesClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataProcessInstanceInputClass,
    DataProcessInstanceOutputClass,
    DataProcessInstancePropertiesClass,
    DataProcessInstanceRelationshipsClass,
    DataProcessInstanceRunEventClass,
    DataTransformLogicClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    _Aspect,
)
from datahub.metadata.urns import DataFlowUrn, DataJobUrn

SUB = "00000000-0000-0000-0000-000000000000"
RG = "my-rg"
FACTORY = "my-factory"

SQL_LS = "SqlLs"
BLOB_LS = "BlobLs"
HTTP_LS = "HttpLs"

MSSQL = "urn:li:dataPlatform:mssql"
ORDERS = f"urn:li:dataset:({MSSQL},salesdb.dbo.orders,PROD)"
STG_ORDERS = f"urn:li:dataset:({MSSQL},salesdb.stg.orders,PROD)"
CUSTOMERS = f"urn:li:dataset:({MSSQL},salesdb.dbo.customers,PROD)"
STG_CUSTOMERS = f"urn:li:dataset:({MSSQL},salesdb.stg.customers,PROD)"
ORDERS_CSV = "urn:li:dataset:(urn:li:dataPlatform:abs,raw/orders/orders.csv,PROD)"
# A dataset whose table name is dynamic falls back to the ADF dataset name.
PARAM_PLACEHOLDER = f"urn:li:dataset:({MSSQL},ParamDs,PROD)"

T = TypeVar("T", bound=_Aspect)
M = TypeVar("M")


def _raw(value: object) -> Any:
    # ARM returns these properties as untyped JSON -- a connection string or a
    # table name arrives as a plain string -- so the SDK's declared types
    # (MutableMapping) don't describe what this fixture must imitate.
    return value


def _hydrate(model: M, **readonly: Any) -> M:
    """Populate readonly SDK attributes (name, id, run fields) the way the
    SDK deserializer does, since their constructors don't accept them."""
    for attribute, value in readonly.items():
        setattr(model, attribute, value)
    return model


def _factory_id(name: str, rg: str = RG) -> str:
    return (
        f"/subscriptions/{SUB}/resourceGroups/{rg}"
        f"/providers/Microsoft.DataFactory/factories/{name}"
    )


def _factory(name: Optional[str] = FACTORY, rg: str = RG) -> adf.Factory:
    return _hydrate(
        adf.Factory(location="westeurope", tags={"team": "data"}),
        name=name,
        id=_factory_id(name, rg) if name else None,
        provisioning_state="Succeeded",
    )


def _ls_ref(name: str) -> adf.LinkedServiceReference:
    return adf.LinkedServiceReference(
        type="LinkedServiceReference", reference_name=name
    )


def _ds_ref(name: str, **parameters: Any) -> adf.DatasetReference:
    return adf.DatasetReference(
        type="DatasetReference", reference_name=name, parameters=parameters or None
    )


def _pipeline_ref(name: str) -> adf.PipelineReference:
    return adf.PipelineReference(type="PipelineReference", reference_name=name)


def _dataset(name: str, properties: adf.Dataset) -> adf.DatasetResource:
    return _hydrate(adf.DatasetResource(properties=properties), name=name)


def _sql_table(
    name: str, sql_schema: Optional[str], table: Any, **kwargs: Any
) -> adf.DatasetResource:
    return _dataset(
        name,
        adf.AzureSqlTableDataset(
            linked_service_name=_ls_ref(SQL_LS),
            schema_type_properties_schema=_raw(sql_schema),
            table=_raw(table),
            **kwargs,
        ),
    )


def _linked_service(
    name: str, properties: adf.LinkedService
) -> adf.LinkedServiceResource:
    return _hydrate(adf.LinkedServiceResource(properties=properties), name=name)


def _pipeline(
    name: Optional[str], activities: List[adf.Activity], **kwargs: Any
) -> adf.PipelineResource:
    return _hydrate(
        adf.PipelineResource(activities=activities, **kwargs),
        name=name,
        id=f"{_factory_id(FACTORY)}/pipelines/{name}",
    )


def _copy(
    name: str,
    source: str,
    sink: str,
    translator: Optional[Dict[str, Any]] = None,
    source_params: Optional[Dict[str, Any]] = None,
) -> adf.CopyActivity:
    return adf.CopyActivity(
        name=name,
        source=adf.AzureSqlSource(),
        sink=adf.AzureSqlSink(),
        inputs=[_ds_ref(source, **(source_params or {}))],
        outputs=[_ds_ref(sink)],
        translator=translator,
    )


def _wait(name: str) -> adf.WaitActivity:
    return adf.WaitActivity(name=name, wait_time_in_seconds=_raw(1))


def _execute_data_flow(name: str, data_flow: Optional[str]) -> adf.Activity:
    activity = adf.ExecuteDataFlowActivity(
        name=name,
        data_flow=adf.DataFlowReference(
            type="DataFlowReference", reference_name=data_flow or "unused"
        ),
    )
    if data_flow is None:
        # The SDK drops the reference when ADF returns "dataflow" in a
        # different case than the SDK expects; the source then matches on
        # the activity name.
        _hydrate(activity, data_flow=None)
    return activity


def _mapping_data_flow(
    name: str,
    sources: List[str],
    sinks: List[str],
    script_lines: Optional[List[str]] = None,
    script: Optional[str] = None,
) -> adf.DataFlowResource:
    resource = adf.DataFlowResource(
        properties=adf.MappingDataFlow(
            sources=[
                adf.DataFlowSource(name=f"src_{s}", dataset=_ds_ref(s)) for s in sources
            ],
            sinks=[
                adf.DataFlowSink(name=f"snk_{s}", dataset=_ds_ref(s)) for s in sinks
            ],
            script_lines=script_lines,
            script=script,
        )
    )
    return _hydrate(resource, name=name)


def _schedule_trigger(name: str, pipelines: List[str]) -> adf.TriggerResource:
    return _hydrate(
        adf.TriggerResource(
            properties=adf.ScheduleTrigger(
                recurrence=adf.ScheduleTriggerRecurrence(frequency="Day", interval=1),
                pipelines=[
                    adf.TriggerPipelineReference(pipeline_reference=_pipeline_ref(p))
                    for p in pipelines
                ],
            )
        ),
        name=name,
    )


def _global_parameters(**values: str) -> adf.GlobalParameterResource:
    return _hydrate(
        adf.GlobalParameterResource(
            properties={
                k: adf.GlobalParameterSpecification(type="String", value=_raw(v))
                for k, v in values.items()
            }
        ),
        name="default",
    )


def _pipeline_run(
    pipeline: Optional[str],
    run_id: Optional[str],
    status: str = "Succeeded",
    parameters: Optional[Dict[str, str]] = None,
    ended: bool = True,
) -> adf.PipelineRun:
    invoked_by = _hydrate(
        adf.PipelineRunInvokedBy(),
        name="DailyTrigger",
        invoked_by_type="ScheduleTrigger",
    )
    return _hydrate(
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


def _activity_run(
    activity: Optional[str],
    run_id: str,
    pipeline: str = "MainPipeline",
    pipeline_run_id: str = "run-1",
    activity_type: str = "Copy",
    status: str = "Succeeded",
    run_input: Optional[Dict[str, Any]] = None,
    error: Optional[Dict[str, Any]] = None,
) -> adf.ActivityRun:
    return _hydrate(
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


class FakeAdfClient:
    """One factory's resources, served through the AzureDataFactoryClient
    surface the source calls. A method named in `errors` raises instead."""

    def __init__(self, factories: Optional[List[adf.Factory]] = None) -> None:
        self.factories = factories if factories is not None else [_factory()]
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


def _source(client: FakeAdfClient, **overrides: Any) -> AzureDataFactorySource:
    config = AzureDataFactoryConfig.model_validate(
        {
            "subscription_id": SUB,
            "credential": {"authentication_method": "cli"},
            **overrides,
        }
    )
    with patch(
        "datahub.ingestion.source.azure_data_factory.adf_source.AzureDataFactoryClient"
    ):
        source = AzureDataFactorySource(config, PipelineContext(run_id="adf-test"))
    source.client = cast(AzureDataFactoryClient, client)
    return source


def _tenant() -> FakeAdfClient:
    """A factory with the activity shapes lineage is extracted from."""
    client = FakeAdfClient()
    client.linked_services = [
        _linked_service(
            SQL_LS,
            adf.AzureSqlDatabaseLinkedService(
                connection_string=_raw(
                    "Server=tcp:placeholder;Initial Catalog=salesdb;"
                )
            ),
        ),
        _linked_service(
            BLOB_LS,
            adf.AzureBlobStorageLinkedService(
                connection_string=_raw(
                    "DefaultEndpointsProtocol=https;AccountName=acct"
                )
            ),
        ),
        # HttpServer has no DataHub platform mapping.
        _linked_service(
            HTTP_LS, adf.HttpLinkedService(url=_raw("https://placeholder"))
        ),
    ]
    client.datasets = [
        _sql_table(
            "SrcOrders", "dbo", "orders", schema=[{"name": "id"}, {"name": "amount"}]
        ),
        _sql_table("StgOrders", "stg", "orders"),
        _sql_table(
            "ParamDs",
            None,
            {"value": "@dataset().tbl", "type": "Expression"},
            parameters={"tbl": adf.ParameterSpecification(type="String")},
        ),
        _dataset(
            "OrdersCsv",
            adf.DelimitedTextDataset(
                linked_service_name=_ls_ref(BLOB_LS),
                location=adf.AzureBlobStorageLocation(
                    container=_raw("raw"),
                    folder_path=_raw("orders"),
                    file_name=_raw("orders.csv"),
                ),
            ),
        ),
        _dataset("HttpDs", adf.HttpDataset(linked_service_name=_ls_ref(HTTP_LS))),
    ]
    client.global_parameters = [_global_parameters(env_name="prod")]
    client.triggers = [
        _schedule_trigger("DailyTrigger", ["MainPipeline"]),
        # A trigger kind without pipelines (e.g. a tumbling window) must
        # not break trigger lookup.
        adf.TriggerResource(properties=adf.Trigger()),
    ]
    client.data_flows = [
        _mapping_data_flow(
            "TransformOrders",
            ["SrcOrders"],
            ["StgOrders"],
            script_lines=["source()", "sink()"],
        ),
        _mapping_data_flow("Legacy Flow", ["SrcOrders"], [], script="source()"),
        _mapping_data_flow("Scriptless", [], []),
    ]
    client.pipelines = [
        _pipeline(
            "MainPipeline",
            [
                _copy("CopyOrders", "SrcOrders", "OrdersCsv"),
                _execute_data_flow("RunTransform", "TransformOrders"),
                adf.ExecutePipelineActivity(
                    name="CallChild", pipeline=_pipeline_ref("ChildPipeline")
                ),
                adf.LookupActivity(
                    name="LookupConfig",
                    source=adf.AzureSqlSource(),
                    dataset=_ds_ref("SrcOrders"),
                ),
                adf.ForEachActivity(
                    name="LoopTables",
                    items=adf.Expression(
                        type="Expression", value="@pipeline().parameters.tables"
                    ),
                    activities=[_copy("CopyEach", "SrcOrders", "StgOrders")],
                ),
                _copy("CopyHttp", "HttpDs", "StgOrders"),
                _wait("Pause"),
            ],
            description="Nightly load",
            folder=adf.PipelineFolder(name="etl"),
            annotations=["nightly"],
            concurrency=2,
        ),
        _pipeline("ChildPipeline", [_wait("ChildStart")]),
        _pipeline(
            "LegacyPipeline",
            [
                _execute_data_flow("Legacy Flow", None),
                _execute_data_flow("LEGACYFLOW", None),
            ],
        ),
        _pipeline("ScratchPipeline", [_wait("Nothing")]),
        _pipeline(None, [_wait("Unnamed")]),
    ]
    return client


def _aspects(
    workunits: List[MetadataWorkUnit], aspect_type: Type[T]
) -> Dict[str, List[T]]:
    found: Dict[str, List[T]] = {}
    for wu in workunits:
        metadata = wu.metadata
        if isinstance(metadata, MetadataChangeProposalWrapper) and isinstance(
            metadata.aspect, aspect_type
        ):
            assert metadata.entityUrn is not None
            found.setdefault(metadata.entityUrn, []).append(metadata.aspect)
    return found


def _last(workunits: List[MetadataWorkUnit], aspect_type: Type[T]) -> Dict[str, T]:
    return {
        urn: aspects[-1] for urn, aspects in _aspects(workunits, aspect_type).items()
    }


def _flow_urn(pipeline: str) -> str:
    return str(
        DataFlowUrn.create_from_ids(
            orchestrator="azure-data-factory",
            flow_id=f"{FACTORY}.{pipeline}",
            env="PROD",
        )
    )


def _job_urn(pipeline: str, activity: str) -> str:
    return str(
        DataJobUrn.create_from_ids(data_flow_urn=_flow_urn(pipeline), job_id=activity)
    )


def _run_urns(workunits: List[MetadataWorkUnit], key: str) -> Dict[str, str]:
    """DataProcessInstance URNs are hashed, so runs are found by the run id
    (`run_id` or `activity_run_id`) recorded on their properties."""
    return {
        props.customProperties[key]: urn
        for urn, props in _last(workunits, DataProcessInstancePropertiesClass).items()
        if key in props.customProperties
    }


def _script(logic: DataTransformLogicClass) -> Optional[str]:
    statement = logic.transforms[0].queryStatement
    return statement.value if statement is not None else None


def _edge(fgl: Any) -> tuple[str, str]:
    assert fgl.upstreams and fgl.downstreams
    return fgl.upstreams[0], fgl.downstreams[0]


def _warning_titles(source: AzureDataFactorySource) -> List[Optional[str]]:
    return [w.title for w in source.report.warnings]


def _ingest(
    client: FakeAdfClient, **overrides: Any
) -> tuple[AzureDataFactorySource, List[MetadataWorkUnit]]:
    source = _source(client, pipeline_pattern={"deny": ["Scratch.*"]}, **overrides)
    return source, list(source.get_workunits_internal())


class TestStaticMetadata:
    def test_factory_becomes_a_container_with_its_azure_properties(self) -> None:
        _, workunits = _ingest(_tenant())

        containers = list(_last(workunits, ContainerPropertiesClass).values())
        assert len(containers) == 1
        assert containers[0].name == FACTORY
        assert (
            containers[0].customProperties.items()
            >= {
                "azure_resource_id": _factory_id(FACTORY),
                "location": "westeurope",
                "tag:team": "data",
                "provisioning_state": "Succeeded",
            }.items()
        )

    def test_pipelines_become_dataflows_carrying_folder_annotations_and_triggers(
        self,
    ) -> None:
        source, workunits = _ingest(_tenant())

        flows = _last(workunits, DataFlowInfoClass)
        main = flows[_flow_urn("MainPipeline")]
        assert main.name == "MainPipeline"
        assert main.customProperties["folder"] == "etl"
        assert main.customProperties["annotations"] == "nightly"
        assert main.customProperties["concurrency"] == "2"
        assert main.customProperties["triggers"] == "DailyTrigger"
        assert "triggers" not in flows[_flow_urn("ChildPipeline")].customProperties
        # Denied by pipeline_pattern, and the nameless pipeline is skipped.
        assert _flow_urn("ScratchPipeline") not in flows
        assert list(source.report.filtered_pipelines) == ["ScratchPipeline"]
        assert source.report.pipelines_scanned == 3

    def test_copy_activity_lineage_resolves_tables_through_linked_services(
        self,
    ) -> None:
        """The SQL dataset is qualified with the catalog parsed from its
        linked service's connection string; the blob dataset becomes a path."""
        _, workunits = _ingest(_tenant())

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("MainPipeline", "CopyOrders")
        ]
        assert io.inputDatasets == [ORDERS]
        assert io.outputDatasets == [ORDERS_CSV]

    def test_copy_without_a_translator_maps_the_adf_declared_columns_by_name(
        self,
    ) -> None:
        _, workunits = _ingest(_tenant())

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("MainPipeline", "CopyOrders")
        ]
        assert io.fineGrainedLineages is not None
        assert {
            (tuple(fgl.upstreams or ()), tuple(fgl.downstreams or ()))
            for fgl in io.fineGrainedLineages
        } == {
            (
                (f"urn:li:schemaField:({ORDERS},{col})",),
                (f"urn:li:schemaField:({ORDERS_CSV},{col})",),
            )
            for col in ("id", "amount")
        }

    def test_nested_activities_get_datajobs_with_lineage(self) -> None:
        _, workunits = _ingest(_tenant())

        jobs = _last(workunits, DataJobInfoClass)
        assert _job_urn("MainPipeline", "LoopTables") in jobs
        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("MainPipeline", "CopyEach")
        ]
        assert (io.inputDatasets, io.outputDatasets) == ([ORDERS], [STG_ORDERS])

    def test_lookup_activity_reads_its_dataset(self) -> None:
        _, workunits = _ingest(_tenant())

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("MainPipeline", "LookupConfig")
        ]
        assert io.inputDatasets == [ORDERS]
        assert io.outputDatasets == []

    def test_execute_data_flow_reads_the_flows_endpoints_and_emits_its_script(
        self,
    ) -> None:
        _, workunits = _ingest(_tenant())

        job = _job_urn("MainPipeline", "RunTransform")
        io = _last(workunits, DataJobInputOutputClass)[job]
        assert (io.inputDatasets, io.outputDatasets) == ([ORDERS], [STG_ORDERS])
        assert (
            _script(_last(workunits, DataTransformLogicClass)[job])
            == "source()\nsink()"
        )

    def test_data_flow_is_found_by_activity_name_when_the_sdk_drops_the_reference(
        self,
    ) -> None:
        """Both an exact and a case/space-insensitive activity name match
        the data flow; the fallback script field is emitted too."""
        _, workunits = _ingest(_tenant())

        io = _last(workunits, DataJobInputOutputClass)
        logic = _last(workunits, DataTransformLogicClass)
        for activity in ("Legacy Flow", "LEGACYFLOW"):
            job = _job_urn("LegacyPipeline", activity)
            assert io[job].inputDatasets == [ORDERS]
            assert _script(logic[job]) == "source()"

    def test_execute_pipeline_links_the_caller_to_the_childs_first_activity(
        self,
    ) -> None:
        source, workunits = _ingest(_tenant())

        caller = _job_urn("MainPipeline", "CallChild")
        props = _last(workunits, DataJobInfoClass)[caller].customProperties
        assert props["calls_pipeline"] == "ChildPipeline"
        assert props["child_pipeline_urn"] == _flow_urn("ChildPipeline")
        assert props["child_first_activity"] == "ChildStart"
        child_io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("ChildPipeline", "ChildStart")
        ]
        assert child_io.inputDatajobs == [caller]
        assert source.report.pipeline_lineage_extracted == 1

    def test_unmapped_linked_service_type_skips_lineage_with_a_warning(self) -> None:
        source, workunits = _ingest(_tenant())

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("MainPipeline", "CopyHttp")
        ]
        assert io.inputDatasets == []
        assert io.outputDatasets == [STG_ORDERS]
        assert "Unmapped Linked Service Type" in _warning_titles(source)

    def test_activity_without_lineage_still_gets_a_typed_datajob(self) -> None:
        _, workunits = _ingest(_tenant())

        job = _last(workunits, DataJobInfoClass)[_job_urn("MainPipeline", "Pause")]
        assert job.customProperties["activity_type"] == "Wait"
        assert _job_urn("MainPipeline", "Pause") not in _last(
            workunits, DataJobInputOutputClass
        )

    def test_factory_pattern_and_nameless_factories_are_skipped(self) -> None:
        client = _tenant()
        client.factories = [_factory(), _factory("other-factory"), _factory(None)]
        source, workunits = _ingest(client, factory_pattern={"deny": ["other.*"]})

        containers = _last(workunits, ContainerPropertiesClass)
        assert [c.name for c in containers.values()] == [FACTORY]
        assert list(source.report.filtered_factories) == ["other-factory"]
        assert source.report.factories_scanned == 1

    def test_global_parameters_resolve_dataset_references(self) -> None:
        client = _tenant()
        client.datasets.append(
            _sql_table(
                "GlobalDs",
                "dbo",
                {"value": "@dataset().tbl", "type": "Expression"},
                parameters={
                    "tbl": adf.ParameterSpecification(
                        type="String",
                        default_value=_raw("@pipeline().globalParameters.env_name"),
                    )
                },
            )
        )
        client.pipelines.append(
            _pipeline("GlobalPipeline", [_copy("CopyGlobal", "GlobalDs", "StgOrders")])
        )
        _, workunits = _ingest(client)

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("GlobalPipeline", "CopyGlobal")
        ]
        assert io.inputDatasets == [f"urn:li:dataset:({MSSQL},salesdb.dbo.prod,PROD)"]

    def test_close_releases_the_client(self) -> None:
        client = _tenant()
        source = _source(client)
        source.close()
        assert client.closed

    def test_create_builds_from_a_recipe_dict(self) -> None:
        with patch(
            "datahub.ingestion.source.azure_data_factory.adf_source.AzureDataFactoryClient"
        ):
            source = AzureDataFactorySource.create(
                {
                    "subscription_id": SUB,
                    "credential": {"authentication_method": "cli"},
                },
                PipelineContext(run_id="adf-test"),
            )
        assert source.config.subscription_id == SUB


class TestLineageShapes:
    def test_legacy_blob_dataset_combines_folder_and_file(self) -> None:
        client = _tenant()
        client.datasets.append(
            _dataset(
                "LegacyBlob",
                adf.AzureBlobDataset(
                    linked_service_name=_ls_ref(BLOB_LS),
                    folder_path=_raw("raw/legacy"),
                    file_name=_raw("orders.csv"),
                ),
            )
        )
        client.pipelines.append(
            _pipeline(
                "LegacyBlobPipeline", [_copy("CopyLegacy", "SrcOrders", "LegacyBlob")]
            )
        )
        _, workunits = _ingest(client)

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("LegacyBlobPipeline", "CopyLegacy")
        ]
        assert io.outputDatasets == [
            "urn:li:dataset:(urn:li:dataPlatform:abs,raw/legacy/orders.csv,PROD)"
        ]

    def test_execute_data_flow_referencing_a_missing_flow_has_no_lineage(self) -> None:
        client = _tenant()
        client.pipelines.append(
            _pipeline(
                "MissingFlowPipeline", [_execute_data_flow("RunGone", "GoneFlow")]
            )
        )
        _, workunits = _ingest(client)

        job = _job_urn("MissingFlowPipeline", "RunGone")
        assert job in _last(workunits, DataJobInfoClass)
        assert job not in _last(workunits, DataJobInputOutputClass)
        assert job not in _last(workunits, DataTransformLogicClass)

    def test_execute_pipeline_to_an_unknown_child_records_the_call_without_an_edge(
        self,
    ) -> None:
        client = _tenant()
        client.pipelines.append(
            _pipeline(
                "CallerPipeline",
                [
                    adf.ExecutePipelineActivity(
                        name="CallGone", pipeline=_pipeline_ref("GonePipeline")
                    )
                ],
            )
        )
        _, workunits = _ingest(client)

        props = _last(workunits, DataJobInfoClass)[
            _job_urn("CallerPipeline", "CallGone")
        ].customProperties
        assert props["calls_pipeline"] == "GonePipeline"
        assert "child_first_activity" not in props
        assert not any(
            urn.startswith(f"urn:li:dataJob:({_flow_urn('GonePipeline')}")
            for urn in _last(workunits, DataJobInputOutputClass)
        )


class TestAzureFailures:
    def test_listing_factories_failure_is_a_failure_with_no_workunits(self) -> None:
        client = _tenant()
        client.errors["get_factories"] = RuntimeError("401")
        source, workunits = _ingest(client)

        assert workunits == []
        assert [f.title for f in source.report.failures] == [
            "Failed to List Data Factories"
        ]

    def test_listing_pipelines_failure_keeps_the_factory_container(self) -> None:
        client = _tenant()
        client.errors["get_pipelines"] = RuntimeError("403")
        source, workunits = _ingest(client)

        assert len(_last(workunits, ContainerPropertiesClass)) == 1
        assert _last(workunits, DataFlowInfoClass) == {}
        assert _warning_titles(source) == ["Failed to List Pipelines"]

    def test_global_parameters_failure_does_not_skip_the_factory(self) -> None:
        client = _tenant()
        client.errors["get_global_parameters"] = RuntimeError("403")
        source, workunits = _ingest(client)

        assert _flow_urn("MainPipeline") in _last(workunits, DataFlowInfoClass)
        assert "Failed to List Global Parameters" in _warning_titles(source)

    def test_a_failing_factory_is_skipped_and_the_next_one_processed(self) -> None:
        client = _tenant()
        client.factories = [_factory(), _factory("second")]
        calls = 0

        def fail_once(*args: Any) -> Iterator[adf.DatasetResource]:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("transient")
            return iter([])

        with patch.object(client, "get_datasets", side_effect=fail_once):
            source, workunits = _ingest(client)

        names = [c.name for c in _last(workunits, ContainerPropertiesClass).values()]
        assert names == ["second"]
        assert "Failed to Process Data Factory" in _warning_titles(source)
        assert source.report.factories_scanned == 2

    def test_unparseable_factory_id_falls_back_to_an_unknown_resource_group(
        self,
    ) -> None:
        client = _tenant()
        broken = _hydrate(
            _factory(),
            id="/subscriptions/x/providers/Microsoft.DataFactory/factories/y",
        )
        client.factories = [broken]
        _, workunits = _ingest(client)

        container = next(iter(_last(workunits, ContainerPropertiesClass).values()))
        assert container.externalUrl is not None
        assert "/resourceGroups/unknown/" in container.externalUrl


def _history_client() -> FakeAdfClient:
    client = _tenant()
    main_activities = client.pipelines[0].activities
    assert main_activities is not None
    main_activities.extend(
        [
            _copy(
                "CopyParam",
                "ParamDs",
                "StgOrders",
                source_params={"tbl": "@pipeline().parameters.SourceTable"},
            ),
            _copy(
                "CopyMapped",
                "SrcOrders",
                "StgOrders",
                translator={
                    "type": "Expression",
                    "value": "@activity('Lookup').output.firstRow.mapping",
                },
            ),
        ]
    )
    client.pipeline_runs = [
        _pipeline_run(
            "MainPipeline", "run-1", parameters={"SourceTable": "dbo.customers"}
        ),
        _pipeline_run("MainPipeline", "run-2", status="InProgress", ended=False),
        _pipeline_run("ScratchPipeline", "run-3"),
        _pipeline_run("MainPipeline", None),
    ]
    return client


def _history(
    client: FakeAdfClient, **overrides: Any
) -> tuple[AzureDataFactorySource, List[MetadataWorkUnit]]:
    return _ingest(
        client, include_execution_history=True, execution_history_days=3, **overrides
    )


class TestExecutionHistory:
    def test_pipeline_runs_become_process_instances_with_events(self) -> None:
        client = _history_client()
        source, workunits = _history(client)

        props = _last(workunits, DataProcessInstancePropertiesClass)
        runs = _run_urns(workunits, "run_id")
        run_urn = runs["run-1"]
        assert props[run_urn].customProperties["status"] == "Succeeded"
        assert props[run_urn].customProperties["invoked_by"] == "DailyTrigger"
        assert props[run_urn].customProperties["invoked_by_type"] == "ScheduleTrigger"
        assert props[run_urn].customProperties["param:SourceTable"] == "dbo.customers"
        assert len(props[run_urn].customProperties["message"]) == 500
        relationships = _last(workunits, DataProcessInstanceRelationshipsClass)
        assert relationships[run_urn].parentTemplate == _flow_urn("MainPipeline")

        events = _aspects(workunits, DataProcessInstanceRunEventClass)
        assert [e.status for e in events[run_urn]] == ["STARTED", "COMPLETE"]
        # Still running: no completion event yet.
        assert [e.status for e in events[runs["run-2"]]] == ["STARTED"]
        # Denied by pipeline_pattern, or missing a run id.
        assert set(runs) == {"run-1", "run-2"}
        assert source.report.pipeline_runs_scanned == 4
        assert client.run_queries == ["run-1", "run-2"]

    def test_activity_runs_link_to_their_datajob(self) -> None:
        client = _history_client()
        client.activity_runs["run-1"] = [
            _activity_run("Pause", "ar-wait", activity_type="Wait"),
            _activity_run(
                "CopyOrders", "ar-failed", status="Failed", error={"message": "E" * 600}
            ),
            _activity_run(None, "ar-nameless"),
        ]
        source, workunits = _history(client)

        props = _last(workunits, DataProcessInstancePropertiesClass)
        relationships = _last(workunits, DataProcessInstanceRelationshipsClass)
        runs = _run_urns(workunits, "activity_run_id")
        wait = runs["ar-wait"]
        assert relationships[wait].parentTemplate == _job_urn("MainPipeline", "Pause")
        assert props[wait].customProperties["duration_ms"] == "1200"
        assert props[wait].customProperties["pipeline_run_id"] == "run-1"

        failed = runs["ar-failed"]
        assert len(props[failed].customProperties["error"]) == 500
        events = _aspects(workunits, DataProcessInstanceRunEventClass)[failed]
        assert events[-1].result is not None
        assert events[-1].result.type == "FAILURE"
        # Static lineage is attached to the run even without a per-run query.
        inputs = _last(workunits, DataProcessInstanceInputClass)[failed]
        assert inputs.inputs == [ORDERS]
        assert "ar-nameless" not in runs
        assert source.report.activity_runs_scanned == 3

    def test_run_query_and_pre_copy_script_resolve_the_real_tables_and_columns(
        self,
    ) -> None:
        """A ForEach-looped Copy only reveals its per-iteration tables in the
        resolved query and preCopyScript recorded on each run; those replace
        the static lineage on the run and on the activity's DataJob."""
        client = _history_client()
        client.activity_runs["run-1"] = [
            _activity_run(
                "CopyEach",
                "ar-each",
                run_input={
                    "source": {"query": "SELECT id, amount FROM dbo.customers"},
                    "sink": {"preCopyScript": "TRUNCATE TABLE stg.customers"},
                },
            )
        ]
        source, workunits = _history(client)

        run = _run_urns(workunits, "activity_run_id")["ar-each"]
        assert _last(workunits, DataProcessInstanceInputClass)[run].inputs == [
            CUSTOMERS
        ]
        assert _last(workunits, DataProcessInstanceOutputClass)[run].outputs == [
            STG_CUSTOMERS
        ]

        job = _job_urn("MainPipeline", "CopyEach")
        io = _last(workunits, DataJobInputOutputClass)[job]
        assert (io.inputDatasets, io.outputDatasets) == ([CUSTOMERS], [STG_CUSTOMERS])
        assert io.fineGrainedLineages is not None
        assert {_edge(fgl)[1] for fgl in io.fineGrainedLineages} == {
            f"urn:li:schemaField:({STG_CUSTOMERS},id)",
            f"urn:li:schemaField:({STG_CUSTOMERS},amount)",
        }
        assert source.report.column_lineage_extracted >= 2

    def test_unresolved_templating_in_a_run_query_is_not_a_table(self) -> None:
        client = _history_client()
        client.activity_runs["run-1"] = [
            _activity_run(
                "CopyEach",
                "ar-garbage",
                run_input={
                    "source": {"query": "SELECT * FROM @{pipeline().parameters.t}"},
                    "sink": {"preCopyScript": "TRUNCATE TABLE @{item().name}"},
                },
            )
        ]
        source, workunits = _history(client)

        run = _run_urns(workunits, "activity_run_id")["ar-garbage"]
        assert _last(workunits, DataProcessInstanceInputClass)[run].inputs == [ORDERS]
        assert _last(workunits, DataProcessInstanceOutputClass)[run].outputs == [
            STG_ORDERS
        ]
        titles = _warning_titles(source)
        assert "Unresolved ADF Expression in Activity Run Query" in titles
        assert "Unresolved ADF Expression in Activity Run DDL Statement" in titles

    def test_pipeline_run_parameters_replace_a_placeholder_dataset(self) -> None:
        client = _history_client()
        client.activity_runs["run-1"] = [_activity_run("CopyParam", "ar-param")]
        _, workunits = _history(client)

        job = _job_urn("MainPipeline", "CopyParam")
        io = _aspects(workunits, DataJobInputOutputClass)[job]
        # Statically the table is unknown; the run's SourceTable resolves it.
        assert io[0].inputDatasets == [PARAM_PLACEHOLDER]
        assert io[-1].inputDatasets == [CUSTOMERS]
        run = _run_urns(workunits, "activity_run_id")["ar-param"]
        assert _last(workunits, DataProcessInstanceInputClass)[run].inputs == [
            CUSTOMERS
        ]

    def test_dynamic_translator_resolved_on_the_run_gives_column_lineage(self) -> None:
        client = _history_client()
        client.activity_runs["run-1"] = [
            _activity_run(
                "CopyMapped",
                "ar-mapped",
                run_input={
                    "translator": {
                        "type": "TabularTranslator",
                        "mappings": [
                            {"source": {"name": "id"}, "sink": {"name": "ID"}}
                        ],
                    }
                },
            )
        ]
        _, workunits = _history(client)

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("MainPipeline", "CopyMapped")
        ]
        assert io.fineGrainedLineages is not None
        assert [_edge(fgl) for fgl in io.fineGrainedLineages] == [
            (
                f"urn:li:schemaField:({ORDERS},id)",
                f"urn:li:schemaField:({STG_ORDERS},ID)",
            )
        ]

    def test_dynamic_translator_resolving_to_nothing_falls_back_to_the_query_columns(
        self,
    ) -> None:
        client = _history_client()
        client.activity_runs["run-1"] = [
            _activity_run(
                "CopyMapped",
                "ar-unmapped",
                run_input={
                    "translator": None,
                    "source": {"query": "SELECT id FROM dbo.customers"},
                    "sink": {"preCopyScript": "TRUNCATE TABLE stg.customers"},
                },
            )
        ]
        _, workunits = _history(client)

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("MainPipeline", "CopyMapped")
        ]
        assert io.fineGrainedLineages is not None
        assert [fgl.downstreams for fgl in io.fineGrainedLineages] == [
            [f"urn:li:schemaField:({STG_CUSTOMERS},id)"]
        ]

    def test_select_star_query_maps_columns_from_the_datahub_schemas(self) -> None:
        """A bare SELECT * names no columns, so the source table's schema is
        read from DataHub and matched against the sink's own casing."""
        client = _history_client()
        client.activity_runs["run-1"] = [
            _activity_run(
                "CopyEach",
                "ar-star",
                run_input={
                    "source": {"query": "SELECT * FROM dbo.customers"},
                    "sink": {"preCopyScript": "TRUNCATE TABLE stg.customers"},
                },
            )
        ]
        source = _source(
            client,
            pipeline_pattern={"deny": ["Scratch.*"]},
            include_execution_history=True,
        )
        graph = MagicMock()
        graph.get_schema_metadata.side_effect = lambda urn: _schema_metadata(
            ["id", "amount"] if urn == CUSTOMERS else ["ID", "AMOUNT"]
        )
        source.ctx.graph = graph
        workunits = list(source.get_workunits_internal())

        io = _last(workunits, DataJobInputOutputClass)[
            _job_urn("MainPipeline", "CopyEach")
        ]
        assert io.fineGrainedLineages is not None
        assert {_edge(fgl) for fgl in io.fineGrainedLineages} == {
            (
                f"urn:li:schemaField:({CUSTOMERS},id)",
                f"urn:li:schemaField:({STG_CUSTOMERS},ID)",
            ),
            (
                f"urn:li:schemaField:({CUSTOMERS},amount)",
                f"urn:li:schemaField:({STG_CUSTOMERS},AMOUNT)",
            ),
        }

    def test_pre_copy_script_without_a_table_keeps_the_static_sink(self) -> None:
        """A T-SQL batch sqlglot can only treat as an opaque command names no
        table, so the sink is left as statically resolved rather than guessed."""
        client = _history_client()
        client.activity_runs["run-1"] = [
            _activity_run(
                "CopyEach",
                "ar-batch",
                run_input={
                    "source": {"query": "SELECT id FROM dbo.customers"},
                    "sink": {
                        "preCopyScript": "BEGIN TRY TRUNCATE TABLE stg.customers END TRY BEGIN CATCH END CATCH"
                    },
                },
            )
        ]
        _, workunits = _history(client)

        run = _run_urns(workunits, "activity_run_id")["ar-batch"]
        assert _last(workunits, DataProcessInstanceInputClass)[run].inputs == [
            CUSTOMERS
        ]
        assert _last(workunits, DataProcessInstanceOutputClass)[run].outputs == [
            STG_ORDERS
        ]

    def test_execution_history_failure_is_a_warning(self) -> None:
        client = _history_client()
        client.errors["get_pipeline_runs"] = RuntimeError("throttled")
        source, workunits = _history(client)

        assert _last(workunits, DataProcessInstancePropertiesClass) == {}
        assert "Failed to Fetch Execution History" in _warning_titles(source)
        # Static metadata is unaffected.
        assert _flow_urn("MainPipeline") in _last(workunits, DataFlowInfoClass)

    def test_activity_run_failure_keeps_the_pipeline_run(self) -> None:
        client = _history_client()
        client.errors["get_activity_runs"] = RuntimeError("throttled")
        _, workunits = _history(client)

        assert "run-1" in _run_urns(workunits, "run_id")

    def test_lineage_disabled_skips_run_lineage_and_dataset_caches(self) -> None:
        client = _history_client()
        client.activity_runs["run-1"] = [_activity_run("CopyOrders", "ar-plain")]
        _, workunits = _history(client, include_lineage=False)

        assert _last(workunits, DataProcessInstanceInputClass) == {}
        assert _job_urn("MainPipeline", "CopyOrders") not in _last(
            workunits, DataJobInputOutputClass
        )
        assert "ar-plain" in _run_urns(workunits, "activity_run_id")


def _schema_metadata(columns: List[str]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="t",
        platform=MSSQL,
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
