from typing import Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.snaplogic.snaplogic_config import SnaplogicConfig
from datahub.ingestion.source.snaplogic.snaplogic_lineage_extractor import (
    SnaplogicLineageExtractor,
)
from datahub.ingestion.source.snaplogic.snaplogic_parser import (
    ColumnMapping,
    Dataset,
    SnapLogicParser,
)
from datahub.ingestion.source.snaplogic.snaplogic_utils import SnaplogicUtils
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
)


@platform_name("SnapLogic")
@config_class(SnaplogicConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "SnapLogic does not support platform instances",
    supported=False,
)
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.LINEAGE_FINE, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Not supported yet", supported=False)
class SnaplogicSource(StatefulIngestionSourceBase):
    """
    A source plugin for ingesting lineage and metadata from SnapLogic.
    """

    def __init__(self, config: SnaplogicConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = StaleEntityRemovalSourceReport()
        self.graph: Optional[DataHubGraph] = ctx.graph
        self.snaplogic_parser = SnapLogicParser(
            config.case_insensitive_namespaces, self.config.namespace_mapping
        )
        self.redundant_lineage_run_skip_handler: Optional[
            RedundantLineageRunSkipHandler
        ] = None
        if self.config.enable_stateful_lineage_ingestion:
            self.redundant_lineage_run_skip_handler = RedundantLineageRunSkipHandler(
                source=self,
                config=self.config,
                pipeline_name=ctx.pipeline_name,
                run_id=ctx.run_id,
            )
        self.snaplogic_lineage_extractor = SnaplogicLineageExtractor(
            config=config,
            redundant_run_skip_handler=self.redundant_lineage_run_skip_handler,
            report=self.report,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        try:
            self.report.info(
                message="Starting lineage ingestion from SnapLogic",
                title="Lineage Ingestion",
            )

            records_processed = 0
            for lineage in self.snaplogic_lineage_extractor.get_lineages():
                try:
                    for workunit in self._process_lineage_record(lineage):
                        yield workunit
                    records_processed += 1

                    if records_processed % 20 == 0:
                        self.report.info(
                            message=f"Processed {records_processed} lineage records",
                            title="Lineage Ingestion Progress",
                        )
                except Exception as e:
                    self.report.report_failure(
                        message="Failed to process lineage record",
                        context=str(lineage),
                        exc=e,
                    )
            self.report.info(
                message=f"Completed processing {records_processed} lineage records",
                title="Lineage Ingestion Complete",
            )
            self.snaplogic_lineage_extractor.report_status("lineage_ingestion", True)
            self.snaplogic_lineage_extractor.update_stats()
        except Exception as e:
            self.report.report_failure(message="Failed to fetch lineages", exc=e)
            self.snaplogic_lineage_extractor.report_status("lineage_ingestion", False)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _process_lineage_record(self, lineage: dict) -> Iterable[MetadataWorkUnit]:
        """Process a lineage record to create pipeline and task workunits with relationships."""
        producer = lineage.get("producer")
        if not producer:
            return
        pipeline_snode_id = producer.split("#pipe_snode=")[1]
        if not pipeline_snode_id:
            return
        datasets = self.snaplogic_parser.extract_datasets_from_lineage(lineage)
        pipeline = self.snaplogic_parser.extract_pipeline_from_lineage(lineage)
        task = self.snaplogic_parser.extract_task_from_lineage(lineage)
        columns_mapping = self.snaplogic_parser.extract_columns_mapping_from_lineage(
            lineage
        )

        # Create pipeline MCP
        for pipeline_workunit in self.create_pipeline_mcp(
            name=pipeline.name,
            pipeline_snode_id=pipeline.id,
            namespace=pipeline.namespace,
        ):
            self.report.report_workunit(pipeline_workunit)
            yield pipeline_workunit

        # Create dataset MCP
        for dataset in datasets:
            for dataset_workunit in self.create_dataset_mcp(
                dataset_name=dataset.name,
                dataset_display_name=dataset.display_name,
                fields=dataset.fields,
                platform=dataset.platform,
                platform_instance=dataset.platform_instance,
            ):
                self.report.report_workunit(dataset_workunit)
                yield dataset_workunit

        # Create task MCP
        for task_workunit in self.create_task_mcp(
            name=task.name,
            task_id=task.id,
            namespace=task.namespace,
            pipeline_snode_id=pipeline_snode_id,
            input_datasets=[dataset for dataset in datasets if dataset.type == "INPUT"],
            output_datasets=[
                dataset for dataset in datasets if dataset.type == "OUTPUT"
            ],
            columns_mapping=columns_mapping,
        ):
            self.report.report_workunit(task_workunit)
            yield task_workunit

    def create_task_mcp(
        self,
        task_id: str,
        name: str,
        namespace: str,
        pipeline_snode_id: str,
        input_datasets: list[Dataset],
        output_datasets: list[Dataset],
        columns_mapping: list[ColumnMapping],
    ) -> Iterable[MetadataWorkUnit]:
        """Create MCPs for a task (snap) including metadata and lineage."""
        job_urn = make_data_job_urn(
            orchestrator=namespace,
            flow_id=pipeline_snode_id,
            job_id=task_id,
            cluster="PROD",
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=name,
                description="",
                externalUrl=f"{self.config.base_url}/sl/designer.html?v=21818#pipe_snode={pipeline_snode_id}",
                type="SNAPLOGIC_SNAP",
            ),
        ).as_workunit()

        # Helper functions
        def dataset_urn(d: Dataset) -> str:
            return make_dataset_urn_with_platform_instance(
                d.platform, d.name, d.platform_instance
            )

        def field_urn(d, f):
            return make_schema_field_urn(dataset_urn(d), f["name"])

        # Emit lineage
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=[dataset_urn(d) for d in input_datasets],
                outputDatasets=[dataset_urn(d) for d in output_datasets],
                inputDatasetFields=[
                    field_urn(d, f) for d in input_datasets for f in d.fields
                ],
                outputDatasetFields=[
                    field_urn(d, f) for d in output_datasets for f in d.fields
                ],
                fineGrainedLineages=[
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        upstreams=[
                            make_schema_field_urn(
                                make_dataset_urn_with_platform_instance(
                                    cl.input_dataset.platform,
                                    cl.input_dataset.name,
                                    cl.input_dataset.platform_instance,
                                    cl.input_dataset.env,
                                ),
                                cl.input_field,
                            )
                        ],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
                        downstreams=[
                            make_schema_field_urn(
                                make_dataset_urn_with_platform_instance(
                                    cl.output_dataset.platform,
                                    cl.output_dataset.name,
                                    cl.output_dataset.platform_instance,
                                    cl.output_dataset.env,
                                ),
                                cl.output_field,
                            )
                        ],
                    )
                    for cl in columns_mapping
                ],
            ),
        ).as_workunit()

    def create_dataset_mcp(
        self,
        dataset_name: str,
        dataset_display_name: str,
        fields: list[dict],
        platform: str = "snaplogic",
        env: str = "PROD",
        platform_instance: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=platform,
            name=dataset_name,
            env=env,
            platform_instance=platform_instance,
        )

        # Skip dataset creation if:
        # 1. The platform is not "snaplogic" AND
        # 2. Either:
        #    a) The config `create_non_snaplogic_datasets` is disabled (False), meaning
        #       we do not create datasets for non-snaplogic platforms, OR
        #    b) The dataset already exists in DataHub (`self.graph.exists(dataset_urn)`).
        if platform != "snaplogic" and (
            not self.config.create_non_snaplogic_datasets
            or (self.graph and self.graph.exists(dataset_urn))
        ):
            return

        dataset_properties = DatasetPropertiesClass(
            name=dataset_display_name,
            qualifiedName=dataset_name,
        )
        schema_fields = [
            SchemaFieldClass(
                fieldPath=field["name"],
                type=SnaplogicUtils.get_datahub_type(field.get("type", "Varchar")),
                nativeDataType=field.get("type", "Varchar"),
            )
            for field in fields
        ]
        schema_metadata = SchemaMetadataClass(
            schemaName=dataset_name,
            platform=make_data_platform_urn(platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=schema_fields,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def create_pipeline_mcp(
        self, name: str, namespace: str, pipeline_snode_id: str
    ) -> Iterable[MetadataWorkUnit]:
        flow_urn = make_data_flow_urn(
            orchestrator=namespace, flow_id=pipeline_snode_id, cluster="PROD"
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=name,
                description="",
                externalUrl=f"{self.config.base_url}/sl/designer.html?v=21818#pipe_snode={pipeline_snode_id}",
            ),
        ).as_workunit()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        super().close()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SnaplogicSource":
        config = SnaplogicConfig.model_validate(config_dict)
        return cls(config, ctx)
