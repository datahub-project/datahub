""""This module contains datahub workunits and metadata change events"""

import logging
from typing import Dict, Iterable, Optional, Union

from dremio_connector.dremio_config import (
    DremioFolderKey,
    DremioSourceConfig,
    DremioSpaceKey,
)
from dremio_connector.dremio_source_controller import DremioController

from datahub.emitter import mcp_builder
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceCapability, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata._urns.urn_defs import CorpUserUrn
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    SqlAggregatorReport,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


class DremioSourceReport(SourceReport):
    num_queries_parsed: int = 0
    num_table_parse_failures: int = 0
    num_column_parse_failures: int = 0
    num_tables_with_known_upstreams: int = 0
    table_failure_rate: str
    column_failure_rate: str

    sql_aggregator: Optional[SqlAggregatorReport] = None

    def compute_stats(self) -> None:
        super().compute_stats()
        self.table_failure_rate = (
            f"{self.num_table_parse_failures / self.num_queries_parsed:.4f}"
            if self.num_queries_parsed
            else "0"
        )
        self.column_failure_rate = (
            f"{self.num_column_parse_failures / self.num_queries_parsed:.4f}"
            if self.num_queries_parsed
            else "0"
        )


@platform_name("Dremio", id="dremio")
@config_class(DremioSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(
    capability_name=SourceCapability.CONTAINERS, description="Enabled by default"
)
@capability(
    capability_name=SourceCapability.DOMAINS,
    description="Supported via the `domain` config field",
)
@capability(
    capability_name=SourceCapability.DATA_PROFILING,
    description="Optionally enabled via configuration",
)
@capability(
    capability_name=SourceCapability.PLATFORM_INSTANCE, description="Enabled by default"
)
@capability(
    capability_name=SourceCapability.SCHEMA_METADATA, description="Enabled by default"
)
class DremioSource(Source):
    config: DremioSourceConfig
    report: DremioSourceReport = DremioSourceReport()
    builder: SqlParsingBuilder
    aggregator: SqlParsingAggregator

    def __init__(self, config: DremioSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.ctx = ctx
        self.graph: DataHubGraph = self.ctx.graph
        self.config = config
        self.dremio_source: DremioController = self.create_controller()
        self.platform_instance = config.platform_instance
        self.aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(self.get_platform()),
            platform_instance=self.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=BaseUsageConfig(),
        )
        self.report.sql_aggregator = self.aggregator.report
        self.builder = SqlParsingBuilder(usage_config=BaseUsageConfig())
        self.is_enterprise_edition = (
            self.dremio_source.dremio_api.test_for_enterprise_edition()
        )

        self.schema_resolver = self.graph.initialize_schema_resolver_from_datahub(
            platform=self.get_platform(),
            env=self.config.env,
            platform_instance=config.platform_instance,
        )

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "Source":
        config = DremioSourceConfig.parse_obj(config_dict)
        return cls(config=config, ctx=ctx)

    def get_platform(self):
        return "dremio"

    def create_controller(self):
        return DremioController(self.config.__dict__)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        dp_iter = self.dremio_source.get_datasets()

        for schema, table, typ, definition in dp_iter:
            item = MetadataChangeEvent._construct_with_defaults()
            table_schema = self.construct_metadata_changes(item, schema, table)

            if not table_schema:
                continue
            space_name = schema[0] if schema else ""
            lst_con_key: Union[DremioFolderKey, DremioSpaceKey]

            if typ in ("TABLE", "SYSTEM_TABLE"):
                sub_type = "Dremio Source"
            else:
                sub_type = "Dremio Space"

            if len(schema) > 1:
                lst_con_key = DremioFolderKey(
                    folder_name=schema[-1].lower(),
                    parent_folder=schema[-2].lower(),
                    platform=make_data_platform_urn("dremio"),
                    platform_instance=self.platform_instance,
                    env=self.config.env,
                )
            else:
                lst_con_key = DremioSpaceKey(
                    space_name=space_name.lower(),
                    platform=make_data_platform_urn("dremio"),
                    platform_instance=self.platform_instance,
                    env=self.config.env,
                )

            yield from self.generate_containers(space_name, sub_type, schema[1:])

            wu_s = mcp_builder.add_dataset_to_container(
                container_key=lst_con_key, dataset_urn=item.proposedSnapshot.urn
            )

            for con_tbl_wu in wu_s:
                self.report.report_workunit(con_tbl_wu)
                yield con_tbl_wu

            wu = MetadataWorkUnit(
                f"{schema}.{table}-{item.proposedSnapshot.urn}", mce=item
            )
            self.report.report_workunit(wu)
            yield wu

            if typ == "VIEW":

                if self.is_enterprise_edition:
                    self.generate_view_table_lineage(
                        schema=schema, table=table, definition=definition
                    )

                self.aggregator.add_view_definition(
                    view_urn=make_dataset_urn_with_platform_instance(
                        platform=make_data_platform_urn("dremio"),
                        name=f"{'.'.join(schema)}.{table}".lower(),
                        platform_instance=self.platform_instance,
                        env=self.config.env,
                    ),
                    view_definition=definition,
                    default_db=schema[0].lower() if len(schema) > 0 else "",
                    default_schema=".".join(schema[1:]) if len(schema) > 1 else "",
                )

        for query in self.dremio_source.dremio_api.extract_all_queries():
            if query.get("query") != "":
                upstream = []
                for ds in query.get("queried_datasets"):
                    upstream.append(
                        make_dataset_urn_with_platform_instance(
                            platform=make_data_platform_urn("dremio"),
                            name=ds.lower(),
                            platform_instance=self.platform_instance,
                            env=self.config.env,
                        )
                    )

                definition = query.get("query")

                if query.get("affected_datasets"):
                    downstream = make_dataset_urn_with_platform_instance(
                        platform=make_data_platform_urn("dremio"),
                        name=query.get("affected_datasets").lower(),
                        platform_instance=self.platform_instance,
                        env=self.config.env,
                    )

                    self.aggregator.add_known_query_lineage(
                        KnownQueryLineageInfo(
                            query_text=definition,
                            upstreams=upstream,
                            downstream=downstream,
                            query_type=query.get("operation_type"),
                        ),
                        merge_lineage=True,
                    )

                    self.aggregator.add_observed_query(
                        query=definition,
                        query_timestamp=query.get("submitted_ts"),
                        user=CorpUserUrn(make_user_urn(query.get("user_name"))),
                    )
                    self.report.num_tables_with_known_upstreams += 1

        for mcp in self.aggregator.gen_metadata():
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

    def generate_containers(self, space_name, sub_type, rest_of_schema_items: list):
        if not space_name:
            return

        parent_key = None

        db_key = DremioSpaceKey(
            space_name=space_name.lower(),
            platform=make_data_platform_urn("dremio"),
            platform_instance=self.platform_instance,
            env=self.config.env,
        )
        con_wu_s = mcp_builder.gen_containers(
            container_key=db_key,
            name=space_name,
            sub_types=[sub_type],
            parent_container_key=parent_key,
        )

        for wu in con_wu_s:
            self.report.report_workunit(wu)
            yield wu

        parent_key = db_key
        for i in range(len(rest_of_schema_items)):
            if i > 0:
                schema_key = DremioFolderKey(
                    folder_name=rest_of_schema_items[i].lower(),
                    parent_folder=rest_of_schema_items[i - 1].lower(),
                    platform=make_data_platform_urn("dremio"),
                    platform_instance=self.platform_instance,
                    env=self.config.env,
                )
            else:
                schema_key = DremioFolderKey(
                    folder_name=rest_of_schema_items[i].lower(),
                    parent_folder=space_name.lower(),
                    platform=make_data_platform_urn("dremio"),
                    platform_instance=self.platform_instance,
                    env=self.config.env,
                )
            con_wu_s = mcp_builder.gen_containers(
                container_key=schema_key,
                name=rest_of_schema_items[i],
                sub_types=["Dremio Folder"],
                parent_container_key=parent_key,
            )
            parent_key = schema_key

            for wu in con_wu_s:
                self.report.report_workunit(wu)
                yield wu

    def generate_view_table_lineage(self, table, schema, definition) -> None:
        upstream = []
        for upstream_dataset in self.dremio_source.get_parents(
            schema=schema, dataset=table
        ):
            upstream.append(
                make_dataset_urn_with_platform_instance(
                    platform=make_data_platform_urn("dremio"),
                    name=upstream_dataset.lower(),
                    platform_instance=self.platform_instance,
                    env=self.config.env,
                )
            )

        if upstream:
            self.aggregator.add_known_query_lineage(
                KnownQueryLineageInfo(
                    query_text=definition,
                    upstreams=list(set(upstream)),
                    downstream=make_dataset_urn_with_platform_instance(
                        platform=make_data_platform_urn("dremio"),
                        name=f"{'.'.join(schema)}.{table}".lower(),
                        env=self.config.env,
                        platform_instance=self.platform_instance,
                    ),
                ),
                merge_lineage=True,
            )
            self.report.num_tables_with_known_upstreams += 1

    def construct_metadata_changes(
        self,
        mce: MetadataChangeEventClass,
        schema: list,
        table: str,
    ) -> bool:
        if schema:
            dataset = f"{'.'.join(schema)}.{table}"
        else:
            dataset = table

        env = self.config.env

        mce.proposedSnapshot = DatasetSnapshotClass._construct_with_defaults()
        mce.proposedSnapshot.urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn("dremio"),
            name=dataset.lower(),
            env=env,
            platform_instance=self.platform_instance,
        )

        pop_res, table_metadata = self.dremio_source.populate_dataset_aspects(
            mce=mce,
            schema=".".join(schema) if schema else "",
            folder_path=[self.platform_instance] + schema if schema else [],
            table_name=table,
            all_tables_and_columns=self.dremio_source.dremio_api.all_tables_and_columns,
        )

        if not pop_res:
            self.report.report_warning(
                "InvalidDataProduct",
                f"Failed to ingest data product: {dataset} as it was not found inside Dremio , or is an invalid dataset.",
            )
            return True
        else:
            self.aggregator.register_schema(
                urn=mce.proposedSnapshot.urn, schema=table_metadata
            )

        if self.config.data_product_specs:
            specs = self.config.data_product_specs
            if specs.get(dataset, None):
                spec = specs.get(dataset)
                sp_tags = self.val_and_get_param(spec, "sp_tags", list)
                sp_paths = self.val_and_get_param(spec, "sp_browse_paths", list)
                self.dremio_source.populate_tag_aspects(mce, sp_tags)
                self.dremio_source.populate_path_aspects(mce, sp_paths)

        return True

    def val_and_get_param(self, spec_map: dict, param_name: str, param_type: type):
        if param_name in spec_map and isinstance(param_type, spec_map[param_name]):
            return spec_map[param_name]

        self.report.report_warning(
            "DataProductSpec",
            f"The parameter '{param_name}' is either not available or invalid.",
        )

        return None

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
