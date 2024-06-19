import logging
import re
from abc import ABC, abstractmethod
from typing import List, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import LookerViewId
from datahub.ingestion.source.looker.looker_connection import LookerConnectionDefinition
from datahub.ingestion.source.looker.lookml_concept_context import LookerViewContext, LookerFieldContext
from datahub.ingestion.source.looker.lookml_config import LookMLSourceReport, LookMLSourceConfig
from datahub.ingestion.source.looker.lookml_resolver import LookerViewIdCache, get_derived_looker_view_id
from datahub.sql_parsing.sqlglot_lineage import ColumnRef, Urn, create_lineage_sql_parsed_result, SqlParsingResult

logger = logging.getLogger(__name__)


def _platform_names_have_2_parts(platform: str) -> bool:
    return platform in {"hive", "mysql", "athena"}


def _drop_hive_dot(urn: str) -> str:
    """
    This is special handling for hive platform where "hive." is coming in urn's id because of the way SQL
    is written in lookml.

    Example: urn:li:dataset:(urn:li:dataPlatform:hive,hive.my_database.my_table,PROD)

    Here we need to transform hive.my_database.my_table to my_database.my_table
    """
    if urn.startswith("urn:li:dataset:(urn:li:dataPlatform:hive"):
        return re.sub(r"hive\.", "", urn)

    return urn


def get_upstream_column_names(field_context: LookerFieldContext) -> List[str]:
    if field_context.sql() is None:
        # If no "sql" is specified, we assume this is referencing an upstream field
        # with the same name. This commonly happens for extends and derived tables.
        return [
            field_context.name()
        ]

    upstream_column_names: List[str] = []

    for upstream_field_match in re.finditer(
            r"\${TABLE}\.[\"]*([\.\w]+)", field_context.sql()
    ):
        matched_field = upstream_field_match.group(1)
        # Remove quotes from field names
        matched_field = (
            matched_field.replace('"', "").replace("`", "").lower()
        )
        upstream_column_names.append(matched_field)

    return upstream_column_names


def _generate_fully_qualified_name(
        sql_table_name: str,
        connection_def: LookerConnectionDefinition,
        reporter: LookMLSourceReport,
) -> str:
    """Returns a fully qualified dataset name, resolved through a connection definition.
    Input sql_table_name can be in three forms: table, db.table, db.schema.table"""
    # TODO: This function should be extracted out into a Platform specific naming class since name translations
    #  are required across all connectors

    # Bigquery has "project.db.table" which can be mapped to db.schema.table form
    # All other relational db's follow "db.schema.table"
    # With the exception of mysql, hive, athena which are "db.table"

    # first detect which one we have
    parts = len(sql_table_name.split("."))

    if parts == 3:
        # fully qualified, but if platform is of 2-part, we drop the first level
        if _platform_names_have_2_parts(connection_def.platform):
            sql_table_name = ".".join(sql_table_name.split(".")[1:])
        return sql_table_name.lower()

    if parts == 1:
        # Bare table form
        if _platform_names_have_2_parts(connection_def.platform):
            dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        else:
            dataset_name = f"{connection_def.default_db}.{connection_def.default_schema}.{sql_table_name}"
        return dataset_name.lower()

    if parts == 2:
        # if this is a 2 part platform, we are fine
        if _platform_names_have_2_parts(connection_def.platform):
            return sql_table_name.lower()
        # otherwise we attach the default top-level container
        dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        return dataset_name.lower()

    reporter.report_warning(
        key=sql_table_name, reason=f"{sql_table_name} has more than 3 parts."
    )
    return sql_table_name.lower()


class AbstractViewUpstream(ABC):
    """
    Implementation of this interface extracts the view upstream as per the way the view is bound to datasets.
    For detail explanation please refer lookml_concept_context.LookerViewContext documentation.
    """
    view_context: LookerViewContext
    looker_view_id_cache: LookerViewIdCache
    config: LookMLSourceConfig
    ctx: PipelineContext

    def __init__(
            self,
            view_context: LookerViewContext,
            looker_view_id_cache: LookerViewIdCache,
            config: LookMLSourceConfig,
            ctx: PipelineContext,
    ):
        self.view_context = view_context
        self.looker_view_id_cache = looker_view_id_cache
        self.config = config
        self.ctx = ctx

    @abstractmethod
    def get_upstream_column_ref(self, field_context: LookerFieldContext) -> List[ColumnRef]:
        pass

    @abstractmethod
    def get_upstream_dataset_urn(self) -> List[Urn]:
        pass

    def get_derived_view_looker_id(self) -> Optional[LookerViewId]:
        return get_derived_looker_view_id(
            qualified_table_name=_generate_fully_qualified_name(
                self.view_context.sql_table_name(),
                self.view_context.view_connection,
                self.view_context.reporter,
            ),
            base_folder_path=self.view_context.base_folder_path,
            looker_view_id_cache=self.looker_view_id_cache,
        )


class SqlBasedDerivedViewUpstream(AbstractViewUpstream):
    spr: SqlParsingResult
    upstream_dataset_urns: List[str]

    def __init__(
            self,
            view_context: LookerViewContext,
            looker_view_id_cache: LookerViewIdCache,
            config: LookMLSourceConfig,
            ctx: PipelineContext,

    ):
        super().__init__(view_context, looker_view_id_cache, config, ctx)

    def _get_spr(self) -> Optional[SqlParsingResult]:
        # TODO : Add lru cache
        if not self.spr:
            self.spr = create_lineage_sql_parsed_result(
                query=self.view_context.sql(),
                default_schema=self.view_context.view_connection.default_schema,
                default_db=self.view_context.view_connection.default_db,
                platform=self.view_context.view_connection.platform,
                platform_instance=self.view_context.view_connection.platform_instance,
                env=self.view_context.view_connection.platform_env,  # It's never going to be None
                graph=self.ctx.graph,
            )

            if (
                self.spr.debug_info.table_error is not None
                or self.spr.debug_info.column_error is not None
            ):
                logging.debug(f"Failed to parsed the sql query. table_error={self.spr.debug_info.table_error} and column_error={self.spr.debug_info.column_error}")
                return None

        return self.spr

    def _get_upstream_dataset_urn(self) -> List[Urn]:
        # TODO : Add lru cache

        sql_parsing_result: Optional[SqlParsingResult] = self._get_spr()

        if sql_parsing_result is None:
            return []

        if self.upstream_dataset_urns:
            return self.upstream_dataset_urns

        self.upstream_dataset_urns = [_drop_hive_dot(urn) for urn in sql_parsing_result.in_tables]

        return self.upstream_dataset_urns

    def get_upstream_column_ref(self, field_context: LookerFieldContext) -> List[ColumnRef]:
        sql_parsing_result: Optional[SqlParsingResult] = self._get_spr()

        if sql_parsing_result is None:
            return []

        upstreams_column_refs: List[ColumnRef] = []

        for cll in sql_parsing_result.column_lineage:
            if cll.downstream.column == field_context.name():
                upstreams_column_refs = cll.upstreams

        # field might get skip either because of Parser not able to identify the column from GMS
        # in-case of "select * from look_ml_view.SQL_TABLE_NAME" or extra field are defined in the looker view which is
        # referring to upstream table
        if self._get_upstream_dataset_urn() and not upstreams_column_refs:
            upstreams_column_refs = [
                ColumnRef(
                    table=self._get_upstream_dataset_urn()[0],  # 0th index has table of from clause
                    column=column,
                )
                for column in get_upstream_column_names(field_context)
            ]

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return self._get_upstream_dataset_urn()


class NativeDerivedViewUpstream(AbstractViewUpstream):
    def get_upstream_column_ref(self, field_context: LookerFieldContext) -> List[ColumnRef]:
        pass

    def get_upstream_dataset_urn(self) -> List[Urn]:
        pass


class RegularViewUpstream(AbstractViewUpstream):
    upstream_dataset_urn: Optional[str]

    def __init__(
            self,
            view_context: LookerViewContext,
            looker_view_id_cache: LookerViewIdCache,
            config: LookMLSourceConfig,
            ctx: PipelineContext,

    ):
        super().__init__(view_context, looker_view_id_cache, config, ctx)
        self.upstream_dataset_urn = None

    def _get_upstream_dataset_urn(self) -> Urn:
        # In regular case view's upstream dataset is either same as view-name or mentioned in "sql_table_name" field
        # view_context.sql_table_name() handle this condition to return dataset name
        if self.upstream_dataset_urn is None:
            self.upstream_dataset_urn = _generate_fully_qualified_name(
                sql_table_name=self.view_context.sql_table_name(),
                connection_def=self.view_context.view_connection,
                reporter=self.view_context.reporter,
            )

        return self.upstream_dataset_urn

    def get_upstream_column_ref(self, field_context: LookerFieldContext) -> List[ColumnRef]:
        upstream_column_ref: List[ColumnRef] = []

        for column_name in get_upstream_column_names(field_context):
            upstream_column_ref.append(
                ColumnRef(
                    table=self._get_upstream_dataset_urn(),
                    column=column_name
                )
            )

        return upstream_column_ref

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return [self._get_upstream_dataset_urn()]


class DotSqlTableNameViewUpstream(AbstractViewUpstream):
    upstream_dataset_urn: Optional[Urn]

    def __init__(
            self,
            view_context: LookerViewContext,
            looker_view_id_cache: LookerViewIdCache,
            config: LookMLSourceConfig,
            ctx: PipelineContext,

    ):
        super().__init__(view_context, looker_view_id_cache, config, ctx)
        self.upstream_dataset_urn = None

    def _get_upstream_dataset_urn(self) -> Optional[Urn]:
        if self.upstream_dataset_urn is None:
            # In this case view_context.sql_table_name() refers to derived view name
            looker_view_id = self.get_derived_view_looker_id()

            self.upstream_dataset_urn = looker_view_id.get_urn(
                config=self.config,
            ) if looker_view_id is not None else None

        return self.upstream_dataset_urn

    def get_upstream_column_ref(self, field_context: LookerFieldContext) -> List[ColumnRef]:
        upstream_column_ref: List[ColumnRef] = []
        if self._get_upstream_dataset_urn() is None:
            return upstream_column_ref

        for column_name in get_upstream_column_names(field_context):
            upstream_column_ref.append(
                ColumnRef(
                    table=self._get_upstream_dataset_urn(),
                    column=column_name
                )
            )

        return upstream_column_ref

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return [self._get_upstream_dataset_urn()] if self._get_upstream_dataset_urn() is not None else []


def create_view_upstream(
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        ctx: PipelineContext,
        reporter: LookMLSourceReport
) -> Optional[AbstractViewUpstream]:
    if view_context.is_regular_case():
        return RegularViewUpstream(
            view_context=view_context,
            config=config,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_sql_table_name_referring_to_view():
        return DotSqlTableNameViewUpstream(
            view_context=view_context,
            config=config,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_sql_based_derived_case():
        return SqlBasedDerivedViewUpstream(
            view_context=view_context,
            config=config,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_native_derived_case():
        return NativeDerivedViewUpstream(
            view_context=view_context,
            config=config,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    reporter.report_warning(view_context.view_file_name(), "No implementation found to resolve upstream of the view")

    return None
