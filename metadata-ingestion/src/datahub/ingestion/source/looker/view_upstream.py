import logging
import re
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Dict, List, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.looker.looker_common import (
    LookerExplore,
    LookerViewId,
    ViewField,
    ViewFieldType,
)
from datahub.ingestion.source.looker.looker_view_id_cache import LookerViewIdCache
from datahub.ingestion.source.looker.lookml_concept_context import (
    LookerFieldContext,
    LookerViewContext,
)
from datahub.ingestion.source.looker.lookml_config import (
    DERIVED_VIEW_SUFFIX,
    NAME,
    LookerConnectionDefinition,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.urn_functions import get_qualified_table_name
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    SqlParsingResult,
    Urn,
    create_lineage_sql_parsed_result,
)

logger = logging.getLogger(__name__)


def is_derived_view(view_name: str) -> bool:
    if DERIVED_VIEW_SUFFIX in view_name.lower():
        return True

    return False


def get_derived_looker_view_id(
    qualified_table_name: str,
    looker_view_id_cache: "LookerViewIdCache",
    base_folder_path: str,
) -> Optional[LookerViewId]:
    # qualified_table_name can be in either of below format
    # 1) db.schema.employee_income_source.sql_table_name
    # 2) db.employee_income_source.sql_table_name
    # 3) employee_income_source.sql_table_name
    # In any of the form we need the text coming before ".sql_table_name" and after last "."
    parts: List[str] = re.split(
        DERIVED_VIEW_SUFFIX, qualified_table_name, flags=re.IGNORECASE
    )
    view_name: str = parts[0].split(".")[-1]

    looker_view_id: Optional[LookerViewId] = looker_view_id_cache.get_looker_view_id(
        view_name=view_name,
        base_folder_path=base_folder_path,
    )

    return looker_view_id


def resolve_derived_view_urn_of_col_ref(
    column_refs: List[ColumnRef],
    looker_view_id_cache: "LookerViewIdCache",
    base_folder_path: str,
    config: LookMLSourceConfig,
) -> List[ColumnRef]:

    new_column_refs: List[ColumnRef] = []
    for col_ref in column_refs:
        if is_derived_view(col_ref.table.lower()):
            new_urns: List[str] = fix_derived_view_urn(
                urns=[col_ref.table],
                looker_view_id_cache=looker_view_id_cache,
                base_folder_path=base_folder_path,
                config=config,
            )
            if not new_urns:
                logger.warning(
                    f"Not able to resolve to derived view looker id for {col_ref.table}"
                )
                continue

            new_column_refs.append(ColumnRef(table=new_urns[0], column=col_ref.column))
        else:
            new_column_refs.append(col_ref)

    return new_column_refs


def fix_derived_view_urn(
    urns: List[str],
    looker_view_id_cache: "LookerViewIdCache",
    base_folder_path: str,
    config: LookMLSourceConfig,
) -> List[str]:
    # Regenerate view urn if .sql_table_name is present in urn
    new_urns: List[str] = []
    for urn in urns:
        if is_derived_view(urn):
            looker_view_id = get_derived_looker_view_id(
                qualified_table_name=get_qualified_table_name(urn),
                looker_view_id_cache=looker_view_id_cache,
                base_folder_path=base_folder_path,
            )

            if looker_view_id is None:
                logger.warning(
                    f"Not able to resolve to derived view looker id for {urn}"
                )
                continue

            new_urns.append(looker_view_id.get_urn(config=config))
        else:
            new_urns.append(urn)

    return new_urns


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


def _drop_hive_dot_from_upstream(upstreams: List[ColumnRef]) -> List[ColumnRef]:
    return [
        ColumnRef(table=_drop_hive_dot(column_ref.table), column=column_ref.column)
        for column_ref in upstreams
    ]


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
        title="Malformed Table Name",
        message="Table name has more than 3 parts.",
        context=f"Table Name: {sql_table_name}",
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
    reporter: LookMLSourceReport
    ctx: PipelineContext

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        self.view_context = view_context
        self.looker_view_id_cache = looker_view_id_cache
        self.config = config
        self.reporter = reporter
        self.ctx = ctx

    @abstractmethod
    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        pass

    @abstractmethod
    def get_upstream_dataset_urn(self) -> List[Urn]:
        pass

    def create_fields(self) -> List[ViewField]:
        return []  # it is for the special case


class SqlBasedDerivedViewUpstream(AbstractViewUpstream, ABC):
    """
    Handle the case where upstream dataset is defined in derived_table.sql
    """

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)
        # These are the function where we need to catch the response once calculated
        self._get_spr = lru_cache(maxsize=1)(self.__get_spr)
        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )

    def __get_spr(self) -> Optional[SqlParsingResult]:
        # for backward compatibility
        if not self.config.parse_table_names_from_sql:
            return None

        spr = create_lineage_sql_parsed_result(
            query=self.get_sql_query(),
            default_schema=self.view_context.view_connection.default_schema,
            default_db=self.view_context.view_connection.default_db,
            platform=self.view_context.view_connection.platform,
            platform_instance=self.view_context.view_connection.platform_instance,
            env=self.view_context.view_connection.platform_env or self.config.env,
            graph=self.ctx.graph,
        )
        return spr

    def __get_upstream_dataset_urn(self) -> List[Urn]:
        sql_parsing_result: Optional[SqlParsingResult] = self._get_spr()

        if sql_parsing_result is None:
            return []

        if sql_parsing_result.debug_info.table_error is not None:
            self.reporter.report_warning(
                title="Table Level Lineage Missing",
                message="Error in parsing derived sql",
                context=f"View-name: {self.view_context.name()}",
                exc=sql_parsing_result.debug_info.table_error,
            )
            return []

        upstream_dataset_urns: List[str] = [
            _drop_hive_dot(urn) for urn in sql_parsing_result.in_tables
        ]

        # fix any derived view reference present in urn
        upstream_dataset_urns = fix_derived_view_urn(
            urns=upstream_dataset_urns,
            looker_view_id_cache=self.looker_view_id_cache,
            base_folder_path=self.view_context.base_folder_path,
            config=self.config,
        )

        return upstream_dataset_urns

    def create_fields(self) -> List[ViewField]:
        spr: Optional[SqlParsingResult] = self._get_spr()

        if spr is None:
            return []

        if spr.debug_info.column_error is not None:
            self.reporter.report_warning(
                title="Column Level Lineage Missing",
                message="Error in parsing derived sql for CLL",
                context=f"View-name: {self.view_context.name()}",
                exc=spr.debug_info.column_error,
            )
            return []

        fields: List[ViewField] = []

        column_lineages: List[ColumnLineageInfo] = (
            spr.column_lineage if spr.column_lineage is not None else []
        )

        for cll in column_lineages:
            fields.append(
                ViewField(
                    name=cll.downstream.column,
                    label="",
                    type=(
                        cll.downstream.native_column_type
                        if cll.downstream.native_column_type is not None
                        else "unknown"
                    ),
                    description="",
                    field_type=ViewFieldType.UNKNOWN,
                    upstream_fields=_drop_hive_dot_from_upstream(cll.upstreams),
                )
            )

        return fields

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        sql_parsing_result: Optional[SqlParsingResult] = self._get_spr()

        if sql_parsing_result is None:
            return []

        if sql_parsing_result.debug_info.column_error is not None:
            self.reporter.report_warning(
                title="Column Level Lineage Missing",
                message="Error in parsing derived sql for CLL",
                context=f"View-name: {self.view_context.name()}. "
                f"Error: {sql_parsing_result.debug_info.column_error}",
            )
            return []

        upstreams_column_refs: List[ColumnRef] = []
        if sql_parsing_result.column_lineage:
            for cll in sql_parsing_result.column_lineage:
                if cll.downstream.column == field_context.name():
                    upstreams_column_refs = cll.upstreams
                    break

        # field might get skip either because of Parser not able to identify the column from GMS
        # in-case of "select * from look_ml_view.SQL_TABLE_NAME" or extra field are defined in the looker view which is
        # referring to upstream table
        if self._get_upstream_dataset_urn() and not upstreams_column_refs:
            upstreams_column_refs = [
                ColumnRef(
                    table=self._get_upstream_dataset_urn()[
                        0
                    ],  # 0th index has table of from clause
                    column=column,
                )
                for column in field_context.column_name_in_sql_attribute()
            ]

        # fix any derived view reference present in urn
        upstreams_column_refs = resolve_derived_view_urn_of_col_ref(
            column_refs=upstreams_column_refs,
            looker_view_id_cache=self.looker_view_id_cache,
            base_folder_path=self.view_context.base_folder_path,
            config=self.config,
        )

        return _drop_hive_dot_from_upstream(upstreams_column_refs)

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return self._get_upstream_dataset_urn()

    @abstractmethod
    def get_sql_query(self) -> str:
        pass


class DirectQueryUpstreamSource(SqlBasedDerivedViewUpstream):
    """
    Pattern 7 as per view-context documentation
    """

    def get_sql_query(self) -> str:
        return self.view_context.datahub_transformed_sql_table_name()


class DerivedQueryUpstreamSource(SqlBasedDerivedViewUpstream):
    """
    Pattern 4 as per view-context documentation
    """

    def get_sql_query(self) -> str:
        return self.view_context.datahub_transformed_sql()


class NativeDerivedViewUpstream(AbstractViewUpstream):
    """
    Handle the case where upstream dataset is defined as derived_table.explore_source
    """

    upstream_dataset_urns: List[str]
    explore_column_mapping: Dict

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)

        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )
        self._get_explore_column_mapping = lru_cache(maxsize=1)(
            self.__get_explore_column_mapping
        )

    def __get_upstream_dataset_urn(self) -> List[str]:
        current_view_id: Optional[
            LookerViewId
        ] = self.looker_view_id_cache.get_looker_view_id(
            view_name=self.view_context.name(),
            base_folder_path=self.view_context.base_folder_path,
        )

        # Current view will always be present in cache. assert  will silence the lint
        assert current_view_id

        # We're creating a "LookerExplore" just to use the urn generator.
        upstream_dataset_urns: List[str] = [
            LookerExplore(
                name=self.view_context.explore_source()[NAME],
                model_name=current_view_id.model_name,
            ).get_explore_urn(self.config)
        ]

        return upstream_dataset_urns

    def __get_explore_column_mapping(self) -> Dict:
        explore_columns: Dict = self.view_context.explore_source().get("columns", {})

        explore_column_mapping = {}

        for column in explore_columns:
            explore_column_mapping[column[NAME]] = column

        return explore_column_mapping

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        upstream_column_refs: List[ColumnRef] = []

        if not self._get_upstream_dataset_urn():
            # No upstream explore dataset found
            logging.debug(
                f"upstream explore not found for field {field_context.name()} of view {self.view_context.name()}"
            )
            return upstream_column_refs

        explore_urn: str = self._get_upstream_dataset_urn()[0]

        for column in field_context.column_name_in_sql_attribute():
            if column in self._get_explore_column_mapping():
                explore_column: Dict = self._get_explore_column_mapping()[column]
                upstream_column_refs.append(
                    ColumnRef(
                        column=explore_column.get("field", explore_column[NAME]),
                        table=explore_urn,
                    )
                )

        return upstream_column_refs

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return self._get_upstream_dataset_urn()


class RegularViewUpstream(AbstractViewUpstream):
    """
    Handle the case where upstream dataset name is equal to view-name
    """

    upstream_dataset_urn: Optional[str]

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)
        self.upstream_dataset_urn = None

        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )

    def __get_upstream_dataset_urn(self) -> Urn:
        # In regular case view's upstream dataset is either same as view-name or mentioned in "sql_table_name" field
        # view_context.datahub_transformed_sql_table_name() handle this condition to return dataset name
        qualified_table_name: str = _generate_fully_qualified_name(
            sql_table_name=self.view_context.datahub_transformed_sql_table_name(),
            connection_def=self.view_context.view_connection,
            reporter=self.view_context.reporter,
        )

        self.upstream_dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.view_context.view_connection.platform,
            name=qualified_table_name.lower(),
            platform_instance=self.view_context.view_connection.platform_instance,
            env=self.view_context.view_connection.platform_env or self.config.env,
        )

        return self.upstream_dataset_urn

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        upstream_column_ref: List[ColumnRef] = []

        for column_name in field_context.column_name_in_sql_attribute():
            upstream_column_ref.append(
                ColumnRef(table=self._get_upstream_dataset_urn(), column=column_name)
            )

        return upstream_column_ref

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return [self._get_upstream_dataset_urn()]


class DotSqlTableNameViewUpstream(AbstractViewUpstream):
    """
    Handle the case where upstream dataset name is mentioned as sql_table_name: ${view-name.SQL_TABLE_NAME}
    """

    upstream_dataset_urn: List[Urn]

    def __init__(
        self,
        view_context: LookerViewContext,
        looker_view_id_cache: LookerViewIdCache,
        config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
        ctx: PipelineContext,
    ):
        super().__init__(view_context, looker_view_id_cache, config, reporter, ctx)
        self.upstream_dataset_urn = []

        self._get_upstream_dataset_urn = lru_cache(maxsize=1)(
            self.__get_upstream_dataset_urn
        )

    def __get_upstream_dataset_urn(self) -> List[Urn]:
        # In this case view_context.datahub_transformed_sql_table_name() refers to derived view name
        looker_view_id = get_derived_looker_view_id(
            qualified_table_name=_generate_fully_qualified_name(
                self.view_context.datahub_transformed_sql_table_name(),
                self.view_context.view_connection,
                self.view_context.reporter,
            ),
            base_folder_path=self.view_context.base_folder_path,
            looker_view_id_cache=self.looker_view_id_cache,
        )

        if looker_view_id is not None:
            self.upstream_dataset_urn = [
                looker_view_id.get_urn(
                    config=self.config,
                )
            ]

        return self.upstream_dataset_urn

    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        upstream_column_ref: List[ColumnRef] = []
        if not self._get_upstream_dataset_urn():
            return upstream_column_ref

        for column_name in field_context.column_name_in_sql_attribute():
            upstream_column_ref.append(
                ColumnRef(table=self._get_upstream_dataset_urn()[0], column=column_name)
            )

        return upstream_column_ref

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return self._get_upstream_dataset_urn()


class EmptyImplementation(AbstractViewUpstream):
    def get_upstream_column_ref(
        self, field_context: LookerFieldContext
    ) -> List[ColumnRef]:
        return []

    def get_upstream_dataset_urn(self) -> List[Urn]:
        return []


def create_view_upstream(
    view_context: LookerViewContext,
    looker_view_id_cache: LookerViewIdCache,
    config: LookMLSourceConfig,
    ctx: PipelineContext,
    reporter: LookMLSourceReport,
) -> AbstractViewUpstream:

    if view_context.is_regular_case():
        return RegularViewUpstream(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_sql_table_name_referring_to_view():
        return DotSqlTableNameViewUpstream(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if any(
        [
            view_context.is_sql_based_derived_case(),
            view_context.is_sql_based_derived_view_without_fields_case(),
        ]
    ):

        return DerivedQueryUpstreamSource(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_direct_sql_query_case():
        return DirectQueryUpstreamSource(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    if view_context.is_native_derived_case():
        return NativeDerivedViewUpstream(
            view_context=view_context,
            config=config,
            reporter=reporter,
            ctx=ctx,
            looker_view_id_cache=looker_view_id_cache,
        )

    reporter.report_warning(
        title="ViewUpstream Implementation Not Found",
        message="No implementation found to resolve upstream of the view",
        context=f"view_name={view_context.name()} , view_file_name={view_context.view_file_name()}",
    )

    return EmptyImplementation(
        view_context=view_context,
        config=config,
        reporter=reporter,
        ctx=ctx,
        looker_view_id_cache=looker_view_id_cache,
    )
