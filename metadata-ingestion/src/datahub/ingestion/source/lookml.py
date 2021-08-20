import glob
import importlib
import itertools
import logging
import pathlib
import re
import sys
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from dataclasses import replace
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type

import pydantic

from datahub.utilities.sql_parser import SQLParser

if sys.version_info >= (3, 7):
    import lkml
else:
    raise ModuleNotFoundError("The lookml plugin requires Python 3.7 or newer.")

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_types import (
    POSTGRES_TYPES_MAP,
    SNOWFLAKE_TYPES_MAP,
    resolve_postgres_modified_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchema,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
)
from datahub.metadata.schema_classes import EnumTypeClass, SchemaMetadataClass

assert sys.version_info[1] >= 7  # needed for mypy

logger = logging.getLogger(__name__)


class LookMLSourceConfig(ConfigModel):
    base_folder: pydantic.DirectoryPath
    connection_to_platform_map: Dict[str, str]
    platform_name: str = "looker"
    model_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    view_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    env: str = builder.DEFAULT_ENV
    parse_table_names_from_sql: bool = False
    sql_parser: str = "datahub.utilities.sql_parser.DefaultSQLParser"


@dataclass
class LookMLSourceReport(SourceReport):
    models_scanned: int = 0
    views_scanned: int = 0
    filtered_models: List[str] = dataclass_field(default_factory=list)
    filtered_views: List[str] = dataclass_field(default_factory=list)

    def report_models_scanned(self) -> None:
        self.models_scanned += 1

    def report_views_scanned(self) -> None:
        self.views_scanned += 1

    def report_models_dropped(self, model: str) -> None:
        self.filtered_models.append(model)

    def report_views_dropped(self, view: str) -> None:
        self.filtered_views.append(view)


@dataclass
class LookerModel:
    connection: str
    includes: List[str]
    resolved_includes: List[str]

    @staticmethod
    def from_looker_dict(
        looker_model_dict: dict,
        base_folder: str,
        path: str,
        reporter: LookMLSourceReport,
    ) -> "LookerModel":
        connection = looker_model_dict["connection"]
        includes = looker_model_dict["includes"]
        resolved_includes = LookerModel.resolve_includes(
            includes, base_folder, path, reporter
        )

        return LookerModel(
            connection=connection,
            includes=includes,
            resolved_includes=resolved_includes,
        )

    @staticmethod
    def resolve_includes(
        includes: List[str], base_folder: str, path: str, reporter: LookMLSourceReport
    ) -> List[str]:
        """Resolve ``include`` statements in LookML model files to a list of ``.lkml`` files.

        For rules on how LookML ``include`` statements are written, see
            https://docs.looker.com/data-modeling/getting-started/ide-folders#wildcard_examples
        """
        resolved = []
        for inc in includes:
            # Filter out dashboards - we get those through the looker source.
            if (
                inc.endswith(".dashboard")
                or inc.endswith(".dashboard.lookml")
                or inc.endswith(".dashboard.lkml")
            ):
                logger.debug(f"include '{inc}' is a dashboard, skipping it")
                continue

            # Massage the looker include into a valid glob wildcard expression
            if inc.startswith("/"):
                glob_expr = f"{base_folder}{inc}"
            else:
                # Need to handle a relative path.
                glob_expr = str(pathlib.Path(path).parent / inc)
            # "**" matches an arbitrary number of directories in LookML
            outputs = sorted(
                glob.glob(glob_expr, recursive=True)
                + glob.glob(f"{glob_expr}.lkml", recursive=True)
            )
            if "*" not in inc and not outputs:
                reporter.report_failure(path, f"cannot resolve include {inc}")
            elif not outputs:
                reporter.report_failure(
                    path, f"did not resolve anything for wildcard include {inc}"
                )

            resolved.extend(outputs)
        return resolved


@dataclass
class LookerViewFile:
    absolute_file_path: str
    connection: Optional[str]
    includes: List[str]
    resolved_includes: List[str]
    views: List[Dict]

    @staticmethod
    def from_looker_dict(
        absolute_file_path: str,
        looker_view_file_dict: dict,
        base_folder: str,
        reporter: LookMLSourceReport,
    ) -> "LookerViewFile":
        includes = looker_view_file_dict.get("includes", [])
        resolved_includes = LookerModel.resolve_includes(
            includes, base_folder, absolute_file_path, reporter
        )
        views = looker_view_file_dict.get("views", [])

        return LookerViewFile(
            absolute_file_path=absolute_file_path,
            connection=None,
            includes=includes,
            resolved_includes=resolved_includes,
            views=views,
        )


class LookerViewFileLoader:
    """
    Loads the looker viewfile at a :path and caches the LookerViewFile in memory
    This is to avoid reloading the same file off of disk many times during the recursive include resolution process
    """

    def __init__(self, base_folder: str, reporter: LookMLSourceReport) -> None:
        self.viewfile_cache: Dict[str, LookerViewFile] = {}
        self._base_folder = base_folder
        self.reporter = reporter

    def is_view_seen(self, path: str) -> bool:
        return path in self.viewfile_cache

    def _load_viewfile(
        self, path: str, reporter: LookMLSourceReport
    ) -> Optional[LookerViewFile]:
        if self.is_view_seen(path):
            return self.viewfile_cache[path]

        try:
            with open(path, "r") as file:
                parsed = lkml.load(file)
                looker_viewfile = LookerViewFile.from_looker_dict(
                    path, parsed, self._base_folder, reporter
                )
                logger.debug(f"adding viewfile for path {path} to the cache")
                self.viewfile_cache[path] = looker_viewfile
                return looker_viewfile
        except Exception as e:
            self.reporter.report_failure(path, f"failed to load view file: {e}")
            return None

    def load_viewfile(
        self, path: str, connection: str, reporter: LookMLSourceReport
    ) -> Optional[LookerViewFile]:
        viewfile = self._load_viewfile(path, reporter)
        if viewfile is None:
            return None

        return replace(viewfile, connection=connection)


class ViewFieldType(Enum):
    DIMENSION = "Dimension"
    DIMENSION_GROUP = "Dimension Group"
    MEASURE = "Measure"


@dataclass
class ViewField:
    name: str
    type: str
    description: str
    field_type: ViewFieldType
    is_primary_key: bool = False


@dataclass
class LookerView:
    absolute_file_path: str
    connection: str
    view_name: str
    sql_table_names: List[str]
    fields: List[ViewField]

    @classmethod
    def _import_sql_parser_cls(cls, sql_parser_path: str) -> Type[SQLParser]:
        assert "." in sql_parser_path, "sql_parser-path must contain a ."
        module_name, cls_name = sql_parser_path.rsplit(".", 1)
        import sys

        logger.info(sys.path)
        parser_cls = getattr(importlib.import_module(module_name), cls_name)
        if not issubclass(parser_cls, SQLParser):
            raise ValueError(f"must be derived from {SQLParser}; got {parser_cls}")

        return parser_cls

    @classmethod
    def _get_sql_table_names(cls, sql: str, sql_parser_path: str) -> List[str]:
        parser_cls = cls._import_sql_parser_cls(sql_parser_path)

        sql_table_names: List[str] = parser_cls(sql).get_tables()

        # Remove quotes from table names
        sql_table_names = [t.replace('"', "") for t in sql_table_names]
        sql_table_names = [t.replace("`", "") for t in sql_table_names]

        return sql_table_names

    @classmethod
    def _get_fields(
        cls, field_list: List[Dict], type_cls: ViewFieldType
    ) -> List[ViewField]:
        fields = []
        for field_dict in field_list:
            is_primary_key = field_dict.get("primary_key", "no") == "yes"
            name = field_dict["name"]
            native_type = field_dict.get("type", "string")
            description = field_dict.get("description", "")
            field = ViewField(
                name=name,
                type=native_type,
                description=description,
                is_primary_key=is_primary_key,
                field_type=type_cls,
            )
            fields.append(field)
        return fields

    @classmethod
    def from_looker_dict(
        cls,
        looker_view: dict,
        connection: str,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        reporter: LookMLSourceReport,
        parse_table_names_from_sql: bool = False,
        sql_parser_path: str = "datahub.utilities.sql_parser.DefaultSQLParser",
    ) -> Optional["LookerView"]:
        view_name = looker_view["name"]
        logger.debug(f"Handling view {view_name}")

        # The sql_table_name might be defined in another view and this view is extending that view,
        # so we resolve this field while taking that into account.
        sql_table_name: Optional[str] = LookerView.get_including_extends(
            view_name,
            looker_view,
            connection,
            looker_viewfile,
            looker_viewfile_loader,
            "sql_table_name",
            reporter,
        )

        # Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
        sql_table_name = (
            sql_table_name.replace('"', "").replace("`", "")
            if sql_table_name is not None
            else None
        )
        derived_table = looker_view.get("derived_table", None)

        dimensions = cls._get_fields(
            looker_view.get("dimensions", []), ViewFieldType.DIMENSION
        )
        dimension_groups = cls._get_fields(
            looker_view.get("dimension_groups", []), ViewFieldType.DIMENSION_GROUP
        )
        measures = cls._get_fields(
            looker_view.get("measures", []), ViewFieldType.MEASURE
        )
        fields: List[ViewField] = dimensions + dimension_groups + measures

        # Parse SQL from derived tables to extract dependencies
        if derived_table is not None:
            sql_table_names = []
            if parse_table_names_from_sql and "sql" in derived_table:
                # Get the list of tables in the query
                sql_table_names = cls._get_sql_table_names(
                    derived_table["sql"], sql_parser_path
                )

            return LookerView(
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                view_name=view_name,
                sql_table_names=sql_table_names,
                fields=fields,
            )

        # If not a derived table, then this view essentially wraps an existing
        # object in the database.
        if sql_table_name is not None:
            # If sql_table_name is set, there is a single dependency in the view, on the sql_table_name.
            sql_table_names = [sql_table_name]
        else:
            # Otherwise, default to the view name as per the docs:
            # https://docs.looker.com/reference/view-params/sql_table_name-for-view
            sql_table_names = [view_name]

        output_looker_view = LookerView(
            absolute_file_path=looker_viewfile.absolute_file_path,
            connection=connection,
            view_name=view_name,
            sql_table_names=sql_table_names,
            fields=fields,
        )
        return output_looker_view

    @classmethod
    def resolve_extends_view_name(
        cls,
        connection: str,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        target_view_name: str,
        reporter: LookMLSourceReport,
    ) -> Optional[dict]:
        # The view could live in the same file.
        for raw_view in looker_viewfile.views:
            raw_view_name = raw_view["name"]
            if raw_view_name == target_view_name:
                return raw_view

        # Or it could live in one of the included files. We do not know which file the base view
        # lives in, so we try them all!
        for include in looker_viewfile.resolved_includes:
            included_looker_viewfile = looker_viewfile_loader.load_viewfile(
                include, connection, reporter
            )
            if not included_looker_viewfile:
                logger.warning(
                    f"unable to load {include} (included from {looker_viewfile.absolute_file_path})"
                )
                continue
            for raw_view in included_looker_viewfile.views:
                raw_view_name = raw_view["name"]
                # Make sure to skip loading view we are currently trying to resolve
                if raw_view_name == target_view_name:
                    return raw_view

        return None

    @classmethod
    def get_including_extends(
        cls,
        view_name: str,
        looker_view: dict,
        connection: str,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
        field: str,
        reporter: LookMLSourceReport,
    ) -> Optional[Any]:
        extends = list(
            itertools.chain.from_iterable(
                looker_view.get("extends", looker_view.get("extends__all", []))
            )
        )

        # First, check the current view.
        if field in looker_view:
            return looker_view[field]

        # Then, check the views this extends, following Looker's precedence rules.
        for extend in reversed(extends):
            assert extend != view_name, "a view cannot extend itself"
            extend_view = LookerView.resolve_extends_view_name(
                connection, looker_viewfile, looker_viewfile_loader, extend, reporter
            )
            if not extend_view:
                raise NameError(
                    f"failed to resolve extends view {extend} in view {view_name} of file {looker_viewfile.absolute_file_path}"
                )
            if field in extend_view:
                return extend_view[field]

        return None


field_type_mapping = {
    **POSTGRES_TYPES_MAP,
    **SNOWFLAKE_TYPES_MAP,
    "date": DateTypeClass,
    "date_day_of_month": NumberTypeClass,
    "date_day_of_week": EnumTypeClass,
    "date_day_of_week_index": EnumTypeClass,
    "date_fiscal_month_num": NumberTypeClass,
    "date_fiscal_quarter": DateTypeClass,
    "date_fiscal_quarter_of_year": EnumTypeClass,
    "date_hour": TimeTypeClass,
    "date_hour_of_day": NumberTypeClass,
    "date_month": DateTypeClass,
    "date_month_num": NumberTypeClass,
    "date_month_name": EnumTypeClass,
    "date_quarter": DateTypeClass,
    "date_quarter_of_year": EnumTypeClass,
    "date_time": TimeTypeClass,
    "date_time_of_day": TimeTypeClass,
    "date_microsecond": TimeTypeClass,
    "date_millisecond": TimeTypeClass,
    "date_minute": TimeTypeClass,
    "date_raw": TimeTypeClass,
    "date_second": TimeTypeClass,
    "date_week": TimeTypeClass,
    "date_year": DateTypeClass,
    "date_day_of_year": NumberTypeClass,
    "date_week_of_year": NumberTypeClass,
    "date_fiscal_year": DateTypeClass,
    "duration_day": StringTypeClass,
    "duration_hour": StringTypeClass,
    "duration_minute": StringTypeClass,
    "duration_month": StringTypeClass,
    "duration_quarter": StringTypeClass,
    "duration_second": StringTypeClass,
    "duration_week": StringTypeClass,
    "duration_year": StringTypeClass,
    "distance": NumberTypeClass,
    "duration": NumberTypeClass,
    "location": UnionTypeClass,
    "number": NumberTypeClass,
    "string": StringTypeClass,
    "tier": EnumTypeClass,
    "time": TimeTypeClass,
    "unquoted": StringTypeClass,
    "yesno": BooleanTypeClass,
    "zipcode": EnumTypeClass,
    "int": NumberTypeClass,
    "average": NumberTypeClass,
    "average_distinct": NumberTypeClass,
    "count": NumberTypeClass,
    "count_distinct": NumberTypeClass,
    "list": ArrayTypeClass,
    "max": NumberTypeClass,
    "median": NumberTypeClass,
    "median_distinct": NumberTypeClass,
    "min": NumberTypeClass,
    "percent_of_previous": NumberTypeClass,
    "percent_of_total": NumberTypeClass,
    "percentile": NumberTypeClass,
    "percentile_distinct": NumberTypeClass,
    "running_total": NumberTypeClass,
    "sum": NumberTypeClass,
    "sum_distinct": NumberTypeClass,
}


class LookMLSource(Source):
    source_config: LookMLSourceConfig
    reporter: LookMLSourceReport

    def __init__(self, config: LookMLSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.reporter = LookMLSourceReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = LookMLSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _load_model(self, path: str) -> LookerModel:
        with open(path, "r") as file:
            parsed = lkml.load(file)
            looker_model = LookerModel.from_looker_dict(
                parsed, str(self.source_config.base_folder), path, self.reporter
            )
        return looker_model

    def _construct_datalineage_urn(self, sql_table_name: str, connection: str) -> str:
        platform = self._get_platform_based_on_connection(connection)

        # Check if table name matches cascading derived tables pattern (same platform)
        if re.fullmatch(r"\w+\.SQL_TABLE_NAME", sql_table_name):
            platform_name = self.source_config.platform_name
            sql_table_name = sql_table_name.lower().split(".")[0]
        # Check if table database is in platform name (upstream platform)
        elif "." in platform:
            platform_name, database_name = platform.lower().split(".", maxsplit=1)
            sql_table_name = f"{database_name}.{sql_table_name}".lower()
        else:
            platform_name = platform.lower()
            sql_table_name = sql_table_name.lower()

        return builder.make_dataset_urn(
            platform_name, sql_table_name, self.source_config.env
        )

    def _get_platform_based_on_connection(self, connection: str) -> str:
        if connection in self.source_config.connection_to_platform_map:
            return self.source_config.connection_to_platform_map[connection]
        else:
            raise Exception(
                f"Could not find a platform for looker view with connection: {connection}"
            )

    def _get_upstream_lineage(self, looker_view: LookerView) -> UpstreamLineage:
        upstreams = []
        for sql_table_name in looker_view.sql_table_names:

            sql_table_name = sql_table_name.replace('"', "").replace("`", "")

            upstream = UpstreamClass(
                dataset=self._construct_datalineage_urn(
                    sql_table_name, looker_view.connection
                ),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstreams.append(upstream)

        upstream_lineage = UpstreamLineage(upstreams=upstreams)

        return upstream_lineage

    def _get_field_type(self, native_type: str) -> SchemaFieldDataType:

        type_class = field_type_mapping.get(native_type)

        if type_class is None:

            # attempt Postgres modified type
            type_class = resolve_postgres_modified_type(native_type)

        # if still not found, report a warning
        if type_class is None:
            self.reporter.report_warning(
                native_type,
                f"The type '{native_type}' is not recognized for field type, setting as NullTypeClass.",
            )
            type_class = NullTypeClass

        data_type = SchemaFieldDataType(type=type_class())
        return data_type

    def _get_fields_and_primary_keys(
        self, looker_view: LookerView
    ) -> Tuple[List[SchemaField], List[str]]:
        fields: List[SchemaField] = []
        primary_keys: List = []
        for field in looker_view.fields:
            schema_field = SchemaField(
                fieldPath=field.name,
                type=self._get_field_type(field.type),
                nativeDataType=field.type,
                description=f"{field.field_type.value}. {field.description}",
            )
            fields.append(schema_field)
            if field.is_primary_key:
                primary_keys.append(schema_field.fieldPath)
        return fields, primary_keys

    def _get_schema(self, looker_view: LookerView) -> SchemaMetadataClass:
        fields, primary_keys = self._get_fields_and_primary_keys(looker_view)
        schema_metadata = SchemaMetadata(
            schemaName=looker_view.view_name,
            platform=f"urn:li:dataPlatform:{self.source_config.platform_name}",
            version=0,
            fields=fields,
            primaryKeys=primary_keys,
            hash="",
            platformSchema=OtherSchema(rawSchema="looker-view"),
        )
        return schema_metadata

    def _build_dataset_mce(self, looker_view: LookerView) -> MetadataChangeEvent:
        """
        Creates MetadataChangeEvent for the dataset, creating upstream lineage links
        """
        logger.debug(f"looker_view = {looker_view.view_name}")
        dataset_name = looker_view.view_name

        # Sanitize the urn creation.
        dataset_name = dataset_name.replace('"', "").replace("`", "")
        dataset_snapshot = DatasetSnapshot(
            urn=builder.make_dataset_urn(
                self.source_config.platform_name, dataset_name, self.source_config.env
            ),
            aspects=[],  # we append to this list later on
        )
        dataset_snapshot.aspects.append(Status(removed=False))
        dataset_snapshot.aspects.append(self._get_upstream_lineage(looker_view))
        dataset_snapshot.aspects.append(self._get_schema(looker_view))

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

        return mce

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        viewfile_loader = LookerViewFileLoader(
            str(self.source_config.base_folder), self.reporter
        )

        # some views can be mentioned by multiple 'include' statements, so this set is used to prevent
        # creating duplicate MCE messages
        views_with_workunits: Set[str] = set()

        # The ** means "this directory and all subdirectories", and hence should
        # include all the files we want.
        model_files = sorted(self.source_config.base_folder.glob("**/*.model.lkml"))

        for file_path in model_files:
            self.reporter.report_models_scanned()
            model_name = file_path.stem

            if not self.source_config.model_pattern.allowed(model_name):
                self.reporter.report_models_dropped(model_name)
                continue
            try:
                logger.debug(f"Attempting to load model: {file_path}")
                model = self._load_model(str(file_path))
            except Exception as e:
                self.reporter.report_warning(
                    model_name, f"unable to load Looker model at {file_path}: {repr(e)}"
                )
                continue

            for include in model.resolved_includes:
                if include in views_with_workunits:
                    logger.debug(f"view '{include}' already processed, skipping it")
                    continue

                logger.debug(f"Attempting to load view file: {include}")
                looker_viewfile = viewfile_loader.load_viewfile(
                    include, model.connection, self.reporter
                )
                if looker_viewfile is not None:
                    for raw_view in looker_viewfile.views:
                        self.reporter.report_views_scanned()
                        try:
                            maybe_looker_view = LookerView.from_looker_dict(
                                raw_view,
                                model.connection,
                                looker_viewfile,
                                viewfile_loader,
                                self.reporter,
                                self.source_config.parse_table_names_from_sql,
                                self.source_config.sql_parser,
                            )
                        except Exception as e:
                            self.reporter.report_warning(
                                include,
                                f"unable to load Looker view {raw_view}: {repr(e)}",
                            )
                            continue
                        if maybe_looker_view:
                            if self.source_config.view_pattern.allowed(
                                maybe_looker_view.view_name
                            ):
                                mce = self._build_dataset_mce(maybe_looker_view)
                                workunit = MetadataWorkUnit(
                                    id=f"lookml-{maybe_looker_view.view_name}", mce=mce
                                )
                                self.reporter.report_workunit(workunit)
                                views_with_workunits.add(include)
                                yield workunit
                            else:
                                self.reporter.report_views_dropped(
                                    maybe_looker_view.view_name
                                )

    def get_report(self):
        return self.reporter

    def close(self):
        pass
