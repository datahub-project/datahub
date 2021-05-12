import glob
import logging
import re
import sys
import time
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from dataclasses import replace
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

if sys.version_info[1] >= 7:
    import lkml
else:
    logging.warning("This plugin requres Python 3.7 or newer.")
from sql_metadata import get_query_tables

from datahub.configuration import ConfigModel
from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp, Status
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


class LookMLSourceConfig(ConfigModel):  # pragma: no cover
    base_folder: str
    connection_to_platform_map: Dict[str, str]
    platform_name: str = "looker_views"
    actor: str = "urn:li:corpuser:etl"
    model_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    view_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    env: str = "PROD"
    parse_table_names_from_sql: bool = False


@dataclass
class LookMLSourceReport(SourceReport):  # pragma: no cover
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
class LookerModel:  # pragma: no cover
    connection: str
    includes: List[str]
    resolved_includes: List[str]

    @staticmethod
    def from_looker_dict(looker_model_dict: dict, base_folder: str) -> "LookerModel":
        connection = looker_model_dict["connection"]
        includes = looker_model_dict["includes"]
        resolved_includes = LookerModel.resolve_includes(includes, base_folder)

        return LookerModel(
            connection=connection,
            includes=includes,
            resolved_includes=resolved_includes,
        )

    @staticmethod
    def resolve_includes(includes: List, base_folder: str) -> List[str]:
        resolved = []
        for inc in includes:
            # Massage the looker include into a valid glob wildcard expression
            glob_expr = f"{base_folder}/{inc}"
            outputs = glob.glob(glob_expr)
            resolved.extend(outputs)
        return resolved


@dataclass
class LookerViewFile:  # pragma: no cover
    absolute_file_path: str
    connection: Optional[str]
    includes: List[str]
    resolved_includes: List[str]
    views: List[Dict]

    @staticmethod
    def from_looker_dict(
        absolute_file_path: str, looker_view_file_dict: dict, base_folder: str
    ) -> "LookerViewFile":
        includes = looker_view_file_dict.get("includes", [])
        resolved_includes = LookerModel.resolve_includes(includes, base_folder)
        views = looker_view_file_dict.get("views", [])

        return LookerViewFile(
            absolute_file_path=absolute_file_path,
            connection=None,
            includes=includes,
            resolved_includes=resolved_includes,
            views=views,
        )


class LookerViewFileLoader:  # pragma: no cover
    """
    Loads the looker viewfile at a :path and caches the LookerViewFile in memory
    This is to avoid reloading the same file off of disk many times during the recursive include resolution process
    """

    def __init__(self, base_folder: str) -> None:
        self.viewfile_cache: Dict[str, LookerViewFile] = {}
        self._base_folder = base_folder

    def is_view_seen(self, path: str) -> bool:
        return path in self.viewfile_cache

    def _load_viewfile(self, path: str) -> Optional[LookerViewFile]:
        if self.is_view_seen(path):
            return self.viewfile_cache[path]

        try:
            with open(path, "r") as file:
                parsed = lkml.load(file)
                looker_viewfile = LookerViewFile.from_looker_dict(
                    path, parsed, self._base_folder
                )
                self.viewfile_cache[path] = looker_viewfile
                return looker_viewfile
        except Exception:
            logger.warning(f"Error processing view file {path}. Skipping it")
            return None

    def load_viewfile(self, path: str, connection: str) -> Optional[LookerViewFile]:
        viewfile = self._load_viewfile(path)
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
class LookerView:  # pragma: no cover
    absolute_file_path: str
    connection: str
    view_name: str
    sql_table_names: List[str]
    fields: List[ViewField]

    @classmethod
    def _get_sql_table_names(cls, sql: str) -> List[str]:
        sql_tables: List[str] = get_query_tables(sql)

        # Remove temporary tables from WITH statements
        sql_table_names = [
            t
            for t in sql_tables
            if not re.search(
                fr"WITH(.*,)?\s+{t}(\s*\([\w\s,]+\))?\s+AS\s+\(",
                sql,
                re.IGNORECASE | re.DOTALL,
            )
        ]

        # Remove quotes from tables
        sql_table_names = [t.replace('"', "") for t in sql_table_names]

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
        parse_table_names_from_sql: bool = False,
    ) -> Optional["LookerView"]:
        view_name = looker_view["name"]
        sql_table_name = looker_view.get("sql_table_name", None)
        # Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
        sql_table_name = (
            sql_table_name.replace('"', "") if sql_table_name is not None else None
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
            if parse_table_names_from_sql and "sql" in derived_table:
                # Get the list of tables in the query
                sql_table_names = cls._get_sql_table_names(derived_table["sql"])
            else:
                sql_table_names = []

            return LookerView(
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                view_name=view_name,
                sql_table_names=sql_table_names,
                fields=fields,
            )

        # There is a single dependency in the view, on the sql_table_name
        if sql_table_name is not None:
            return LookerView(
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                view_name=view_name,
                sql_table_names=[sql_table_name],
                fields=fields,
            )

        # The sql_table_name might be defined in another view and this view is extending that view, try to find it
        else:
            extends = looker_view.get("extends", looker_view.get("extends__all", []))
            if len(extends) == 0:
                # The view is malformed, the view is not a derived table, does not contain a sql_table_name or an extends
                logger.warning(
                    f"Skipping malformed with view_name: {view_name} ({looker_viewfile.absolute_file_path}). View should have a sql_table_name if it is not a derived table"
                )
                return None

            extends_to_looker_view = []

            # The base view could live in the same file
            for raw_view in looker_viewfile.views:
                raw_view_name = raw_view["name"]
                # Make sure to skip loading view we are currently trying to resolve
                if raw_view_name != view_name:
                    maybe_looker_view = LookerView.from_looker_dict(
                        raw_view,
                        connection,
                        looker_viewfile,
                        looker_viewfile_loader,
                        parse_table_names_from_sql,
                    )
                    if (
                        maybe_looker_view is not None
                        and maybe_looker_view.view_name in extends
                    ):
                        extends_to_looker_view.append(maybe_looker_view)

            # Or it could live in one of the included files, we do not know which file the base view lives in, try them all!
            for include in looker_viewfile.resolved_includes:
                maybe_looker_viewfile = looker_viewfile_loader.load_viewfile(
                    include, connection
                )
                if maybe_looker_viewfile is not None:
                    for view in looker_viewfile.views:
                        maybe_looker_view = LookerView.from_looker_dict(
                            view,
                            connection,
                            looker_viewfile,
                            looker_viewfile_loader,
                            parse_table_names_from_sql,
                        )
                        if maybe_looker_view is None:
                            continue

                        if (
                            maybe_looker_view is not None
                            and maybe_looker_view.view_name in extends
                        ):
                            extends_to_looker_view.append(maybe_looker_view)

            if len(extends_to_looker_view) != 1:
                logger.warning(
                    f"Skipping malformed view with view_name: {view_name}. View should have a single view in a view inheritance chain with a sql_table_name"
                )
                return None

            output_looker_view = LookerView(
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                view_name=view_name,
                sql_table_names=extends_to_looker_view[0].sql_table_names,
                fields=fields,
            )
            return output_looker_view


class LookMLSource(Source):  # pragma: no cover
    source_config: LookMLSourceConfig
    report = LookMLSourceReport()

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
                parsed, self.source_config.base_folder
            )
        return looker_model

    def _construct_datalineage_urn(self, sql_table_name: str, connection: str) -> str:
        platform = self._get_platform_based_on_connection(connection)
        return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{sql_table_name},{self.source_config.env})"

    def _get_platform_based_on_connection(self, connection: str) -> str:
        if connection in self.source_config.connection_to_platform_map:
            return self.source_config.connection_to_platform_map[connection]
        else:
            raise Exception(
                f"Could not find a platform for looker view with connection: {connection}"
            )

    def _get_upsteam_lineage(
        self, looker_view: LookerView, actor: str, sys_time: int
    ) -> UpstreamLineage:
        upstreams = []
        for sql_table_name in looker_view.sql_table_names:
            upstream = UpstreamClass(
                dataset=self._construct_datalineage_urn(
                    sql_table_name, looker_view.connection
                ),
                auditStamp=AuditStamp(actor=actor, time=sys_time),
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            upstreams.append(upstream)

        upstream_lineage = UpstreamLineage(upstreams=upstreams)

        return upstream_lineage

    def _get_field_type(self, native_type: str) -> SchemaFieldDataType:
        field_type_mapping = {
            "date": DateTypeClass,
            "date_time": TimeTypeClass,
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

        if native_type in field_type_mapping:
            type_class = field_type_mapping[native_type]
        else:
            self.reporter.report_warning(
                native_type,
                f"The type '{native_type}' is not recognised for field type, setting as NullTypeClass.",
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

    def _get_schema(
        self, looker_view: LookerView, actor: str, sys_time: int
    ) -> SchemaMetadataClass:
        fields, primary_keys = self._get_fields_and_primary_keys(looker_view)
        stamp = AuditStamp(time=sys_time, actor=actor)
        schema_metadata = SchemaMetadata(
            schemaName=looker_view.view_name,
            platform=self.source_config.platform_name,
            version=0,
            fields=fields,
            primaryKeys=primary_keys,
            created=stamp,
            lastModified=stamp,
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
        actor = self.source_config.actor
        sys_time = int(time.time()) * 1000

        dataset_snapshot = DatasetSnapshot(
            urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.source_config.platform_name}, {dataset_name}, {self.source_config.env})",
            aspects=[],  # we append to this list later on
        )
        dataset_snapshot.aspects.append(Status(removed=False))
        dataset_snapshot.aspects.append(
            self._get_upsteam_lineage(looker_view, actor, sys_time)
        )
        dataset_snapshot.aspects.append(self._get_schema(looker_view, actor, sys_time))

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)

        return mce

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        viewfile_loader = LookerViewFileLoader(self.source_config.base_folder)

        model_files = sorted(
            f
            for f in glob.glob(
                f"{self.source_config.base_folder}/**/*.model.lkml", recursive=True
            )
        )
        for file_path in model_files:
            model_name = Path(file_path).stem
            self.reporter.report_models_scanned()
            if not self.source_config.model_pattern.allowed(model_name):
                self.reporter.report_models_dropped(model_name)
                continue
            try:
                model = self._load_model(file_path)
            except Exception:
                self.reporter.report_warning(
                    "LookML", f"unable to parse Looker model: {file_path}"
                )
                continue

            for include in model.resolved_includes:
                is_view_seen = viewfile_loader.is_view_seen(include)
                if is_view_seen:
                    continue
                looker_viewfile = viewfile_loader.load_viewfile(
                    include, model.connection
                )
                if looker_viewfile is not None:
                    for raw_view in looker_viewfile.views:
                        maybe_looker_view = LookerView.from_looker_dict(
                            raw_view,
                            model.connection,
                            looker_viewfile,
                            viewfile_loader,
                            self.source_config.parse_table_names_from_sql,
                        )
                        if maybe_looker_view:
                            self.reporter.report_views_scanned()
                            if self.source_config.view_pattern.allowed(
                                maybe_looker_view.view_name
                            ):
                                mce = self._build_dataset_mce(maybe_looker_view)
                                workunit = MetadataWorkUnit(
                                    id=f"lookml-{maybe_looker_view.view_name}", mce=mce
                                )
                                self.reporter.report_workunit(workunit)
                                yield workunit
                            else:
                                self.reporter.report_views_dropped(
                                    maybe_looker_view.view_name
                                )

    def get_report(self):
        return self.report

    def close(self):
        pass
