import glob
import logging
import re
import time
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from dataclasses import replace
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import lkml
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

logger = logging.getLogger(__name__)


class LookMLSourceConfig(ConfigModel):
    base_folder: str
    connection_to_platform_map: Dict[str, str]
    platform_name: str = "looker_views"
    actor: str = "urn:li:corpuser:etl"
    model_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    view_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    env: str = "PROD"


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
class LookerViewFile:
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


class LookerViewFileLoader:
    """
    Loads the looker viewfile at a :path and caches the LookerViewFile in memory
    This is to avoid reloading the same file off of disk many times during the recursive include resolution process
    """

    def __init__(self, base_folder: str) -> None:
        self.viewfile_cache: Dict[str, LookerViewFile] = {}
        self._base_folder = base_folder

    def _load_viewfile(self, path: str) -> Optional[LookerViewFile]:
        if path in self.viewfile_cache:
            return self.viewfile_cache[path]

        try:
            with open(path, "r") as file:
                parsed = lkml.load(file)
                looker_viewfile = LookerViewFile.from_looker_dict(
                    path, parsed, self._base_folder
                )
                self.viewfile_cache[path] = looker_viewfile
                return looker_viewfile
        except Exception as e:
            print(e)
            print(f"Error processing view file {path}. Skipping it")
            return None

    def load_viewfile(self, path: str, connection: str) -> Optional[LookerViewFile]:
        viewfile = self._load_viewfile(path)
        if viewfile is None:
            return None

        return replace(viewfile, connection=connection)


@dataclass
class LookerView:
    absolute_file_path: str
    connection: str
    view_name: str
    sql_table_names: List[str]

    @staticmethod
    def from_looker_dict(
        looker_view: dict,
        connection: str,
        looker_viewfile: LookerViewFile,
        looker_viewfile_loader: LookerViewFileLoader,
    ) -> Optional["LookerView"]:
        view_name = looker_view["name"]
        sql_table_name = looker_view.get("sql_table_name", None)
        # Some sql_table_name fields contain quotes like: optimizely."group", just remove the quotes
        sql_table_name = (
            sql_table_name.replace('"', "") if sql_table_name is not None else None
        )
        derived_table = looker_view.get("derived_table", None)

        # Parse SQL from derived tables to extract dependencies
        if derived_table is not None and "sql" in derived_table:
            # TODO: add featre flag to config!
            # Get the list of tables in the query
            sql_tables: List[str] = get_query_tables(derived_table["sql"])

            # Remove temporary tables from WITH statements
            sql_table_names = [
                t
                for t in sql_tables
                if not re.search(
                    f"WITH(.*,)?\s+{t}(\s*\([\w\s,]+\))?\s+AS\s+\(",
                    derived_table["sql"],
                    re.IGNORECASE | re.DOTALL,
                )
            ]

            # Remove quotes from tables
            sql_table_names = [t.replace('"', "") for t in sql_table_names]

            return LookerView(
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                view_name=view_name,
                sql_table_names=sql_table_names,
            )

        # There is a single dependency in the view, on the sql_table_name
        if sql_table_name is not None:
            return LookerView(
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                view_name=view_name,
                sql_table_names=[sql_table_name],
            )

        # The sql_table_name might be defined in another view and this view is extending that view, try to find it
        else:
            extends = looker_view.get("extends", [])
            if len(extends) == 0:
                # The view is malformed, the view is not a derived table, does not contain a sql_table_name or an extends
                print(
                    f"Skipping malformed with view_name: {view_name}. View should have a sql_table_name if it is not a derived table"
                )
                return None

            extends_to_looker_view = []

            # The base view could live in the same file
            for raw_view in looker_viewfile.views:
                raw_view_name = raw_view["name"]
                # Make sure to skip loading view we are currently trying to resolve
                if raw_view_name != view_name:
                    maybe_looker_view = LookerView.from_looker_dict(
                        raw_view, connection, looker_viewfile, looker_viewfile_loader
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
                            view, connection, looker_viewfile, looker_viewfile_loader
                        )
                        if maybe_looker_view is None:
                            continue

                        if (
                            maybe_looker_view is not None
                            and maybe_looker_view.view_name in extends
                        ):
                            extends_to_looker_view.append(maybe_looker_view)

            if len(extends_to_looker_view) != 1:
                print(
                    f"Skipping malformed view with view_name: {view_name}. View should have a single view in a view inheritance chain with a sql_table_name"
                )
                return None

            output_looker_view = LookerView(
                absolute_file_path=looker_viewfile.absolute_file_path,
                connection=connection,
                view_name=view_name,
                sql_table_names=extends_to_looker_view[0].sql_table_names,
            )
            return output_looker_view


class LookMLSource(Source):
    source_config: LookMLSourceConfig
    report = LookMLSourceReport()

    def __init__(self, config: LookMLSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = LookMLSourceReport()

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
            if not self.source_config.model_pattern.allowed(model_name):
                self.report.report_models_dropped(model_name)
                continue
            try:
                model = self._load_model(file_path)
            except Exception:
                self.report.report_warning(
                    "LookML", f"unable to parse Looker model: {file_path}"
                )
                continue

            for include in model.resolved_includes:
                looker_viewfile = viewfile_loader.load_viewfile(
                    include, model.connection
                )
                if looker_viewfile is not None:
                    for raw_view in looker_viewfile.views:
                        maybe_looker_view = LookerView.from_looker_dict(
                            raw_view, model.connection, looker_viewfile, viewfile_loader
                        )
                        if maybe_looker_view:
                            if self.source_config.view_pattern.allowed(
                                maybe_looker_view.view_name
                            ):
                                mce = self._build_dataset_mce(maybe_looker_view)
                                workunit = MetadataWorkUnit(
                                    id=f"lookml-{maybe_looker_view.view_name}", mce=mce
                                )
                                self.report.report_workunit(workunit)
                                self.report.report_views_scanned()
                                yield workunit
                            else:
                                self.report.report_views_dropped(
                                    maybe_looker_view.view_name
                                )
            self.report.report_models_scanned()

    def get_report(self):
        return self.report

    def close(self):
        pass
