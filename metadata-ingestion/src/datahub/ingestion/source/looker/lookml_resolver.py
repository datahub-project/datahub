import ast
import copy
import itertools
import logging
import pathlib
from dataclasses import replace
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, cast

from liquid import Template, Undefined
from liquid.exceptions import LiquidSyntaxError

from datahub.ingestion.source.looker.lkml_patched import load_lkml
from datahub.ingestion.source.looker.looker_common import (
    LookerConnectionDefinition,
    LookerViewId,
    ViewField,
    ViewFieldValue,
)
from datahub.ingestion.source.looker.lookml_config import (
    _BASE_PROJECT_NAME,
    _EXPLORE_FILE_EXTENSION,
    _VIEW_FILE_EXTENSION,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.lookml_dataclasses import (
    LookerModel,
    LookerViewFile,
)
from datahub.ingestion.source.looker.urn_functions import get_qualified_table_name
from datahub.sql_parsing.sqlglot_lineage import ColumnRef

logger = logging.getLogger(__name__)

NAME: str = "name"

DERIVED_TABLE_PREFIX = r".sql_table_name"


def _get_derived_view_urn(
    qualified_table_name: str,
    looker_view_id_cache: "LookerViewIdCache",
    base_folder_path: str,
    config: LookMLSourceConfig,
) -> Optional[str]:

    view_name: str = qualified_table_name.split(".")[
        1
    ]  # it is in format of part1.view_name.sql_table_name

    looker_view_id: Optional[LookerViewId] = looker_view_id_cache.get_looker_view_id(
        view_name=view_name,
        base_folder_path=base_folder_path,
    )

    if looker_view_id is None:
        return None

    return looker_view_id.get_urn(config=config)


def resolve_derived_view_urn(
    looker_view_id_cache: "LookerViewIdCache",
    base_folder_path: str,
    config: LookMLSourceConfig,
    fields: List[ViewField],
    upstream_urns: List[str],
) -> Tuple[List[ViewField], List[str]]:
    for field in fields:
        # if list is not list of ColumnRef then continue
        if field.upstream_fields and not isinstance(
            field.upstream_fields[0], ColumnRef
        ):
            continue

        upstream_fields: List[ColumnRef] = []
        for col_ref in cast(List[ColumnRef], field.upstream_fields):
            if DERIVED_TABLE_PREFIX in col_ref.table.lower():
                view_urn: Optional[str] = _get_derived_view_urn(
                    qualified_table_name=get_qualified_table_name(col_ref.table),
                    looker_view_id_cache=looker_view_id_cache,
                    base_folder_path=base_folder_path,
                    config=config,
                )

                if view_urn is None:
                    logger.warning(
                        f"Not able to resolve to derived view for view urn {col_ref.table}"
                    )
                    continue

                upstream_fields.append(ColumnRef(table=view_urn, column=col_ref.column))
            else:
                upstream_fields.append(col_ref)

        field.upstream_fields = upstream_fields

    # Regenerate upstream_urns if .sql_table_name is present
    new_upstream_urns: List[str] = []
    for urn in upstream_urns:
        if DERIVED_TABLE_PREFIX in urn.lower():
            view_urn = _get_derived_view_urn(
                qualified_table_name=get_qualified_table_name(urn),
                looker_view_id_cache=looker_view_id_cache,
                base_folder_path=base_folder_path,
                config=config,
            )

            if view_urn is None:
                logger.warning(
                    f"Not able to resolve to derived view for view urn {urn}"
                )
                continue

            new_upstream_urns.append(view_urn)
        else:
            new_upstream_urns.append(urn)

    return fields, new_upstream_urns


def determine_view_file_path(base_folder_path: str, absolute_file_path: str) -> str:
    splits: List[str] = absolute_file_path.split(base_folder_path, 1)
    if len(splits) != 2:
        logger.debug(
            f"base_folder_path({base_folder_path}) and absolute_file_path({absolute_file_path}) not matching"
        )
        return ViewFieldValue.NOT_AVAILABLE.value

    file_path: str = splits[1]
    logger.debug(f"file_path={file_path}")

    return file_path.strip(
        "/"
    )  # strip / from path to make it equivalent to source_file attribute of LookerModelExplore API


def resolve_liquid_variable(text: str, liquid_variable: Dict[Any, Any]) -> str:
    # Set variable value to NULL if not present in liquid_variable dictionary
    Undefined.__str__ = lambda instance: "NULL"  # type: ignore
    try:
        # Resolve liquid template
        return Template(text).render(liquid_variable)
    except LiquidSyntaxError as e:
        logger.warning(f"Unsupported liquid template encountered. error [{e.message}]")
        # TODO: There are some tag specific to looker and python-liquid library does not understand them. currently
        #  we are not parsing such liquid template.
        #
        # See doc: https://cloud.google.com/looker/docs/templated-filters and look for { % condition region %}
        # order.region { % endcondition %}

    return text


class LookerViewFileLoader:
    """
    Loads the looker viewfile at a :path and caches the LookerViewFile in memory
    This is to avoid reloading the same file off of disk many times during the recursive include resolution process
    """

    def __init__(
        self,
        root_project_name: Optional[str],
        base_projects_folder: Dict[str, pathlib.Path],
        reporter: LookMLSourceReport,
        liquid_variable: Dict[Any, Any],
    ) -> None:
        self.viewfile_cache: Dict[str, LookerViewFile] = {}
        self._root_project_name = root_project_name
        self._base_projects_folder = base_projects_folder
        self.reporter = reporter
        self.liquid_variable = liquid_variable

    def is_view_seen(self, path: str) -> bool:
        return path in self.viewfile_cache

    def _load_viewfile(
        self, project_name: str, path: str, reporter: LookMLSourceReport
    ) -> Optional[LookerViewFile]:
        # always fully resolve paths to simplify de-dup
        path = str(pathlib.Path(path).resolve())
        allowed_extensions = [_VIEW_FILE_EXTENSION, _EXPLORE_FILE_EXTENSION]
        matched_any_extension = [
            match for match in [path.endswith(x) for x in allowed_extensions] if match
        ]
        if not matched_any_extension:
            # not a view file
            logger.debug(
                f"Skipping file {path} because it doesn't appear to be a view file. Matched extensions {allowed_extensions}"
            )
            return None

        if self.is_view_seen(str(path)):
            return self.viewfile_cache[path]

        try:
            with open(path) as file:
                raw_file_content = file.read()
        except Exception as e:
            self.reporter.report_failure(path, f"failed to load view file: {e}")
            return None
        try:
            logger.debug(f"Loading viewfile {path}")
            parsed = load_lkml(path)

            # replace any liquid variable
            parsed_text: str = resolve_liquid_variable(
                text=str(parsed),
                liquid_variable=self.liquid_variable,
            )
            parsed = ast.literal_eval(parsed_text)

            looker_viewfile = LookerViewFile.from_looker_dict(
                absolute_file_path=path,
                looker_view_file_dict=parsed,
                project_name=project_name,
                root_project_name=self._root_project_name,
                base_projects_folder=self._base_projects_folder,
                raw_file_content=raw_file_content,
                reporter=reporter,
            )
            logger.debug(f"adding viewfile for path {path} to the cache")
            self.viewfile_cache[path] = looker_viewfile
            return looker_viewfile
        except Exception as e:
            self.reporter.report_failure(path, f"failed to load view file: {e}")
            return None

    def load_viewfile(
        self,
        path: str,
        project_name: str,
        connection: Optional[LookerConnectionDefinition],
        reporter: LookMLSourceReport,
    ) -> Optional[LookerViewFile]:
        viewfile = self._load_viewfile(
            project_name=project_name,
            path=path,
            reporter=reporter,
        )
        if viewfile is None:
            return None

        return replace(viewfile, connection=connection)


class LookerRefinementResolver:
    """
    Refinements are a way to "edit" an existing view or explore.
    Refer: https://cloud.google.com/looker/docs/lookml-refinements

    A refinement to an existing view/explore is only applied if it' refinement is reachable from include files in a
    model. For refinement applied order please refer:
    https://cloud.google.com/looker/docs/lookml-refinements#refinements_are_applied_in_order
    """

    REFINEMENT_PREFIX: ClassVar[str] = "+"
    DIMENSIONS: ClassVar[str] = "dimensions"
    MEASURES: ClassVar[str] = "measures"
    DIMENSION_GROUPS: ClassVar[str] = "dimension_groups"
    EXTENDS: ClassVar[str] = "extends"
    EXTENDS_ALL: ClassVar[str] = "extends__all"

    looker_model: LookerModel
    looker_viewfile_loader: LookerViewFileLoader
    connection_definition: LookerConnectionDefinition
    source_config: LookMLSourceConfig
    reporter: LookMLSourceReport
    view_refinement_cache: Dict[
        str, dict
    ]  # Map of view-name as key, and it is raw view dictionary after applying refinement process
    explore_refinement_cache: Dict[
        str, dict
    ]  # Map of explore-name as key, and it is raw view dictionary after applying refinement process

    def __init__(
        self,
        looker_model: LookerModel,
        looker_viewfile_loader: LookerViewFileLoader,
        connection_definition: LookerConnectionDefinition,
        source_config: LookMLSourceConfig,
        reporter: LookMLSourceReport,
    ):
        self.looker_model = looker_model
        self.looker_viewfile_loader = looker_viewfile_loader
        self.connection_definition = connection_definition
        self.source_config = source_config
        self.reporter = reporter
        self.view_refinement_cache = {}
        self.explore_refinement_cache = {}

    @staticmethod
    def is_refinement(view_name: str) -> bool:
        return view_name.startswith(LookerRefinementResolver.REFINEMENT_PREFIX)

    @staticmethod
    def merge_column(
        original_dict: dict, refinement_dict: dict, key: str
    ) -> List[dict]:
        """
        Merge a dimension/measure/other column with one from a refinement.
        This follows the process documented at https://help.looker.com/hc/en-us/articles/4419773929107-LookML-refinements
        """
        merge_column: List[dict] = []
        original_value: List[dict] = original_dict.get(key, [])
        refine_value: List[dict] = refinement_dict.get(key, [])
        # name is required field, not going to be None
        original_column_map = {column[NAME]: column for column in original_value}
        refine_column_map = {column[NAME]: column for column in refine_value}
        for existing_column_name in original_column_map:
            existing_column = original_column_map[existing_column_name]
            refine_column = refine_column_map.get(existing_column_name)
            if refine_column is not None:
                existing_column.update(refine_column)

            merge_column.append(existing_column)

        # merge any remaining column from refine_column_map
        for new_column_name in refine_column_map:
            if new_column_name not in original_column_map:
                merge_column.append(refine_column_map[new_column_name])

        return merge_column

    @staticmethod
    def merge_and_set_column(
        new_raw_view: dict, refinement_view: dict, key: str
    ) -> None:
        merged_column = LookerRefinementResolver.merge_column(
            new_raw_view, refinement_view, key
        )
        if merged_column:
            new_raw_view[key] = merged_column

    @staticmethod
    def merge_refinements(raw_view: dict, refinement_views: List[dict]) -> dict:
        """
        Iterate over refinement_views and merge parameter of each view with raw_view.
        Detail of merging order can be found at https://cloud.google.com/looker/docs/lookml-refinements
        """
        new_raw_view: dict = copy.deepcopy(raw_view)

        for refinement_view in refinement_views:
            # Merge dimension and measure
            # TODO: low priority: handle additive parameters
            # https://cloud.google.com/looker/docs/lookml-refinements#some_parameters_are_additive

            # Merge Dimension
            LookerRefinementResolver.merge_and_set_column(
                new_raw_view, refinement_view, LookerRefinementResolver.DIMENSIONS
            )
            # Merge Measure
            LookerRefinementResolver.merge_and_set_column(
                new_raw_view, refinement_view, LookerRefinementResolver.MEASURES
            )
            # Merge Dimension Group
            LookerRefinementResolver.merge_and_set_column(
                new_raw_view, refinement_view, LookerRefinementResolver.DIMENSION_GROUPS
            )

        return new_raw_view

    def get_refinements(self, views: List[dict], view_name: str) -> List[dict]:
        """
        Refinement syntax for view and explore are same.
        This function can be used to filter out view/explore refinement from raw dictionary list
        """
        view_refinement_name: str = self.REFINEMENT_PREFIX + view_name
        refined_views: List[dict] = []

        for raw_view in views:
            if view_refinement_name == raw_view[NAME]:
                refined_views.append(raw_view)

        return refined_views

    def get_refinement_from_model_includes(self, view_name: str) -> List[dict]:
        refined_views: List[dict] = []

        for include in self.looker_model.resolved_includes:
            included_looker_viewfile = self.looker_viewfile_loader.load_viewfile(
                include.include,
                include.project,
                self.connection_definition,
                self.reporter,
            )

            if not included_looker_viewfile:
                continue

            refined_views.extend(
                self.get_refinements(included_looker_viewfile.views, view_name)
            )

        return refined_views

    def should_skip_processing(self, raw_view_name: str) -> bool:
        if LookerRefinementResolver.is_refinement(raw_view_name):
            return True

        if self.source_config.process_refinements is False:
            return True

        return False

    def apply_view_refinement(self, raw_view: dict) -> dict:
        """
        Looker process the lkml file in include order and merge the all refinement to original view.
        """
        assert raw_view.get(NAME) is not None

        raw_view_name: str = raw_view[NAME]

        if self.should_skip_processing(raw_view_name):
            return raw_view

        if raw_view_name in self.view_refinement_cache:
            logger.debug(f"Returning applied refined view {raw_view_name} from cache")
            return self.view_refinement_cache[raw_view_name]

        logger.debug(f"Processing refinement for view {raw_view_name}")

        refinement_views: List[dict] = self.get_refinement_from_model_includes(
            raw_view_name
        )

        self.view_refinement_cache[raw_view_name] = self.merge_refinements(
            raw_view, refinement_views
        )

        return self.view_refinement_cache[raw_view_name]

    @staticmethod
    def add_extended_explore(
        raw_explore: dict, refinement_explores: List[Dict]
    ) -> None:
        extended_explores: Set[str] = set()
        for view in refinement_explores:
            extends = list(
                itertools.chain.from_iterable(
                    view.get(
                        LookerRefinementResolver.EXTENDS,
                        view.get(LookerRefinementResolver.EXTENDS_ALL, []),
                    )
                )
            )
            extended_explores.update(extends)

        if extended_explores:  # if it is not empty then add to the original view
            raw_explore[LookerRefinementResolver.EXTENDS] = list(extended_explores)

    def apply_explore_refinement(self, raw_view: dict) -> dict:
        """
        In explore refinement `extends` parameter is additive.
        Refer looker refinement document: https://cloud.google.com/looker/docs/lookml-refinements#additive
        """
        assert raw_view.get(NAME) is not None

        raw_view_name: str = raw_view[NAME]

        if self.should_skip_processing(raw_view_name):
            return raw_view

        if raw_view_name in self.explore_refinement_cache:
            logger.debug(
                f"Returning applied refined explore {raw_view_name} from cache"
            )
            return self.explore_refinement_cache[raw_view_name]

        logger.debug(f"Processing refinement for explore {raw_view_name}")

        refinement_explore: List[dict] = self.get_refinements(
            self.looker_model.explores, raw_view_name
        )

        self.add_extended_explore(raw_view, refinement_explore)

        self.explore_refinement_cache[raw_view_name] = raw_view

        return self.explore_refinement_cache[raw_view_name]


class LookerViewIdCache:
    """
    For view to view lineage we require LookerViewId object to form urn in advance for lineage generation.
    The case where a view is referencing to another view using derived table can be located in this cache.

    Example: Consider a view registration_monthly_phasing has below SQL
            SELECT *

            FROM ${registration_daily_phasing.SQL_TABLE_NAME}

            {% if date_sel._parameter_value == "'Weekly'"%}
                WHERE  DW_EFF_DT < DATEADD(DAY, (-DAYOFWEEK(current_date()) - 1),current_date())
            {% endif %}

    While generating MCPs for registration_monthly_phasing, the connector can look for view id
    of registration_daily_phasing in this cache to generate the lineage between registration_monthly_phasing
    and registration_daily_phasing

    This cache can be used for any other use case.
    """

    looker_model: LookerModel
    looker_viewfile_loader: LookerViewFileLoader
    project_name: str
    model_name: str
    reporter: LookMLSourceReport
    looker_view_id_cache: Dict[
        str, LookerViewId
    ]  # Map of view-name as key, and LookerViewId instance as value

    def __init__(
        self,
        project_name: str,
        model_name: str,
        looker_model: LookerModel,
        looker_viewfile_loader: LookerViewFileLoader,
        reporter: LookMLSourceReport,
    ):
        self.project_name = project_name
        self.model_name = model_name
        self.looker_model = looker_model
        self.looker_viewfile_loader = looker_viewfile_loader
        self.looker_view_id_cache = {}
        self.reporter = reporter

    def get_looker_view_id(
        self,
        view_name: str,
        base_folder_path: str,
        connection: Optional[LookerConnectionDefinition] = None,
    ) -> Optional[LookerViewId]:
        if view_name in self.looker_view_id_cache:
            return self.looker_view_id_cache[view_name]

        for include in self.looker_model.resolved_includes:
            included_looker_viewfile = self.looker_viewfile_loader.load_viewfile(
                path=include.include,
                project_name=include.project,
                reporter=self.reporter,
                connection=connection,
            )

            if included_looker_viewfile is None:
                continue

            for view in included_looker_viewfile.views:
                if view[NAME] == view_name:
                    file_path = determine_view_file_path(
                        base_folder_path, included_looker_viewfile.absolute_file_path
                    )

                    current_project_name: str = (
                        include.project
                        if include.project != _BASE_PROJECT_NAME
                        else self.project_name
                    )

                    looker_view_id: LookerViewId = LookerViewId(
                        project_name=current_project_name,
                        model_name=self.model_name,
                        view_name=view_name,
                        file_path=file_path,
                    )

                    self.looker_view_id_cache[view_name] = looker_view_id
                    return looker_view_id

        return None
