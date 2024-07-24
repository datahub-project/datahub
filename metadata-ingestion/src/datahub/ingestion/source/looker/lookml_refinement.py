import copy
import itertools
import logging
from typing import ClassVar, Dict, List, Set

from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import LookerModel
from datahub.ingestion.source.looker.looker_view_id_cache import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import (
    NAME,
    LookMLSourceConfig,
    LookMLSourceReport,
)

logger = logging.getLogger(__name__)


class LookerRefinementResolver:
    """
    Refinements are a way to "edit" an existing view or explore.
    Refer: https://cloud.google.com/looker/docs/lookml-refinements

    A refinement to an existing view/explore is only applied if it's refinement is reachable from include files in a
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
