import logging
from typing import Dict, List, Optional

from datahub.ingestion.source.looker.looker_common import LookerViewId, ViewFieldValue
from datahub.ingestion.source.looker.looker_config import LookerConnectionDefinition
from datahub.ingestion.source.looker.looker_dataclasses import LookerModel
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import (
    BASE_PROJECT_NAME,
    NAME,
    LookMLSourceReport,
)

logger = logging.getLogger(__name__)


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

    This cache can be used for many other use case.
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
                        if include.project != BASE_PROJECT_NAME
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
