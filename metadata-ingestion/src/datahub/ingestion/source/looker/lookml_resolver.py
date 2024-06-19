import logging
import re
from typing import Dict, List, Optional, Tuple, cast

from datahub.ingestion.source.looker.looker_common import (
    LookerConnectionDefinition,
    LookerViewId,
    ViewField,
    ViewFieldValue,
)
from datahub.ingestion.source.looker.looker_dataclasses import LookerModel
from datahub.ingestion.source.looker.looker_file_loader import LookerViewFileLoader
from datahub.ingestion.source.looker.lookml_config import (
    _BASE_PROJECT_NAME,
    DERIVED_VIEW_SUFFIX,
    NAME,
    LookMLSourceConfig,
    LookMLSourceReport,
)
from datahub.ingestion.source.looker.urn_functions import get_qualified_table_name
from datahub.sql_parsing.sqlglot_lineage import ColumnRef

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
            if is_derived_view(col_ref.table.lower()):
                looker_view_id = get_derived_looker_view_id(
                    qualified_table_name=get_qualified_table_name(col_ref.table),
                    looker_view_id_cache=looker_view_id_cache,
                    base_folder_path=base_folder_path,
                )

                if looker_view_id is None:
                    logger.warning(
                        f"Not able to resolve to derived view looker id for {col_ref.table}"
                    )
                    continue

                upstream_fields.append(
                    ColumnRef(
                        table=looker_view_id.get_urn(config=config),
                        column=col_ref.column
                    )
                )
            else:
                upstream_fields.append(col_ref)

        field.upstream_fields = upstream_fields

    # Regenerate upstream_urns if .sql_table_name is present
    new_upstream_urns: List[str] = []
    for urn in upstream_urns:
        if is_derived_view(urn):
            looker_view_id = get_derived_looker_view_id(
                qualified_table_name=get_qualified_table_name(col_ref.table),
                looker_view_id_cache=looker_view_id_cache,
                base_folder_path=base_folder_path,
            )

            if looker_view_id is None:
                logger.warning(
                    f"Not able to resolve to derived view looker id for {col_ref.table}"
                )
                continue

            new_upstream_urns.append(
                looker_view_id.get_urn(config=config)
            )
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
