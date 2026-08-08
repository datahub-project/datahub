from dataclasses import dataclass, field
from typing import Dict, List, Optional, cast

from looker_sdk.sdk.api40.models import WriteQuery

from datahub.utilities.str_enum import StrEnum


class LookerModel(StrEnum):
    SYSTEM_ACTIVITY = "system__activity"


class LookerExplore(StrEnum):
    HISTORY = "history"
    FIELD_USAGE = "field_usage"


class LookerView(StrEnum):
    HISTORY = "history"
    USER = "user"
    LOOK = "look"
    # The `query` view in System Activity describes what an execution ran
    # against, including the model and the explore (Looker calls the explore
    # the query's `view`).
    QUERY = "query"
    FIELD_USAGE = "field_usage"


class LookerField(StrEnum):
    DASHBOARD_ID = "dashboard_id"
    CREATED_DATE = "created_date"
    DASHBOARD_RUN_COUNT = "dashboard_run_count"
    ID = "id"
    DASHBOARD_USER = "dashboard_user"
    COUNT = "count"
    MODEL = "model"
    VIEW = "view"
    FIELDS = "fields"


class ViewField(StrEnum):
    """
    Base enum to create new view field enum. Please see  HistoryViewField, UserViewField and LookViewField
    """

    pass


class HistoryViewField(ViewField):
    HISTORY_CREATED_DATE = f"{LookerView.HISTORY}.{LookerField.CREATED_DATE}"
    HISTORY_DASHBOARD_RUN_COUNT = (
        f"{LookerView.HISTORY}.{LookerField.DASHBOARD_RUN_COUNT}"
    )
    HISTORY_DASHBOARD_ID = f"{LookerView.HISTORY}.{LookerField.DASHBOARD_ID}"
    HISTORY_DASHBOARD_USER = f"{LookerView.HISTORY}.{LookerField.DASHBOARD_USER}"
    HISTORY_COUNT = f"{LookerView.HISTORY}.{LookerField.COUNT}"


class UserViewField(ViewField):
    USER_ID = f"{LookerView.USER}.{LookerField.ID}"


class LookViewField(ViewField):
    LOOK_ID = f"{LookerView.LOOK}.{LookerField.ID}"


class QueryViewField(ViewField):
    # `query.view` is the explore the query ran against (Looker's naming, not a
    # LookML view). Together with `query.model` it identifies the explore.
    QUERY_MODEL = f"{LookerView.QUERY}.{LookerField.MODEL}"
    QUERY_VIEW = f"{LookerView.QUERY}.{LookerField.VIEW}"
    # System Activity's list of fully-qualified `view.field` names each query
    # referenced.  Returned as a single delimited string (not an array).
    QUERY_FIELDS = f"{LookerView.QUERY}.{LookerField.FIELDS}"


class FieldUsageViewField(ViewField):
    """Fields from the ``field_usage`` explore in System Activity.

    This explore provides pre-aggregated, lifetime per-field usage counts
    without the row-limit truncation issues of the History explore."""

    FIELD_USAGE_MODEL = f"{LookerView.FIELD_USAGE}.{LookerField.MODEL}"
    FIELD_USAGE_EXPLORE = "field_usage.explore"
    FIELD_USAGE_FIELD = "field_usage.field"
    FIELD_USAGE_TIMES_USED = "field_usage.times_used"


@dataclass
class LookerQuery:
    model: LookerModel
    explore: LookerExplore
    fields: List[ViewField]
    # Check looker documentation for possible values https://docs.looker.com/reference/filter-expressions
    filters: Dict[ViewField, str] = field(default_factory=dict)
    limit: Optional[str] = None

    def to_write_query(self) -> WriteQuery:
        return WriteQuery(
            model=cast(str, self.model.value),  # the cast is jut to silent the lint
            view=cast(str, self.explore.value),
            fields=[cast(str, field.value) for field in self.fields],
            filters=(
                {filter_.value: self.filters[filter_] for filter_ in self.filters}
                if self.filters is not None
                else {}
            ),
            limit=self.limit,
        )
