from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, cast

from looker_sdk.sdk.api40.models import WriteQuery


# Enum whose value is string and compatible with dictionary having string value as key
class StrEnum(str, Enum):
    pass


class LookerModel(StrEnum):
    SYSTEM_ACTIVITY = "system__activity"


class LookerExplore(StrEnum):
    HISTORY = "history"


class LookerView(StrEnum):
    HISTORY = "history"
    USER = "user"
    LOOK = "look"


class LookerField(StrEnum):
    DASHBOARD_ID = "dashboard_id"
    CREATED_DATE = "created_date"
    DASHBOARD_RUN_COUNT = "dashboard_run_count"
    ID = "id"
    DASHBOARD_USER = "dashboard_user"
    COUNT = "count"


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


@dataclass
class LookerQuery:
    model: LookerModel
    explore: LookerExplore
    fields: List[ViewField]
    # Check looker documentation for possible values https://docs.looker.com/reference/filter-expressions
    filters: Dict[ViewField, str] = field(default_factory=dict)

    def to_write_query(self) -> WriteQuery:
        return WriteQuery(
            model=cast(str, self.model.value),  # the cast is jut to silent the lint
            view=cast(str, self.explore.value),
            fields=[cast(str, field.value) for field in self.fields],
            filters={filter_.value: self.filters[filter_] for filter_ in self.filters}
            if self.filters is not None
            else {},
        )
