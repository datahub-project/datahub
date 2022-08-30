# This module is written on the basis of below requirement
#     1) Entity absolute stat
#     2) Entity timeseries stat
#     3) Entity timeseries stat by user

import datetime
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, cast

from looker_sdk.rtl import model
from looker_sdk.sdk.api31.models import Dashboard, LookWithQuery

import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.looker import looker_lib_wrapper
from datahub.ingestion.source.looker.looker_common import LookerDashboardSourceReport
from datahub.ingestion.source.looker.looker_lib_wrapper import (
    LookerAPI,
    LookerUser,
    LookerUserRegistry,
)
from datahub.ingestion.source.looker.looker_query_model import (
    HistoryViewField,
    LookerExplore,
    LookerModel,
    LookerQuery,
    LookViewField,
    StrEnum,
    UserViewField,
    ViewField,
)
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    ChangeTypeClass,
    ChartUsageStatisticsClass,
    ChartUserUsageCountsClass,
    DashboardUsageStatisticsClass,
    DashboardUserUsageCountsClass,
    TimeWindowSizeClass,
)

logger = logging.getLogger(__name__)


@dataclass
class StatGeneratorConfig:
    looker_api_wrapper: LookerAPI
    looker_user_registry: LookerUserRegistry
    strip_user_ids_from_email: bool
    interval: str
    platform_name: str


# QueryId and query_collection helps to return dummy responses in test cases
class QueryId(StrEnum):
    DASHBOARD_PER_DAY_USAGE_STAT = "counts_per_day_per_dashboard"
    DASHBOARD_PER_USER_PER_DAY_USAGE_STAT = "counts_per_day_per_user_per_dashboard"
    LOOK_PER_DAY_USAGE_STAT = "counts_per_day_per_look"
    LOOK_PER_USER_PER_DAY_USAGE_STAT = "counts_per_day_per_user_per_look"


query_collection: Dict[QueryId, LookerQuery] = {
    QueryId.DASHBOARD_PER_DAY_USAGE_STAT: LookerQuery(
        model=LookerModel.SYSTEM_ACTIVITY,
        explore=LookerExplore.HISTORY,
        fields=[
            HistoryViewField.HISTORY_DASHBOARD_ID,
            HistoryViewField.HISTORY_CREATED_DATE,
            HistoryViewField.HISTORY_DASHBOARD_USER,
            HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT,
        ],
    ),
    QueryId.DASHBOARD_PER_USER_PER_DAY_USAGE_STAT: LookerQuery(
        model=LookerModel.SYSTEM_ACTIVITY,
        explore=LookerExplore.HISTORY,
        fields=[
            HistoryViewField.HISTORY_CREATED_DATE,
            HistoryViewField.HISTORY_DASHBOARD_ID,
            HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT,
            UserViewField.USER_ID,
        ],
    ),
    QueryId.LOOK_PER_DAY_USAGE_STAT: LookerQuery(
        model=LookerModel.SYSTEM_ACTIVITY,
        explore=LookerExplore.HISTORY,
        fields=[
            HistoryViewField.HISTORY_CREATED_DATE,
            HistoryViewField.HISTORY_COUNT,
            LookViewField.LOOK_ID,
        ],
        filters={
            LookViewField.LOOK_ID: "NOT NULL",
        },
    ),
    QueryId.LOOK_PER_USER_PER_DAY_USAGE_STAT: LookerQuery(
        model=LookerModel.SYSTEM_ACTIVITY,
        explore=LookerExplore.HISTORY,
        fields=[
            HistoryViewField.HISTORY_CREATED_DATE,
            HistoryViewField.HISTORY_COUNT,
            LookViewField.LOOK_ID,
            UserViewField.USER_ID,
        ],
        filters={
            LookViewField.LOOK_ID: "NOT NULL",
        },
    ),
}


class BaseStatGenerator(ABC):
    """
    A Template class to encapsulate looker stat processing
    """

    def __init__(
        self,
        config: StatGeneratorConfig,
        looker_models: List[model.Model],
        report: LookerDashboardSourceReport,
        ctx: str,
    ):
        self.config = config
        self.looker_models = looker_models
        # Later it will help to find out for what are the looker entities from query result
        self.id_vs_model: Dict[str, model.Model] = {
            self.get_id(looker_object): looker_object for looker_object in looker_models
        }
        self.post_filter = len(self.looker_models) > 100
        self.report = report
        self.ctx = ctx

    @abstractmethod
    def get_entity_timeseries_query(self) -> LookerQuery:
        pass

    @abstractmethod
    def get_entity_user_timeseries_query(self) -> LookerQuery:
        pass

    @abstractmethod
    def get_filter(self) -> Dict[ViewField, str]:
        pass

    @abstractmethod
    def to_entity_absolute_stat_aspect(self, looker_object: model.Model) -> Aspect:
        pass

    @abstractmethod
    def to_entity_timeseries_stat_aspect(self, row: Dict) -> Aspect:
        pass

    @abstractmethod
    def get_entity_stat_key(self, row: Dict) -> Tuple[str, str]:
        pass

    @abstractmethod
    def _get_mcp_attributes(self, model: model.Model) -> Dict:
        pass

    @abstractmethod
    def append_user_stat(
        self, entity_stat_aspect: Aspect, user: LookerUser, row: Dict
    ) -> None:
        pass

    @abstractmethod
    def get_id(self, looker_object: model.Model) -> str:
        pass

    @abstractmethod
    def get_id_from_row(self, row: dict) -> str:
        pass

    def create_mcp(
        self, model: model.Model, aspect: Aspect
    ) -> MetadataChangeProposalWrapper:
        return MetadataChangeProposalWrapper(
            aspect=aspect,
            **self._get_mcp_attributes(model=model),
        )

    def _round_time(self, date_time: str) -> int:
        return round(
            datetime.datetime.strptime(date_time, "%Y-%m-%d")
            .replace(tzinfo=datetime.timezone.utc)
            .timestamp()
            * 1000
        )

    def _get_user_identifier(self, row: Dict) -> int:
        return row[UserViewField.USER_ID]

    def _process_entity_timeseries_rows(
        self, rows: List[Dict]
    ) -> Dict[Tuple[str, str], Aspect]:
        # Convert Looker entity stat i.e. rows to DataHub stat aspect
        entity_stat_aspect: Dict[Tuple[str, str], Aspect] = {}

        for row in rows:
            logger.debug(row)
            entity_stat_aspect[
                self.get_entity_stat_key(row)
            ] = self.to_entity_timeseries_stat_aspect(row)

        return entity_stat_aspect

    def _process_absolute_aspect(self) -> List[Tuple[model.Model, Aspect]]:
        aspects: List[Tuple[model.Model, Aspect]] = []
        for looker_object in self.looker_models:
            aspects.append(
                (looker_object, self.to_entity_absolute_stat_aspect(looker_object))
            )

        return aspects

    def _fill_user_stat_aspect(
        self,
        entity_usage_stat: Dict[Tuple[str, str], Aspect],
        user_wise_rows: List[Dict],
    ) -> Iterable[Tuple[model.Model, Aspect]]:
        for row in user_wise_rows:
            # Confirm looker object was given for stat generation
            looker_object = self.id_vs_model.get(self.get_id_from_row(row))
            if looker_object is None:
                logger.warning(
                    "Looker object with id({}) was not register with stat generator".format(
                        self.get_id_from_row(row)
                    )
                )
                continue

            # Confirm we do have user with user id
            user: Optional[LookerUser] = self.config.looker_user_registry.get_by_id(
                self._get_user_identifier(row)
            )
            if user is None:
                logger.warning(
                    f"Unable to resolve user with id {row[self._get_user_identifier(row)]}, skipping"
                )
                continue

            # Confirm for the user row (entity + time) the entity stat is present for the same time
            entity_stat_aspect: Aspect = entity_usage_stat[
                self.get_entity_stat_key(row)
            ]
            if entity_stat_aspect is None:
                logger.warning(
                    "entity stat is not found for the user stat key = {}".format(
                        self.get_entity_stat_key(row)
                    )
                )
                continue
            self.append_user_stat(entity_stat_aspect, user, row)
            yield looker_object, entity_stat_aspect

    def _execute_query(self, query: LookerQuery, query_name: str) -> List[Dict]:
        start_time = datetime.datetime.now()
        rows = self.config.looker_api_wrapper.execute_query(
            write_query=query.to_write_query()
        )
        end_time = datetime.datetime.now()

        logger.debug(
            f"{self.ctx}: Retrieved {len(rows)} rows in {(end_time - start_time).total_seconds()} seconds"
        )
        self.report.report_query_latency(
            f"{self.ctx}:{query_name}", (end_time - start_time).total_seconds()
        )
        if self.post_filter:
            rows = [r for r in rows if self.get_id_from_row(r) in self.id_vs_model]
            logger.debug(f"Filtered down to {len(rows)} rows")
        return rows

    def _append_filters(self, query: LookerQuery) -> LookerQuery:
        query.filters.update(
            {HistoryViewField.HISTORY_CREATED_DATE: self.config.interval}
        )
        if not self.post_filter:
            query.filters.update(self.get_filter())
        return query

    def generate_usage_stat_mcps(self) -> Iterable[MetadataChangeProposalWrapper]:

        # No looker entities available to process stat generation
        if len(self.looker_models) == 0:
            return

        # yield absolute stat for looker entities
        for looker_object, aspect in self._process_absolute_aspect():  # type: ignore
            yield self.create_mcp(looker_object, aspect)

        # Execute query and process the raw json which contains stat information
        entity_query_with_filters: LookerQuery = self._append_filters(
            self.get_entity_timeseries_query()
        )
        entity_rows: List[Dict] = self._execute_query(
            entity_query_with_filters, "entity_query"
        )
        entity_usage_stat: Dict[
            Tuple[str, str], Any
        ] = self._process_entity_timeseries_rows(
            entity_rows
        )  # Any type to pass mypy unbound Aspect type error

        user_wise_query_with_filters: LookerQuery = self._append_filters(
            self.get_entity_user_timeseries_query()
        )
        user_wise_rows = self._execute_query(user_wise_query_with_filters, "user_query")
        # yield absolute stat for entity
        for looker_object, aspect in self._fill_user_stat_aspect(
            entity_usage_stat, user_wise_rows
        ):
            yield self.create_mcp(looker_object, aspect)


class DashboardStatGenerator(BaseStatGenerator):
    def __init__(
        self,
        config: StatGeneratorConfig,
        looker_dashboards: List[Dashboard],
        report: LookerDashboardSourceReport,
    ):
        super().__init__(
            config,
            looker_models=cast(List[model.Model], looker_dashboards),
            report=report,
            ctx="Dashboard",
        )
        self.report = report
        self.report.report_dashboards_scanned_for_usage(len(looker_dashboards))

    def get_filter(self) -> Dict[ViewField, str]:
        return {
            HistoryViewField.HISTORY_DASHBOARD_ID: ",".join(
                [
                    str(cast(Dashboard, looker_dashboard).id)  # type: ignore
                    for looker_dashboard in self.looker_models
                ]
            )
        }

    def get_id(self, looker_object: model.Model) -> str:
        dashboard: Dashboard = cast(Dashboard, looker_object)
        if dashboard.id is None:
            raise ValueError("Looker dashboard id is None")
        return str(dashboard.id)

    def get_id_from_row(self, row: dict) -> str:
        return str(row[HistoryViewField.HISTORY_DASHBOARD_ID])

    def get_entity_stat_key(self, row: Dict) -> Tuple[str, str]:
        self.report.dashboards_with_activity.add(
            row[HistoryViewField.HISTORY_DASHBOARD_ID]
        )
        return (
            row[HistoryViewField.HISTORY_DASHBOARD_ID],
            row[HistoryViewField.HISTORY_CREATED_DATE],
        )

    def _get_mcp_attributes(self, model: model.Model) -> Dict:
        dashboard: Dashboard = cast(Dashboard, model)
        if dashboard is None or dashboard.id is None:  # to pass mypy lint
            return {}

        return {
            "entityUrn": builder.make_dashboard_urn(
                self.config.platform_name,
                looker_lib_wrapper.get_urn_looker_dashboard_id(dashboard.id),
            ),
            "entityType": "dashboard",
            "changeType": ChangeTypeClass.UPSERT,
            "aspectName": "dashboardUsageStatistics",
        }

    def to_entity_absolute_stat_aspect(self, looker_object: model.Model) -> Aspect:
        looker_dashboard: Dashboard = cast(Dashboard, looker_object)
        if looker_dashboard.view_count:
            self.report.dashboards_with_activity.add(str(looker_dashboard.id))
        return cast(
            Aspect,
            DashboardUsageStatisticsClass(
                timestampMillis=round(datetime.datetime.now().timestamp() * 1000),
                favoritesCount=looker_dashboard.favorite_count,
                viewsCount=looker_dashboard.view_count,
                lastViewedAt=round(looker_dashboard.last_viewed_at.timestamp() * 1000)
                if looker_dashboard.last_viewed_at
                else None,
                userCounts=[],
            ),
        )

    def get_entity_timeseries_query(self) -> LookerQuery:
        return query_collection[QueryId.DASHBOARD_PER_DAY_USAGE_STAT]

    def get_entity_user_timeseries_query(self) -> LookerQuery:
        return query_collection[QueryId.DASHBOARD_PER_USER_PER_DAY_USAGE_STAT]

    def to_entity_timeseries_stat_aspect(self, row: dict) -> Aspect:
        self.report.dashboards_with_activity.add(
            row[HistoryViewField.HISTORY_DASHBOARD_ID]
        )
        return cast(
            Aspect,
            DashboardUsageStatisticsClass(
                timestampMillis=self._round_time(
                    row[HistoryViewField.HISTORY_CREATED_DATE]
                ),
                eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
                uniqueUserCount=row[HistoryViewField.HISTORY_DASHBOARD_USER],
                executionsCount=row[HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT],
                userCounts=[],
            ),
        )

    def append_user_stat(
        self, entity_stat_aspect: Aspect, user: LookerUser, row: Dict
    ) -> None:
        dashboard_stat_aspect: DashboardUsageStatisticsClass = cast(
            DashboardUsageStatisticsClass, entity_stat_aspect
        )

        if dashboard_stat_aspect.userCounts is None:
            dashboard_stat_aspect.userCounts = []

        user_urn: Optional[str] = user.get_urn(self.config.strip_user_ids_from_email)

        if user_urn is None:
            logger.warning("user_urn not found for the user {}".format(user))
            return

        dashboard_stat_aspect.userCounts.append(
            DashboardUserUsageCountsClass(
                user=user_urn,
                executionsCount=row[HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT],
                usageCount=row[HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT],
                userEmail=user.email,
            )
        )


class LookStatGenerator(BaseStatGenerator):
    def __init__(
        self,
        config: StatGeneratorConfig,
        looker_looks: List[LookWithQuery],
        report: LookerDashboardSourceReport,
    ):
        super().__init__(
            config,
            looker_models=cast(List[model.Model], looker_looks),
            report=report,
            ctx="Look",
        )
        self.report = report
        report.report_charts_scanned_for_usage(len(looker_looks))

    def get_filter(self) -> Dict[ViewField, str]:
        return {
            LookViewField.LOOK_ID: ",".join(
                [str(cast(LookWithQuery, look).id) for look in self.looker_models]  # type: ignore
            )
        }

    def get_id(self, looker_object: model.Model) -> str:
        look: LookWithQuery = cast(LookWithQuery, looker_object)
        if (
            look.id is None
        ):  # This condition never become true, this check is to pass the mypy lint error
            raise ValueError("Looker look model id is None")
        return str(look.id)

    def get_id_from_row(self, row: dict) -> str:
        return str(row[LookViewField.LOOK_ID])

    def get_entity_stat_key(self, row: Dict) -> Tuple[str, str]:
        return (
            str(row[LookViewField.LOOK_ID]),
            row[HistoryViewField.HISTORY_CREATED_DATE],
        )

    def _get_mcp_attributes(self, model: model.Model) -> Dict:
        look: LookWithQuery = cast(LookWithQuery, model)
        if look is None or look.id is None:
            return {}

        return {
            "entityUrn": builder.make_chart_urn(
                self.config.platform_name,
                looker_lib_wrapper.get_urn_looker_element_id(str(look.id)),
            ),
            "entityType": "chart",
            "changeType": ChangeTypeClass.UPSERT,
            "aspectName": "chartUsageStatistics",
        }

    def to_entity_absolute_stat_aspect(self, looker_object: model.Model) -> Aspect:
        looker_look: LookWithQuery = cast(LookWithQuery, looker_object)
        if looker_look.view_count:
            self.report.charts_with_activity.add(str(looker_look.id))

        return cast(
            Aspect,
            ChartUsageStatisticsClass(
                timestampMillis=round(datetime.datetime.now().timestamp() * 1000),
                viewsCount=looker_look.view_count,
                userCounts=[],
            ),
        )

    def get_entity_timeseries_query(self) -> LookerQuery:
        return query_collection[QueryId.LOOK_PER_DAY_USAGE_STAT]

    def get_entity_user_timeseries_query(self) -> LookerQuery:
        return query_collection[QueryId.LOOK_PER_USER_PER_DAY_USAGE_STAT]

    def to_entity_timeseries_stat_aspect(self, row: dict) -> Aspect:
        self.report.charts_with_activity.add(row[LookViewField.LOOK_ID])

        return cast(
            Aspect,
            ChartUsageStatisticsClass(
                timestampMillis=self._round_time(
                    row[HistoryViewField.HISTORY_CREATED_DATE]
                ),
                eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
                viewsCount=row[HistoryViewField.HISTORY_COUNT],
                userCounts=[],
            ),
        )

    def append_user_stat(
        self, entity_stat_aspect: Aspect, user: LookerUser, row: Dict
    ) -> None:
        chart_stat_aspect: ChartUsageStatisticsClass = cast(
            ChartUsageStatisticsClass, entity_stat_aspect
        )

        if chart_stat_aspect.userCounts is None:
            chart_stat_aspect.userCounts = []

        user_urn: Optional[str] = user.get_urn(self.config.strip_user_ids_from_email)

        if user_urn is None:
            logger.warning("user_urn not found for the user {}".format(user))
            return

        chart_stat_aspect.userCounts.append(
            ChartUserUsageCountsClass(
                user=user_urn,
                viewsCount=row[HistoryViewField.HISTORY_COUNT],
            )
        )


class SupportedStatEntity(Enum):
    DASHBOARD = "dashboard"
    CHART = "chart"


# type_ is because of type is builtin identifier
def create_stat_entity_generator(
    type_: SupportedStatEntity, config: StatGeneratorConfig
) -> Callable[[List[model.Model], LookerDashboardSourceReport], BaseStatGenerator]:
    # Wrapper function to defer creation of actual entities
    # config is generally available at the startup, however entities may get created later during processing
    def create_dashboard_stat_generator(
        looker_dashboards: List[Dashboard], report: LookerDashboardSourceReport
    ) -> BaseStatGenerator:
        logger.debug(
            "Number of dashboard received for stat processing = {}".format(
                len(looker_dashboards)
            )
        )
        return DashboardStatGenerator(
            config=config, looker_dashboards=looker_dashboards, report=report
        )

    def create_chart_stat_generator(
        looker_looks: List[LookWithQuery], report: LookerDashboardSourceReport
    ) -> BaseStatGenerator:
        logger.debug(
            "Number of looks received for stat processing = {}".format(
                len(looker_looks)
            )
        )
        return LookStatGenerator(
            config=config, looker_looks=looker_looks, report=report
        )

    stat_entities_generator = {
        SupportedStatEntity.DASHBOARD: create_dashboard_stat_generator,
        SupportedStatEntity.CHART: create_chart_stat_generator,
    }

    return stat_entities_generator[type_]  # type: ignore
