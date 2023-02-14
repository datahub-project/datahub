# This module is written on the basis of below requirement
#     1) Entity absolute stat
#     2) Entity timeseries stat
#     3) Entity timeseries stat by user

import concurrent
import dataclasses
import datetime
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, cast

from looker_sdk.sdk.api31.models import Dashboard, LookWithQuery

import datahub.emitter.mce_builder as builder
from datahub.emitter.mce_builder import Aspect, AspectAbstract
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.looker import looker_common
from datahub.ingestion.source.looker.looker_common import (
    LookerDashboardSourceReport,
    LookerUser,
    LookerUserRegistry,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
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
class ModelForUsage:
    id: Optional[str]


@dataclass
class LookerChartForUsage(ModelForUsage):
    view_count: Optional[int]

    @classmethod
    def from_chart(cls, chart: LookWithQuery) -> "LookerChartForUsage":
        return cls(id=str(chart.id) if chart.id else None, view_count=chart.view_count)


@dataclass
class LookerDashboardForUsage(ModelForUsage):
    view_count: Optional[int]
    favorite_count: Optional[int]
    last_viewed_at: Optional[int]
    looks: List[LookerChartForUsage] = dataclasses.field(default_factory=list)

    @classmethod
    def from_dashboard(cls, dashboard: Dashboard) -> "LookerDashboardForUsage":
        return cls(
            id=dashboard.id,
            view_count=dashboard.view_count,
            favorite_count=dashboard.favorite_count,
            last_viewed_at=round(dashboard.last_viewed_at.timestamp() * 1000)
            if dashboard.last_viewed_at
            else None,
            looks=[
                LookerChartForUsage.from_chart(e.look)
                for e in dashboard.dashboard_elements
                if e.look is not None
            ]
            if dashboard.dashboard_elements
            else [],
        )


@dataclass
class StatGeneratorConfig:
    looker_api_wrapper: LookerAPI
    looker_user_registry: LookerUserRegistry
    strip_user_ids_from_email: bool
    interval: str
    platform_name: str
    max_threads: int = 1


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
        looker_models: List[ModelForUsage],
        report: LookerDashboardSourceReport,
    ):
        self.config = config
        self.looker_models = looker_models
        # Later it will help to find out for what are the looker entities from query result
        self.id_vs_model: Dict[str, ModelForUsage] = {
            self.get_id(looker_object): looker_object for looker_object in looker_models
        }
        self.post_filter = len(self.looker_models) > 100
        self.report = report

    @abstractmethod
    def get_stats_generator_name(self) -> str:
        """All implementing classes should provide a name identifying them"""
        pass

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
    def to_entity_absolute_stat_aspect(
        self, looker_object: ModelForUsage
    ) -> AspectAbstract:
        pass

    @abstractmethod
    def to_entity_timeseries_stat_aspect(self, row: Dict) -> AspectAbstract:
        pass

    @abstractmethod
    def get_entity_stat_key(self, row: Dict) -> Tuple[str, str]:
        pass

    @abstractmethod
    def _get_mcp_attributes(self, model: ModelForUsage) -> Dict:
        pass

    @abstractmethod
    def append_user_stat(
        self, entity_stat_aspect: Aspect, user: LookerUser, row: Dict
    ) -> None:
        pass

    @abstractmethod
    def get_id(self, looker_object: ModelForUsage) -> str:
        pass

    @abstractmethod
    def get_id_from_row(self, row: dict) -> str:
        pass

    def create_mcp(
        self, model: ModelForUsage, aspect: Aspect
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
    ) -> Dict[Tuple[str, str], AspectAbstract]:
        # Convert Looker entity stat i.e. rows to DataHub stat aspect
        entity_stat_aspect: Dict[Tuple[str, str], AspectAbstract] = {}

        for row in rows:
            logger.debug(row)
            entity_stat_aspect[
                self.get_entity_stat_key(row)
            ] = self.to_entity_timeseries_stat_aspect(row)

        return entity_stat_aspect

    def _process_absolute_aspect(self) -> List[Tuple[ModelForUsage, AspectAbstract]]:
        aspects: List[Tuple[ModelForUsage, AspectAbstract]] = []
        for looker_object in self.looker_models:
            aspects.append(
                (looker_object, self.to_entity_absolute_stat_aspect(looker_object))
            )

        return aspects

    def _fill_user_stat_aspect(
        self,
        entity_usage_stat: Dict[Tuple[str, str], Aspect],
        user_wise_rows: List[Dict],
    ) -> Iterable[Tuple[ModelForUsage, Aspect]]:
        logger.debug("Entering fill user stat aspect")

        # We first resolve all the users using a threadpool to warm up the cache
        user_ids = set([self._get_user_identifier(row) for row in user_wise_rows])
        start_time = datetime.datetime.now()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.max_threads
        ) as async_executor:
            user_futures = {
                async_executor.submit(
                    self.config.looker_user_registry.get_by_id, user_id
                ): 1
                for user_id in user_ids
            }
            for future in concurrent.futures.as_completed(user_futures):
                logger.debug("User result: %s", future.result())
                del user_futures[future]

        user_resolution_latency = datetime.datetime.now() - start_time
        logger.debug(
            f"Resolved {len(user_ids)} in {user_resolution_latency.total_seconds()} seconds"
        )
        self.report.report_user_resolution_latency(
            self.get_stats_generator_name(), user_resolution_latency
        )

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
                    f"Unable to resolve user with id {self._get_user_identifier(row)}, skipping"
                )
                continue

            # Confirm for the user row (entity + time) the entity stat is present for the same time
            entity_stat_aspect: Optional[Aspect] = entity_usage_stat.get(
                self.get_entity_stat_key(row)
            )
            if entity_stat_aspect is None:
                logger.warning(
                    f"entity stat is not found for the row = {row}, in entity_stat = {entity_usage_stat}"
                )
                logger.warning(
                    "entity stat is not found for the user stat key = {}".format(
                        self.get_entity_stat_key(row)
                    )
                )
                continue
            self.append_user_stat(entity_stat_aspect, user, row)

        logger.debug("Starting to yield answers for user-wise counts")

        for (id, _), aspect in entity_usage_stat.items():
            yield self.id_vs_model[id], aspect

    def _execute_query(self, query: LookerQuery, query_name: str) -> List[Dict]:
        rows = []
        try:
            start_time = datetime.datetime.now()
            rows = self.config.looker_api_wrapper.execute_query(
                write_query=query.to_write_query()
            )
            end_time = datetime.datetime.now()

            logger.debug(
                f"{self.get_stats_generator_name()}: Retrieved {len(rows)} rows in {(end_time - start_time).total_seconds()} seconds"
            )
            self.report.report_query_latency(
                f"{self.get_stats_generator_name()}:{query_name}", end_time - start_time
            )
            if self.post_filter:
                logger.debug("post filtering")
                rows = [r for r in rows if self.get_id_from_row(r) in self.id_vs_model]
                logger.debug("Filtered down to %d rows", len(rows))
        except Exception as e:
            logger.warning(f"Failed to execute {query_name} query", e)

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
        looker_dashboards: List[LookerDashboardForUsage],
        report: LookerDashboardSourceReport,
    ):
        super().__init__(
            config,
            looker_models=cast(List[ModelForUsage], looker_dashboards),
            report=report,
        )
        self.report = report
        self.report.report_dashboards_scanned_for_usage(len(looker_dashboards))

    def get_stats_generator_name(self) -> str:
        return "DashboardStats"

    def get_filter(self) -> Dict[ViewField, str]:
        return {
            HistoryViewField.HISTORY_DASHBOARD_ID: ",".join(
                [
                    looker_dashboard.id
                    for looker_dashboard in self.looker_models
                    if looker_dashboard.id is not None
                ]
            )
        }

    def get_id(self, looker_object: ModelForUsage) -> str:
        if looker_object.id is None:
            raise ValueError("Looker dashboard id is None")
        return looker_object.id

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

    def _get_mcp_attributes(self, model: ModelForUsage) -> Dict:
        dashboard: Dashboard = cast(Dashboard, model)
        if dashboard is None or dashboard.id is None:  # to pass mypy lint
            return {}

        return {
            "entityUrn": builder.make_dashboard_urn(
                self.config.platform_name,
                looker_common.get_urn_looker_dashboard_id(dashboard.id),
            ),
            "entityType": "dashboard",
            "changeType": ChangeTypeClass.UPSERT,
            "aspectName": "dashboardUsageStatistics",
        }

    def to_entity_absolute_stat_aspect(
        self, looker_object: ModelForUsage
    ) -> DashboardUsageStatisticsClass:
        looker_dashboard: LookerDashboardForUsage = cast(
            LookerDashboardForUsage, looker_object
        )
        if looker_dashboard.view_count:
            self.report.dashboards_with_activity.add(str(looker_dashboard.id))
        return DashboardUsageStatisticsClass(
            timestampMillis=round(datetime.datetime.now().timestamp() * 1000),
            favoritesCount=looker_dashboard.favorite_count,
            viewsCount=looker_dashboard.view_count,
            lastViewedAt=looker_dashboard.last_viewed_at,
        )

    def get_entity_timeseries_query(self) -> LookerQuery:
        return query_collection[QueryId.DASHBOARD_PER_DAY_USAGE_STAT]

    def get_entity_user_timeseries_query(self) -> LookerQuery:
        return query_collection[QueryId.DASHBOARD_PER_USER_PER_DAY_USAGE_STAT]

    def to_entity_timeseries_stat_aspect(
        self, row: dict
    ) -> DashboardUsageStatisticsClass:
        self.report.dashboards_with_activity.add(
            row[HistoryViewField.HISTORY_DASHBOARD_ID]
        )
        return DashboardUsageStatisticsClass(
            timestampMillis=self._round_time(
                row[HistoryViewField.HISTORY_CREATED_DATE]
            ),
            eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
            uniqueUserCount=row[HistoryViewField.HISTORY_DASHBOARD_USER],
            executionsCount=row[HistoryViewField.HISTORY_DASHBOARD_RUN_COUNT],
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
        looker_looks: List[LookerChartForUsage],
        report: LookerDashboardSourceReport,
    ):
        super().__init__(
            config,
            looker_models=cast(List[ModelForUsage], looker_looks),
            report=report,
        )
        self.report = report
        report.report_charts_scanned_for_usage(len(looker_looks))

    def get_stats_generator_name(self) -> str:
        return "ChartStats"

    def get_filter(self) -> Dict[ViewField, str]:
        return {
            LookViewField.LOOK_ID: ",".join(
                [look.id for look in self.looker_models if look.id is not None]
            )
        }

    def get_id(self, looker_object: ModelForUsage) -> str:
        look: LookerChartForUsage = cast(LookerChartForUsage, looker_object)
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

    def _get_mcp_attributes(self, model: ModelForUsage) -> Dict:
        look: LookerChartForUsage = cast(LookerChartForUsage, model)
        if look is None or look.id is None:
            return {}

        return {
            "entityUrn": builder.make_chart_urn(
                self.config.platform_name,
                looker_common.get_urn_looker_element_id(str(look.id)),
            ),
            "entityType": "chart",
            "changeType": ChangeTypeClass.UPSERT,
            "aspectName": "chartUsageStatistics",
        }

    def to_entity_absolute_stat_aspect(
        self, looker_object: ModelForUsage
    ) -> ChartUsageStatisticsClass:
        looker_look: LookerChartForUsage = cast(LookerChartForUsage, looker_object)
        assert looker_look.id
        if looker_look.view_count:
            self.report.charts_with_activity.add(looker_look.id)

        return ChartUsageStatisticsClass(
            timestampMillis=round(datetime.datetime.now().timestamp() * 1000),
            viewsCount=looker_look.view_count,
        )

    def get_entity_timeseries_query(self) -> LookerQuery:
        return query_collection[QueryId.LOOK_PER_DAY_USAGE_STAT]

    def get_entity_user_timeseries_query(self) -> LookerQuery:
        return query_collection[QueryId.LOOK_PER_USER_PER_DAY_USAGE_STAT]

    def to_entity_timeseries_stat_aspect(self, row: dict) -> ChartUsageStatisticsClass:
        self.report.charts_with_activity.add(str(row[LookViewField.LOOK_ID]))

        return ChartUsageStatisticsClass(
            timestampMillis=self._round_time(
                row[HistoryViewField.HISTORY_CREATED_DATE]
            ),
            eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
            viewsCount=row[HistoryViewField.HISTORY_COUNT],
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
) -> Callable[[List[ModelForUsage], LookerDashboardSourceReport], BaseStatGenerator]:
    # Wrapper function to defer creation of actual entities
    # config is generally available at the startup, however entities may get created later during processing
    def create_dashboard_stat_generator(
        looker_dashboards: List[LookerDashboardForUsage],
        report: LookerDashboardSourceReport,
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
        looker_looks: List[LookerChartForUsage], report: LookerDashboardSourceReport
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
