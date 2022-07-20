# Imports for urn construction utility methods
from datetime import datetime
from typing import List

from datahub.emitter.mce_builder import make_dashboard_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    ChangeTypeClass,
    DashboardUsageStatisticsClass,
    DashboardUserUsageCountsClass,
    TimeWindowSizeClass,
)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

usage_day_1_user_counts: List[DashboardUserUsageCountsClass] = [
    DashboardUserUsageCountsClass(
        user=make_user_urn("user1"), executionsCount=3, usageCount=3
    ),
    DashboardUserUsageCountsClass(
        user=make_user_urn("user2"), executionsCount=2, usageCount=2
    ),
]

usage_day_1: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dashboard",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=make_dashboard_urn("looker", "dashboards.999999"),
    aspectName="dashboardUsageStatistics",
    aspect=DashboardUsageStatisticsClass(
        timestampMillis=round(
            datetime.strptime("2022-02-09", "%Y-%m-%d").timestamp() * 1000
        ),
        eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
        uniqueUserCount=2,
        executionsCount=5,
        userCounts=usage_day_1_user_counts,
    ),
)

absolute_usage_as_of_day_1: MetadataChangeProposalWrapper = (
    MetadataChangeProposalWrapper(
        entityType="dashboard",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=make_dashboard_urn("looker", "dashboards.999999"),
        aspectName="dashboardUsageStatistics",
        aspect=DashboardUsageStatisticsClass(
            timestampMillis=round(
                datetime.strptime("2022-02-09", "%Y-%m-%d").timestamp() * 1000
            ),
            favoritesCount=100,
            viewsCount=25,
            lastViewedAt=round(
                datetime.strptime(
                    "2022-02-09 04:45:30", "%Y-%m-%d %H:%M:%S"
                ).timestamp()
                * 1000
            ),
        ),
    )
)

rest_emitter.emit(usage_day_1)
rest_emitter.emit(absolute_usage_as_of_day_1)

usage_day_2_user_counts: List[DashboardUserUsageCountsClass] = [
    DashboardUserUsageCountsClass(
        user=make_user_urn("user1"), executionsCount=4, usageCount=4
    ),
    DashboardUserUsageCountsClass(
        user=make_user_urn("user2"), executionsCount=6, usageCount=6
    ),
]
usage_day_2: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dashboard",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=make_dashboard_urn("looker", "dashboards.999999"),
    aspectName="dashboardUsageStatistics",
    aspect=DashboardUsageStatisticsClass(
        timestampMillis=round(
            datetime.strptime("2022-02-10", "%Y-%m-%d").timestamp() * 1000
        ),
        eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
        uniqueUserCount=2,
        executionsCount=10,
        userCounts=usage_day_2_user_counts,
    ),
)

absolute_usage_as_of_day_2: MetadataChangeProposalWrapper = (
    MetadataChangeProposalWrapper(
        entityType="dashboard",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=make_dashboard_urn("looker", "dashboards.999999"),
        aspectName="dashboardUsageStatistics",
        aspect=DashboardUsageStatisticsClass(
            timestampMillis=round(
                datetime.strptime("2022-02-10", "%Y-%m-%d").timestamp() * 1000
            ),
            favoritesCount=100,
            viewsCount=27,
            lastViewedAt=round(
                datetime.strptime(
                    "2022-02-10 10:45:30", "%Y-%m-%d %H:%M:%S"
                ).timestamp()
                * 1000
            ),
        ),
    )
)

rest_emitter.emit(usage_day_2)
rest_emitter.emit(absolute_usage_as_of_day_2)

usage_day_3_user_counts: List[DashboardUserUsageCountsClass] = [
    DashboardUserUsageCountsClass(
        user=make_user_urn("user1"), executionsCount=2, usageCount=2
    ),
]
usage_day_3: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityType="dashboard",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=make_dashboard_urn("looker", "dashboards.999999"),
    aspectName="dashboardUsageStatistics",
    aspect=DashboardUsageStatisticsClass(
        timestampMillis=round(
            datetime.strptime("2022-02-11", "%Y-%m-%d").timestamp() * 1000
        ),
        eventGranularity=TimeWindowSizeClass(unit=CalendarIntervalClass.DAY),
        uniqueUserCount=1,
        executionsCount=2,
        userCounts=usage_day_3_user_counts,
    ),
)

absolute_usage_as_of_day_3: MetadataChangeProposalWrapper = (
    MetadataChangeProposalWrapper(
        entityType="dashboard",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=make_dashboard_urn("looker", "dashboards.999999"),
        aspectName="dashboardUsageStatistics",
        aspect=DashboardUsageStatisticsClass(
            timestampMillis=round(
                datetime.strptime("2022-02-11", "%Y-%m-%d").timestamp() * 1000
            ),
            favoritesCount=102,
            viewsCount=30,
            lastViewedAt=round(
                datetime.strptime(
                    "2022-02-11 02:45:30", "%Y-%m-%d %H:%M:%S"
                ).timestamp()
                * 1000
            ),
        ),
    )
)

rest_emitter.emit(usage_day_3)
rest_emitter.emit(absolute_usage_as_of_day_3)
