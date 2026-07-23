from datetime import datetime
from typing import List

import datahub.metadata.schema_classes as models
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk import DataHubClient
from datahub.sdk.dashboard import Dashboard

client = DataHubClient.from_env()

# Day 1: User activity and absolute usage statistics
usage_day_1_user_counts: List[models.DashboardUserUsageCountsClass] = [
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user1")),
        executionsCount=3,
        usageCount=3,
    ),
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user2")),
        executionsCount=2,
        usageCount=2,
    ),
]

usage_day_1_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-09", "%Y-%m-%d").timestamp() * 1000
    ),
    eventGranularity=models.TimeWindowSizeClass(unit=models.CalendarIntervalClass.DAY),
    uniqueUserCount=2,
    executionsCount=5,
    userCounts=usage_day_1_user_counts,
)

absolute_usage_day_1_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-09", "%Y-%m-%d").timestamp() * 1000
    ),
    favoritesCount=100,
    viewsCount=25,
    lastViewedAt=round(
        datetime.strptime("2022-02-09 04:45:30", "%Y-%m-%d %H:%M:%S").timestamp() * 1000
    ),
)

dashboard = Dashboard(
    platform="looker",
    name="dashboards.999999",
    extra_aspects=[usage_day_1_stats, absolute_usage_day_1_stats],
)

client.entities.update(dashboard)

# Day 2: Updated user activity and absolute usage statistics
usage_day_2_user_counts: List[models.DashboardUserUsageCountsClass] = [
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user1")),
        executionsCount=4,
        usageCount=4,
    ),
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user2")),
        executionsCount=6,
        usageCount=6,
    ),
]

usage_day_2_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-10", "%Y-%m-%d").timestamp() * 1000
    ),
    eventGranularity=models.TimeWindowSizeClass(unit=models.CalendarIntervalClass.DAY),
    uniqueUserCount=2,
    executionsCount=10,
    userCounts=usage_day_2_user_counts,
)

absolute_usage_day_2_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-10", "%Y-%m-%d").timestamp() * 1000
    ),
    favoritesCount=100,
    viewsCount=27,
    lastViewedAt=round(
        datetime.strptime("2022-02-10 10:45:30", "%Y-%m-%d %H:%M:%S").timestamp() * 1000
    ),
)

dashboard = Dashboard(
    platform="looker",
    name="dashboards.999999",
    extra_aspects=[usage_day_2_stats, absolute_usage_day_2_stats],
)

client.entities.update(dashboard)

# Day 3: Single user activity and absolute usage statistics
usage_day_3_user_counts: List[models.DashboardUserUsageCountsClass] = [
    models.DashboardUserUsageCountsClass(
        user=str(CorpUserUrn("user1")),
        executionsCount=2,
        usageCount=2,
    ),
]

usage_day_3_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-11", "%Y-%m-%d").timestamp() * 1000
    ),
    eventGranularity=models.TimeWindowSizeClass(unit=models.CalendarIntervalClass.DAY),
    uniqueUserCount=1,
    executionsCount=2,
    userCounts=usage_day_3_user_counts,
)

absolute_usage_day_3_stats = models.DashboardUsageStatisticsClass(
    timestampMillis=round(
        datetime.strptime("2022-02-11", "%Y-%m-%d").timestamp() * 1000
    ),
    favoritesCount=102,
    viewsCount=30,
    lastViewedAt=round(
        datetime.strptime("2022-02-11 02:45:30", "%Y-%m-%d %H:%M:%S").timestamp() * 1000
    ),
)

dashboard = Dashboard(
    platform="looker",
    name="dashboards.999999",
    extra_aspects=[usage_day_3_stats, absolute_usage_day_3_stats],
)

client.entities.update(dashboard)
