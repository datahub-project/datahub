package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DashboardUsageMetricMapper implements TimeSeriesAspectMapper<DashboardUsageMetrics> {

  public static final DashboardUsageMetricMapper INSTANCE = new DashboardUsageMetricMapper();

  public static DashboardUsageMetrics map(
      @Nullable QueryContext context, @Nonnull final EnvelopedAspect envelopedAspect) {
    return INSTANCE.apply(context, envelopedAspect);
  }

  @Override
  public DashboardUsageMetrics apply(
      @Nullable QueryContext context, EnvelopedAspect envelopedAspect) {
    com.linkedin.dashboard.DashboardUsageStatistics gmsDashboardUsageStatistics =
        GenericRecordUtils.deserializeAspect(
            envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(),
            com.linkedin.dashboard.DashboardUsageStatistics.class);

    final com.linkedin.datahub.graphql.generated.DashboardUsageMetrics dashboardUsageMetrics =
        new com.linkedin.datahub.graphql.generated.DashboardUsageMetrics();
    dashboardUsageMetrics.setLastViewed(gmsDashboardUsageStatistics.getLastViewedAt());
    dashboardUsageMetrics.setViewsCount(gmsDashboardUsageStatistics.getViewsCount());
    dashboardUsageMetrics.setExecutionsCount(gmsDashboardUsageStatistics.getExecutionsCount());
    dashboardUsageMetrics.setFavoritesCount(gmsDashboardUsageStatistics.getFavoritesCount());
    dashboardUsageMetrics.setTimestampMillis(gmsDashboardUsageStatistics.getTimestampMillis());

    return dashboardUsageMetrics;
  }
}
