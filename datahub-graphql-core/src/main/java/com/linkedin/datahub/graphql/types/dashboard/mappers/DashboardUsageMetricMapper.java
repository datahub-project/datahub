package com.linkedin.datahub.graphql.types.dashboard.mappers;

import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.types.mappers.TimeSeriesAspectMapper;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.utils.GenericRecordUtils;
import javax.annotation.Nonnull;


public class DashboardUsageMetricMapper implements TimeSeriesAspectMapper<DashboardUsageMetrics> {

  public static final DashboardUsageMetricMapper INSTANCE = new DashboardUsageMetricMapper();

  public static DashboardUsageMetrics map(@Nonnull final EnvelopedAspect envelopedAspect) {
    return INSTANCE.apply(envelopedAspect);
  }

  @Override
  public DashboardUsageMetrics apply(EnvelopedAspect envelopedAspect) {
    com.linkedin.dashboard.DashboardUsageStatistics gmsDashboardUsageStatistics =
        GenericRecordUtils.deserializeAspect(envelopedAspect.getAspect().getValue(),
            envelopedAspect.getAspect().getContentType(), com.linkedin.dashboard.DashboardUsageStatistics.class);

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
