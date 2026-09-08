package com.linkedin.metadata.analytics.postgres;

import java.time.Instant;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PostgresAnalyticsEventInsert {
  @Nonnull Instant eventTime;
  @Nonnull String metricFamily;
  @Nonnull String eventId;
  @Nullable String metricName;
  @Nullable String eventType;
  @Nullable String actorUrn;
  @Nullable String entityUrn;
  @Nullable String entityType;
  @Nullable String usageSource;
  @Nullable String browserId;
  @Nullable String query;
  @Nullable String section;
  @Nullable String actionType;
  @Nullable String aspectName;
  @Nullable String dimensionsJson;
  @Nonnull String documentJson;
}
