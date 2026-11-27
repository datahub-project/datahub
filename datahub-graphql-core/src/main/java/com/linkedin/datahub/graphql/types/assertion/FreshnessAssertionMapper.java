package com.linkedin.datahub.graphql.types.assertion;

import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionInfo;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionSchedule;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionScheduleType;
import com.linkedin.datahub.graphql.generated.FreshnessAssertionType;
import com.linkedin.datahub.graphql.generated.FreshnessCronSchedule;
import com.linkedin.datahub.graphql.types.common.mappers.util.UnknownEnumValues;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetFilterMapper;
import javax.annotation.Nullable;

public class FreshnessAssertionMapper extends AssertionMapper {

  public static FreshnessAssertionInfo mapFreshnessAssertionInfo(
      @Nullable final QueryContext context,
      final com.linkedin.assertion.FreshnessAssertionInfo gmsFreshnessAssertionInfo) {
    FreshnessAssertionInfo freshnessAssertionInfo = new FreshnessAssertionInfo();
    freshnessAssertionInfo.setEntityUrn(gmsFreshnessAssertionInfo.getEntity().toString());
    freshnessAssertionInfo.setType(
        UnknownEnumValues.require(
            FreshnessAssertionType.class, gmsFreshnessAssertionInfo.getType()));
    if (gmsFreshnessAssertionInfo.hasSchedule()) {
      freshnessAssertionInfo.setSchedule(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "freshness schedule",
              () -> mapFreshnessAssertionSchedule(gmsFreshnessAssertionInfo.getSchedule())));
    }
    if (gmsFreshnessAssertionInfo.hasFilter()) {
      freshnessAssertionInfo.setFilter(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "dataset filter",
              () -> DatasetFilterMapper.map(context, gmsFreshnessAssertionInfo.getFilter())));
    }
    return freshnessAssertionInfo;
  }

  private static FreshnessCronSchedule mapFreshnessCronSchedule(
      final com.linkedin.assertion.FreshnessCronSchedule gmsCronSchedule) {
    FreshnessCronSchedule cronSchedule = new FreshnessCronSchedule();
    cronSchedule.setCron(gmsCronSchedule.getCron());
    cronSchedule.setTimezone(gmsCronSchedule.getTimezone());
    cronSchedule.setWindowStartOffsetMs(gmsCronSchedule.getWindowStartOffsetMs(GetMode.NULL));
    return cronSchedule;
  }

  private static FreshnessAssertionSchedule mapFreshnessAssertionSchedule(
      final com.linkedin.assertion.FreshnessAssertionSchedule gmsFreshnessAssertionSchedule) {
    FreshnessAssertionSchedule freshnessAssertionSchedule = new FreshnessAssertionSchedule();
    freshnessAssertionSchedule.setType(
        UnknownEnumValues.require(
            FreshnessAssertionScheduleType.class, gmsFreshnessAssertionSchedule.getType()));
    if (gmsFreshnessAssertionSchedule.hasCron()) {
      freshnessAssertionSchedule.setCron(
          mapFreshnessCronSchedule(gmsFreshnessAssertionSchedule.getCron()));
    }
    if (gmsFreshnessAssertionSchedule.hasFixedInterval()) {
      freshnessAssertionSchedule.setFixedInterval(
          UnknownEnumValues.leaveOutIfUnrepresentable(
              UNREPRESENTABLE,
              "fixed interval schedule",
              () -> mapFixedIntervalSchedule(gmsFreshnessAssertionSchedule.getFixedInterval())));
    }
    return freshnessAssertionSchedule;
  }

  private FreshnessAssertionMapper() {}
}
