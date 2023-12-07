package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.generated.FieldUsageCounts;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class FieldUsageCountsMapper
    implements ModelMapper<com.linkedin.usage.FieldUsageCounts, FieldUsageCounts> {

  public static final FieldUsageCountsMapper INSTANCE = new FieldUsageCountsMapper();

  public static FieldUsageCounts map(
      @Nonnull final com.linkedin.usage.FieldUsageCounts usageCounts) {
    return INSTANCE.apply(usageCounts);
  }

  @Override
  public FieldUsageCounts apply(@Nonnull final com.linkedin.usage.FieldUsageCounts usageCounts) {
    FieldUsageCounts result = new FieldUsageCounts();
    result.setCount(usageCounts.getCount());
    result.setFieldName(usageCounts.getFieldName());

    return result;
  }
}
