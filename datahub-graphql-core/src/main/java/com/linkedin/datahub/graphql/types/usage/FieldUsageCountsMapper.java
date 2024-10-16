package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.FieldUsageCounts;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class FieldUsageCountsMapper
    implements ModelMapper<com.linkedin.usage.FieldUsageCounts, FieldUsageCounts> {

  public static final FieldUsageCountsMapper INSTANCE = new FieldUsageCountsMapper();

  public static FieldUsageCounts map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.usage.FieldUsageCounts usageCounts) {
    return INSTANCE.apply(context, usageCounts);
  }

  @Override
  public FieldUsageCounts apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.usage.FieldUsageCounts usageCounts) {
    FieldUsageCounts result = new FieldUsageCounts();
    result.setCount(usageCounts.getCount());
    result.setFieldName(usageCounts.getFieldName());

    return result;
  }
}
