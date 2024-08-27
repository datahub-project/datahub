package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UsageQueryResultAggregations;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UsageQueryResultAggregationMapper
    implements ModelMapper<
        com.linkedin.usage.UsageQueryResultAggregations, UsageQueryResultAggregations> {

  public static final UsageQueryResultAggregationMapper INSTANCE =
      new UsageQueryResultAggregationMapper();

  public static UsageQueryResultAggregations map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.usage.UsageQueryResultAggregations pdlUsageResultAggregations) {
    return INSTANCE.apply(context, pdlUsageResultAggregations);
  }

  @Override
  public UsageQueryResultAggregations apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.usage.UsageQueryResultAggregations pdlUsageResultAggregations) {
    UsageQueryResultAggregations result = new UsageQueryResultAggregations();
    result.setTotalSqlQueries(pdlUsageResultAggregations.getTotalSqlQueries());
    result.setUniqueUserCount(pdlUsageResultAggregations.getUniqueUserCount());
    if (pdlUsageResultAggregations.hasFields()) {
      result.setFields(
          pdlUsageResultAggregations.getFields().stream()
              .map(f -> FieldUsageCountsMapper.map(context, f))
              .collect(Collectors.toList()));
    }
    if (pdlUsageResultAggregations.hasUsers()) {
      result.setUsers(
          pdlUsageResultAggregations.getUsers().stream()
              .map(aggregation -> UserUsageCountsMapper.map(context, aggregation))
              .collect(Collectors.toList()));
    }
    return result;
  }
}
