package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.datahub.graphql.generated.UsageQueryResultAggregations;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;


public class UsageQueryResultAggregationMapper implements
                                               ModelMapper<com.linkedin.usage.UsageQueryResultAggregations, UsageQueryResultAggregations> {

  public static final UsageQueryResultAggregationMapper INSTANCE = new UsageQueryResultAggregationMapper();

  public static UsageQueryResultAggregations map(@Nonnull final com.linkedin.usage.UsageQueryResultAggregations pdlUsageResultAggregations) {
    return INSTANCE.apply(pdlUsageResultAggregations);
  }

  @Override
  public UsageQueryResultAggregations apply(@Nonnull final com.linkedin.usage.UsageQueryResultAggregations pdlUsageResultAggregations) {
    UsageQueryResultAggregations result = new UsageQueryResultAggregations();
    result.setTotalSqlQueries(pdlUsageResultAggregations.getTotalSqlQueries());
    result.setUniqueUserCount(pdlUsageResultAggregations.getUniqueUserCount());
    result.setUsers(pdlUsageResultAggregations.getUsers());
  }
}
