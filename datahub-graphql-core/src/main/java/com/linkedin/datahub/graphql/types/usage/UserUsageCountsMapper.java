package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.UserUsageCounts;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UserUsageCountsMapper
    implements ModelMapper<com.linkedin.usage.UserUsageCounts, UserUsageCounts> {

  public static final UserUsageCountsMapper INSTANCE = new UserUsageCountsMapper();

  public static UserUsageCounts map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.usage.UserUsageCounts pdlUsageResultAggregations) {
    return INSTANCE.apply(context, pdlUsageResultAggregations);
  }

  @Override
  public UserUsageCounts apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.usage.UserUsageCounts pdlUsageResultAggregations) {
    UserUsageCounts result = new UserUsageCounts();
    if (pdlUsageResultAggregations.hasUser()) {
      CorpUser partialUser = new CorpUser();
      partialUser.setUrn(pdlUsageResultAggregations.getUser().toString());
      result.setUser(partialUser);
    }

    result.setCount(pdlUsageResultAggregations.getCount());
    result.setUserEmail(pdlUsageResultAggregations.getUserEmail());
    return result;
  }
}
