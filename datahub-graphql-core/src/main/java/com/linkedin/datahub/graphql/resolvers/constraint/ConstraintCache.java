package com.linkedin.datahub.graphql.resolvers.constraint;

import com.linkedin.datahub.graphql.QueryContext;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.r2.RemoteInvocationException;
import java.util.HashMap;
import lombok.Data;


@Data
public class ConstraintCache {

  private static final long REFRESH_TIME_IN_MILLI = 60 * 1000;
  private static final String CONSTRAINT_ENTITY_NAME = "constraint";

  // TODO(Gabe): support pagination among constraints
  private static final int MAX_CONSTRAINTS = 100;

  private static final ConstraintCache CACHE = new ConstraintCache();

  private ListResult cachedListResult;
  private long lastFetchedTs = 0;

  private ConstraintCache() { }

  public static ListResult getCachedConstraints(EntityClient entityClient, QueryContext context) throws RemoteInvocationException {
    if (
        System.currentTimeMillis() - CACHE.getLastFetchedTs() < REFRESH_TIME_IN_MILLI
            && CACHE.getCachedListResult() != null
    ) {
      return CACHE.getCachedListResult();
    }

    ListResult currentConstraintList =
        entityClient.list(CONSTRAINT_ENTITY_NAME, new HashMap<>(), 0, MAX_CONSTRAINTS, context.getAuthentication());
    CACHE.setCachedListResult(currentConstraintList);
    CACHE.setLastFetchedTs(System.currentTimeMillis());

    return currentConstraintList;
  }

}
