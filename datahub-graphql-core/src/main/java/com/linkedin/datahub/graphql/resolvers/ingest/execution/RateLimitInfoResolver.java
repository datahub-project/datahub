package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.RateLimitInfo;
import com.linkedin.datahub.graphql.generated.RateLimitThrottledValue;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * Resolver that returns the current rate limiting status by querying the ThrottleSensor. If no
 * sensor is available (e.g., rate limiting disabled), returns NOT_THROTTLED.
 */
public class RateLimitInfoResolver implements DataFetcher<CompletableFuture<RateLimitInfo>> {

  @Nullable private final ThrottleSensor rateLimitThrottle;

  public RateLimitInfoResolver(@Nullable final ThrottleSensor rateLimitThrottle) {
    this.rateLimitThrottle = rateLimitThrottle;
  }

  @Override
  public CompletableFuture<RateLimitInfo> get(final DataFetchingEnvironment environment) {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final RateLimitInfo rateLimitInfo = new RateLimitInfo();

          // If no sensor is available, default to NOT_THROTTLED
          // The default implementation of getIsThrottled() returns false
          final boolean isThrottled =
              rateLimitThrottle != null && rateLimitThrottle.getIsThrottled();
          rateLimitInfo.setRateLimitThrottled(
              isThrottled
                  ? RateLimitThrottledValue.THROTTLED
                  : RateLimitThrottledValue.NOT_THROTTLED);
          return rateLimitInfo;
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
