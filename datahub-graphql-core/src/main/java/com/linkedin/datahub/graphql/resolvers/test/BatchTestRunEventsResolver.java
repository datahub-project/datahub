package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchTestRunEvent;
import com.linkedin.datahub.graphql.generated.BatchTestRunEventsResult;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * GraphQL Resolver used for fetching BatchTestRunEvents.
 */
public class BatchTestRunEventsResolver implements DataFetcher<CompletableFuture<BatchTestRunEventsResult>> {

  private final EntityClient _client;

  public BatchTestRunEventsResolver(@Nonnull final EntityClient client) {
    _client = Objects.requireNonNull(client, "client must not be null");
  }

  @Override
  public CompletableFuture<BatchTestRunEventsResult> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(() -> {

      final QueryContext context = environment.getContext();
      final String urn = ((Test) environment.getSource()).getUrn();
      final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
      final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
      final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);

      try {
        // Step 1: Fetch aspects from GMS
        List<EnvelopedAspect> aspects = _client.getTimeseriesAspectValues(
            urn,
            Constants.TEST_ENTITY_NAME,
            AcrylConstants.BATCH_TEST_RUN_EVENT_ASPECT_NAME,
            maybeStartTimeMillis,
            maybeEndTimeMillis,
            maybeLimit,
            null,
            context.getAuthentication());

        // Step 2: Bind profiles into GraphQL strong types.
        List<BatchTestRunEvent> runEvents = aspects.stream().map(BatchTestRunEventMapper::map).collect(Collectors.toList());

        // Step 3: Package and return response.
        final BatchTestRunEventsResult result = new BatchTestRunEventsResult();
        result.setBatchRunEvents(runEvents);
        return result;
      } catch (RemoteInvocationException e) {
        throw new RuntimeException("Failed to retrieve Batch Test  Run Events from GMS", e);
      }
    });
  }
}
