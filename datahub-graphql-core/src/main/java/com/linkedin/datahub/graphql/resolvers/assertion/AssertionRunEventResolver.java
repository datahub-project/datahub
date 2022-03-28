package com.linkedin.datahub.graphql.resolvers.assertion;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionResultType;
import com.linkedin.datahub.graphql.generated.AssertionRunEvent;
import com.linkedin.datahub.graphql.generated.AssertionRunEventsResult;
import com.linkedin.datahub.graphql.generated.AssertionRunStatus;
import com.linkedin.datahub.graphql.types.dataset.mappers.AssertionRunEventMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


/**
 * GraphQL Resolver used for fetching AssertionRunEvents.
 */
public class AssertionRunEventResolver implements DataFetcher<CompletableFuture<AssertionRunEventsResult>> {

  private final EntityClient _client;

  public AssertionRunEventResolver(final EntityClient client) {
    _client = client;
  }

  @Override
  public CompletableFuture<AssertionRunEventsResult> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(() -> {

      final QueryContext context = environment.getContext();

      final String urn = ((Assertion) environment.getSource()).getUrn();
      final String maybeStatus = environment.getArgumentOrDefault("status", null);
      final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
      final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
      final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);

      try {
        // Step 1: Fetch aspects from GMS
        List<EnvelopedAspect> aspects = _client.getTimeseriesAspectValues(
            urn,
            Constants.ASSERTION_ENTITY_NAME,
            Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
            maybeStartTimeMillis,
            maybeEndTimeMillis,
            maybeLimit,
            false,
            buildStatusFilter(maybeStatus),
            context.getAuthentication());

        // Step 2: Bind profiles into GraphQL strong types.
        List<AssertionRunEvent> runEvents = aspects.stream().map(AssertionRunEventMapper::map).collect(Collectors.toList());

        // Step 3: Package and return response.
        final AssertionRunEventsResult result = new AssertionRunEventsResult();
        result.setTotal(runEvents.size());
        result.setFailed(Math.toIntExact(runEvents.stream().filter(runEvent ->
          AssertionRunStatus.COMPLETE.equals(runEvent.getStatus())
              && runEvent.getResult() != null
              && AssertionResultType.FAILURE.equals(
              runEvent.getResult().getType()
        )).count()));
        result.setSucceeded(Math.toIntExact(runEvents.stream().filter(runEvent ->
            AssertionRunStatus.COMPLETE.equals(runEvent.getStatus())
                && runEvent.getResult() != null
                && AssertionResultType.SUCCESS.equals(runEvent.getResult().getType()
            )).count()));
        result.setRunEvents(runEvents);
        return result;
      } catch (RemoteInvocationException e) {
        throw new RuntimeException("Failed to retrieve Assertion Run Events from GMS", e);
      }
    });
  }

  @Nullable
  private Filter buildStatusFilter(@Nullable final String status) {
    if (status == null) {
      return null;
    }
    return new Filter().setOr(new ConjunctiveCriterionArray(ImmutableList.of(
        new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(
            new Criterion()
              .setField("status")
              .setValue(status)
        )))
    )));
  }
}
