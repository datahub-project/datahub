package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.TimeSeriesAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Generic GraphQL resolver responsible for resolving a list of TimeSeries Aspect Types.
 * The purpose of this class is to consolidate the logic of calling the remote GMS "getTimeSeriesAspectValues" API
 * to a single place.
 *
 * It is expected that the query takes as input an optional startTimeMillis, endTimeMillis, and limit arguments
 * used for filtering the specific TimeSeries Aspects to be fetched.
 *
 * On creation of a AssertionRunEventsResolver, it is expected that a mapper capable of mapping
 * a generic {@link EnvelopedAspect} to a GraphQL {@link TimeSeriesAspect} is provided. This wil
 * be invoked for each {@link EnvelopedAspect} received from the GMS getTimeSeriesAspectValues API.
 *
 */
public class AssertionRunEventsResolver implements DataFetcher<CompletableFuture<List<TimeSeriesAspect>>> {

  private final EntityClient _client;
  private final String _entityName;
  private final String _aspectName;
  private final Function<EnvelopedAspect, TimeSeriesAspect> _aspectMapper;

  public AssertionRunEventsResolver(final EntityClient client, final String entityName, final String aspectName,
      final Function<EnvelopedAspect, TimeSeriesAspect> aspectMapper) {
    _client = client;
    _entityName = entityName;
    _aspectName = aspectName;
    _aspectMapper = aspectMapper;
  }

  @Override
  public CompletableFuture<List<TimeSeriesAspect>> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(() -> {

      final QueryContext context = environment.getContext();

      // Fetch the urn, assuming the parent has an urn field and is of dataset type.
      // Else there must be an argument named asserteeUrn
      final Entity parentEntity = (Entity) environment.getSource();

      final String maybeAsserteeUrn = environment.getArgumentOrDefault("asserteeUrn", null);
      final String urn = parentEntity.getType() == EntityType.DATASET ? parentEntity.getUrn() : maybeAsserteeUrn;
      assert (urn != null);
      final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
      final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
      // Max number of aspects to return.
      final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);
      final String maybeAssertionUrn = parentEntity.getType() == EntityType.ASSERTION ? parentEntity.getUrn()
          : environment.getArgumentOrDefault("assertionUrn", null);

      Filter maybeFilter = null;
      List<Criterion> criteria = new ArrayList<>();

      if (maybeAssertionUrn != null) {
        criteria.add(
            new Criterion().setField("assertionUrn").setCondition(Condition.EQUAL).setValue(maybeAssertionUrn));
      }
      if (!criteria.isEmpty()) {
        maybeFilter = new Filter().setOr(
            new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria))));
      }

      try {
        // Step 1: Get aspects.
        List<EnvelopedAspect> aspects =
            _client.getTimeseriesAspectValues(urn, _entityName, _aspectName, maybeStartTimeMillis, maybeEndTimeMillis,
                maybeLimit, null, maybeFilter, context.getAuthentication());

        // Step 2: Bind profiles into GraphQL strong types.
        return aspects.stream().map(_aspectMapper::apply).collect(Collectors.toList());
      } catch (RemoteInvocationException e) {
        throw new RuntimeException("Failed to retrieve aspects from GMS", e);
      }
    });
  }
}
