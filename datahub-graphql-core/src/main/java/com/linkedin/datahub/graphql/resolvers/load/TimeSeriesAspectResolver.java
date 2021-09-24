package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.TimeSeriesAspect;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
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
 * On creation of a TimeSeriesAspectResolver, it is expected that a mapper capable of mapping
 * a generic {@link EnvelopedAspect} to a GraphQL {@link TimeSeriesAspect} is provided. This wil
 * be invoked for each {@link EnvelopedAspect} received from the GMS getTimeSeriesAspectValues API.
 *
 */
public class TimeSeriesAspectResolver implements DataFetcher<CompletableFuture<List<TimeSeriesAspect>>> {

  private final AspectClient _client;
  private final String _entityName;
  private final String _aspectName;
  private final Function<EnvelopedAspect, TimeSeriesAspect> _aspectMapper;

  public TimeSeriesAspectResolver(final AspectClient client, final String entityName, final String aspectName,
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
      // Fetch the urn, assuming the parent has an urn field.
      // todo: what if the parent urn isn't projected?
      final String urn = ((Entity) environment.getSource()).getUrn();
      final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
      final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
      // Max number of aspects to return.
      final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);

      try {
        // Step 1: Get aspects.
        List<EnvelopedAspect> aspects =
            _client.getTimeseriesAspectValues(urn, _entityName, _aspectName, maybeStartTimeMillis, maybeEndTimeMillis,
                maybeLimit, context.getActor());

        // Step 2: Bind profiles into GraphQL strong types.
        return aspects.stream().map(_aspectMapper::apply).collect(Collectors.toList());
      } catch (RemoteInvocationException e) {
        throw new RuntimeException("Failed to retrieve aspects from GMS", e);
      }
    });
  }
}
