package com.linkedin.datahub.graphql.resolvers.load;

import com.datahub.authorization.ResourceSpec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FilterInput;
import com.linkedin.datahub.graphql.generated.TimeSeriesAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


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
@Slf4j
public class TimeSeriesAspectResolver implements DataFetcher<CompletableFuture<List<TimeSeriesAspect>>> {

  private final EntityClient _client;
  private final String _entityName;
  private final String _aspectName;
  private final Function<EnvelopedAspect, TimeSeriesAspect> _aspectMapper;

  public TimeSeriesAspectResolver(final EntityClient client, final String entityName, final String aspectName,
      final Function<EnvelopedAspect, TimeSeriesAspect> aspectMapper) {
    _client = client;
    _entityName = entityName;
    _aspectName = aspectName;
    _aspectMapper = aspectMapper;
  }

  /**
   * Check whether the actor is authorized to fetch the timeseries aspect given the resource urn
   */
  private boolean isAuthorized(QueryContext context, String urn) {
    if (_entityName.equals(Constants.DATASET_ENTITY_NAME) && _aspectName.equals(
        Constants.DATASET_PROFILE_ASPECT_NAME)) {
      return AuthorizationUtils.isAuthorized(context, Optional.of(new ResourceSpec(_entityName, urn)),
          PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE);
    }
    return true;
  }

  @Override
  public CompletableFuture<List<TimeSeriesAspect>> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(() -> {

      final QueryContext context = environment.getContext();
      // Fetch the urn, assuming the parent has an urn field.
      // todo: what if the parent urn isn't projected?
      final String urn = ((Entity) environment.getSource()).getUrn();

      if (!isAuthorized(context, urn)) {
        return Collections.emptyList();
      }

      final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
      final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
      // Max number of aspects to return.
      final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);
      final FilterInput maybeFilters = environment.getArgument("filter") != null
          ? bindArgument(environment.getArgument("filter"), FilterInput.class)
          : null;

      try {
        // Step 1: Get aspects.
        List<EnvelopedAspect> aspects =
            _client.getTimeseriesAspectValues(urn, _entityName, _aspectName, maybeStartTimeMillis, maybeEndTimeMillis,
                maybeLimit, null, buildFilters(maybeFilters), context.getAuthentication());

        // Step 2: Bind profiles into GraphQL strong types.
        return aspects.stream().map(_aspectMapper::apply).collect(Collectors.toList());
      } catch (RemoteInvocationException e) {
        throw new RuntimeException("Failed to retrieve aspects from GMS", e);
      }
    });
  }

  private Filter buildFilters(@Nullable FilterInput maybeFilters) {
    if (maybeFilters == null) {
      return null;
    }
    return new Filter().setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(new CriterionArray(maybeFilters.getAnd().stream()
        .map(filter -> criterionFromFilter(filter))
        .collect(Collectors.toList())))));
  }
}
