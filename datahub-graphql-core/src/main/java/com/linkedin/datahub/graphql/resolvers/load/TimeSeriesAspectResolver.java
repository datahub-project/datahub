package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.EntitySpec;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.FilterInput;
import com.linkedin.datahub.graphql.generated.TimeSeriesAspect;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.DataLoader;

/**
 * Generic GraphQL resolver responsible for resolving a list of TimeSeries Aspect Types. The purpose
 * of this class is to consolidate the logic of calling the remote GMS "getTimeSeriesAspectValues"
 * API to a single place.
 *
 * <p>It is expected that the query takes as input an optional startTimeMillis, endTimeMillis, and
 * limit arguments used for filtering the specific TimeSeries Aspects to be fetched.
 *
 * <p>On creation of a TimeSeriesAspectResolver, it is expected that a mapper capable of mapping a
 * generic {@link EnvelopedAspect} to a GraphQL {@link TimeSeriesAspect} is provided. This wil be
 * invoked for each {@link EnvelopedAspect} received from the GMS getTimeSeriesAspectValues API.
 */
@Slf4j
public class TimeSeriesAspectResolver
    implements DataFetcher<CompletableFuture<List<TimeSeriesAspect>>> {

  private final EntityClient _client;
  private final String _entityName;
  private final String _aspectName;
  private final BiFunction<QueryContext, EnvelopedAspect, TimeSeriesAspect> _aspectMapper;
  private final SortCriterion _sort;
  private final boolean _batchLoadEnabled;

  public TimeSeriesAspectResolver(
      final EntityClient client,
      final String entityName,
      final String aspectName,
      final BiFunction<QueryContext, EnvelopedAspect, TimeSeriesAspect> aspectMapper) {
    this(client, entityName, aspectName, aspectMapper, null, true);
  }

  public TimeSeriesAspectResolver(
      final EntityClient client,
      final String entityName,
      final String aspectName,
      final BiFunction<QueryContext, EnvelopedAspect, TimeSeriesAspect> aspectMapper,
      final SortCriterion sort) {
    this(client, entityName, aspectName, aspectMapper, sort, true);
  }

  public TimeSeriesAspectResolver(
      final EntityClient client,
      final String entityName,
      final String aspectName,
      final BiFunction<QueryContext, EnvelopedAspect, TimeSeriesAspect> aspectMapper,
      final SortCriterion sort,
      final boolean batchLoadEnabled) {
    _client = client;
    _entityName = entityName;
    _aspectName = aspectName;
    _aspectMapper = aspectMapper;
    _sort = sort;
    _batchLoadEnabled = batchLoadEnabled;
  }

  /** Check whether the actor is authorized to fetch the timeseries aspect given the resource urn */
  private boolean isAuthorized(QueryContext context, String urn) {
    if (_entityName.equals(Constants.DATASET_ENTITY_NAME)
        && _aspectName.equals(Constants.DATASET_PROFILE_ASPECT_NAME)) {
      return AuthUtil.isAuthorized(
          context.getOperationContext(),
          PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE,
          new EntitySpec(_entityName, urn));
    }
    if (_entityName.equals(Constants.DATASET_ENTITY_NAME)
        && _aspectName.equals(Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME)) {
      return AuthUtil.isAuthorized(
          context.getOperationContext(),
          PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE,
          new EntitySpec(_entityName, urn));
    }
    if (_entityName.equals(Constants.DATASET_ENTITY_NAME)
        && _aspectName.equals(Constants.DATASET_OPERATION_ASPECT_NAME)) {
      return AuthUtil.isAuthorized(
          context.getOperationContext(),
          PoliciesConfig.VIEW_DATASET_OPERATIONS_PRIVILEGE,
          new EntitySpec(_entityName, urn));
    }
    return true;
  }

  @Override
  public CompletableFuture<List<TimeSeriesAspect>> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final String urn = ((Entity) environment.getSource()).getUrn();

    // Auth is checked eagerly per-URN before DataLoader dispatch. Unauthorized URNs never
    // submit a key to the loader, so they cannot receive or leak data from authorized URNs
    // that are batched in the same request.
    if (!isAuthorized(context, urn)) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
    final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
    final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);
    final FilterInput maybeFilters =
        environment.getArgument("filter") != null
            ? bindArgument(environment.getArgument("filter"), FilterInput.class)
            : null;
    final Filter maybeFilter = buildFilters(maybeFilters);

    final DataLoader<TimeseriesAspectBatchLoader.Key, List<EnvelopedAspect>> loader =
        _batchLoadEnabled
            ? environment
                .getDataLoaderRegistry()
                .getDataLoader(TimeseriesAspectBatchLoader.LOADER_NAME)
            : null;

    if (loader != null) {
      final TimeseriesAspectBatchLoader.Key key =
          new TimeseriesAspectBatchLoader.Key(
              urn,
              _entityName,
              _aspectName,
              maybeStartTimeMillis,
              maybeEndTimeMillis,
              maybeLimit,
              maybeFilter,
              _sort);
      return loader
          .load(key)
          .thenApply(
              aspects ->
                  aspects.stream()
                      .map(a -> _aspectMapper.apply(context, a))
                      .collect(Collectors.toList()));
    }

    // Fallback: direct single-URN call when no DataLoader is registered.
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return _client
                .getTimeseriesAspectValues(
                    context.getOperationContext(),
                    urn,
                    _entityName,
                    _aspectName,
                    maybeStartTimeMillis,
                    maybeEndTimeMillis,
                    maybeLimit,
                    maybeFilter,
                    _sort)
                .stream()
                .map(a -> _aspectMapper.apply(context, a))
                .collect(Collectors.toList());
          } catch (RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve aspects from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Filter buildFilters(@Nullable FilterInput maybeFilters) {
    if (maybeFilters == null) {
      return null;
    }
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            maybeFilters.getAnd().stream()
                                .map(ResolverUtils::criterionFromFilter)
                                .collect(Collectors.toList())))));
  }
}
