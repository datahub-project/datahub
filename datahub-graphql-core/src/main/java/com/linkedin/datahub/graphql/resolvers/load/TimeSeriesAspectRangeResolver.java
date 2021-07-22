package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.TimeRange;
import com.linkedin.datahub.graphql.generated.TimeSeriesAspect;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetProfileMapper;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.Condition;
import com.linkedin.metadata.query.Criterion;
import com.linkedin.metadata.query.CriterionArray;
import com.linkedin.metadata.query.Filter;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * Generic GraphQL resolver responsible for
 *
 *    1. Generating a single input AspectLoadKey.
 *    2. Resolving a single {@link Aspect}.
 *
 *    TODO: This needs to call a "Type" that performs the mapping.
 */
public class TimeSeriesAspectRangeResolver
    implements DataFetcher<CompletableFuture<List<TimeSeriesAspect>>> {

  private static final String TIMESTAMP_FIELD_NAME = "timestampMillis";

  private final String _entityName;
  private final String _aspectName;
  private final AspectClient _client;

  public TimeSeriesAspectRangeResolver(
      final String entityName,
      final String aspectName,
      final AspectClient client) {
    _entityName = entityName;
    _aspectName = aspectName;
    _client = client;
  }

  @Override
  public CompletableFuture<List<TimeSeriesAspect>> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(() -> {
      final String urn = ((Entity) environment.getSource()).getUrn();

      // TODO: Also support a start and end time. Or a time and count.
      // Currently, we only support this less granular look-back window.
      // For operability we'll likely want to permit a range.

      TimeRange range = null;
      final String maybeTimeRange = environment.getArgumentOrDefault
          ("range", null);
      if (maybeTimeRange != null) {
        range = TimeRange.valueOf(maybeTimeRange);
      }

      // Max number of aspects to return.
      final Integer limit = environment.getArgumentOrDefault("count", null);
      Filter filter = null;

      if (range != null) {
        filter = buildRangeFilter(range);
      }

      List<EnvelopedAspect> aspects;
      try {
        // Step 1: Get profile aspects.
        aspects = _client.getAspectValues(
            urn,
            _entityName,
            _aspectName,
            filter,
            limit);

        // Step 2: Bind profiles into GraphQL strong types.
        return aspects.stream()
            .map(DatasetProfileMapper::map)
            .collect(Collectors.toList());

      } catch (RemoteInvocationException e) {
        // TODO:
        throw new RuntimeException("Failed to retrieve aspects from GMS", e);
      }
    });
  }

  private Filter buildRangeFilter(TimeRange range) {
    Filter filter = new Filter();

    CriterionArray criterionArray = new CriterionArray();

    Long endTime = System.currentTimeMillis();
    Long startTime = endTime - rangeToMillis(range);

    Criterion endTimeCriterion = new Criterion();
    endTimeCriterion.setField(TIMESTAMP_FIELD_NAME);
    endTimeCriterion.setCondition(Condition.LESS_THAN_OR_EQUAL_TO);
    endTimeCriterion.setValue(endTime.toString());

    Criterion startTimeCriterion = new Criterion();
    endTimeCriterion.setField(TIMESTAMP_FIELD_NAME);
    endTimeCriterion.setCondition(Condition.GREATER_THAN_OR_EQUAL_TO);
    endTimeCriterion.setValue(startTime.toString());

    criterionArray.add(endTimeCriterion);
    criterionArray.add(startTimeCriterion);

    filter.setCriteria(criterionArray);
    return filter;
  }

  private Long rangeToMillis(TimeRange range) {
    final long oneHourMillis = 60 * 60 * 1000;
    final long oneDayMillis = 24 * oneHourMillis;
    switch (range) {
      case DAY:
        return (2 * oneDayMillis + 1);
      case WEEK:
        return (8 * oneDayMillis + 1);
      case MONTH:
        return (31 * oneDayMillis + 1);
      case QUARTER:
        return (92 * oneDayMillis + 1);
      case YEAR:
        return (366 * oneDayMillis + 1);
      case ALL:
        return System.currentTimeMillis();
      default:
        throw new RuntimeException(String.format("Unrecognized TimeRange %s provided to TimeSeriesAspectRangeResolver", range));
    }
  }
}
