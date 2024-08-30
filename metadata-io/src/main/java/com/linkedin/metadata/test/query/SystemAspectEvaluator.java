package com.linkedin.metadata.test.query;

import static com.linkedin.metadata.utils.SystemMetadataUtils.*;
import static java.util.function.BinaryOperator.*;

import com.linkedin.common.Siblings;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluator that supports resolving the '__firstSynchronized', '__lastSynchronized',
 * '__lastObserved', '__created' system queries for a given URN.
 *
 * @implNote These queries do not support Timeseries Aspects.
 */
@Slf4j
@RequiredArgsConstructor
public class SystemAspectEvaluator extends BaseQueryEvaluator {

  private final EntityService<?> entityService;

  private static final String FIRST_SYNCHRONIZED_FIELD_NAME = "__firstSynchronized";

  private static final String LAST_SYNCHRONIZED_FIELD_NAME = "__lastSynchronized";

  private static final String LAST_OBSERVED_FIELD_NAME = "__lastObserved";

  // Set of aspects to not evaluate system metrics on.
  private static final Set<String> ASPECTS_TO_IGNORE =
      Set.of(
          Constants.TEST_RESULTS_ASPECT_NAME,
          Constants.STORAGE_FEATURES_ASPECT_NAME,
          Constants.USAGE_FEATURES_ASPECT_NAME);

  @Override
  public boolean isEligible(@Nonnull final String entityType, @Nonnull final TestQuery query) {
    if (query.getQueryParts().size() != 1) {
      return false;
    }

    final String queryName = query.getQuery();
    return FIRST_SYNCHRONIZED_FIELD_NAME.equalsIgnoreCase(queryName)
        || LAST_SYNCHRONIZED_FIELD_NAME.equalsIgnoreCase(queryName)
        || LAST_OBSERVED_FIELD_NAME.equalsIgnoreCase(queryName);
  }

  @Override
  @Nonnull
  public ValidationResult validateQuery(
      @Nonnull final String entityType, @Nonnull final TestQuery query)
      throws IllegalArgumentException {
    return new ValidationResult(isEligible(entityType, query), Collections.emptyList());
  }

  @Override
  @Nonnull
  public Map<Urn, Map<TestQuery, TestQueryResponse>> evaluate(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityType,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<TestQuery> queries) {
    final Map<Urn, Map<TestQuery, TestQueryResponse>> result = new HashMap<>();
    for (TestQuery query : queries) {
      try {
        final EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityType);
        if (entitySpec == null) {
          log.warn("Unable to find entity spec for {} Skipping query {} ", entityType, query);
          continue;
        }
        final Set<String> aspectSpecNames =
            query.getQuery().equalsIgnoreCase(FIRST_SYNCHRONIZED_FIELD_NAME)
                // Explicitly try to pull siblings to check whether a sibling was synced before the
                // asset we are testing.
                ? Set.of(entitySpec.getKeyAspectName(), Constants.SIBLINGS_ASPECT_NAME)
                : entitySpec.getAspectSpecs().stream()
                    .map(AspectSpec::getName)
                    .collect(Collectors.toSet());

        entityService
            .getEntitiesV2(opContext, entityType, urns, aspectSpecNames)
            .forEach(
                (urn, response) -> {
                  result.putIfAbsent(urn, new HashMap<>());
                  try {
                    Map<Urn, EntityResponse> siblingData = null;
                    // If test urn has a sibling, retrieve sibling's data
                    if (response.getAspects().containsKey(Constants.SIBLINGS_ASPECT_NAME)) {
                      Siblings siblings =
                          new Siblings(
                              response
                                  .getAspects()
                                  .get(Constants.SIBLINGS_ASPECT_NAME)
                                  .getValue()
                                  .data());
                      Set<Urn> siblingUrns = new HashSet<>(siblings.getSiblings());

                      siblingData =
                          entityService.getEntitiesV2(
                              opContext, entityType, siblingUrns, aspectSpecNames);
                    }
                    result
                        .get(urn)
                        .put(
                            query,
                            buildSystemQueryResponse(opContext, query, urn, response, siblingData));
                  } catch (RuntimeException | URISyntaxException e) {
                    log.error(
                        "RuntimeException for urn: {} for query {}. Skipping running test for urn",
                        urn,
                        query,
                        e);
                  }
                });
      } catch (URISyntaxException e) {
        log.error("Error while fetching aspects for urns {}", urns, e);
        throw new RuntimeException(String.format("Error while fetching aspects for urns %s", urns));
      }
    }
    return result;
  }

  private TestQueryResponse buildSystemQueryResponse(
      @Nonnull OperationContext opContext,
      @Nonnull final TestQuery query,
      @Nonnull final Urn urn,
      @Nonnull final EntityResponse entityResponse,
      @Nullable Map<Urn, EntityResponse> siblingData) {
    final String queryName = query.getQuery();

    switch (queryName) {
      case FIRST_SYNCHRONIZED_FIELD_NAME:
        final TestQueryResponse firstSyncResponse =
            compute(
                (response) -> firstSynchronized(opContext, response),
                minBy(Long::compareTo),
                entityResponse,
                siblingData);
        if (firstSyncResponse == null) {
          throw new RuntimeException(
              String.format("First synchronized time is null for %s and its siblings", urn));
        }
        return firstSyncResponse;
      case LAST_SYNCHRONIZED_FIELD_NAME:
        final TestQueryResponse lastSyncResponse =
            compute(
                (response) -> lastIngestedTime(response.getAspects()),
                maxBy(Long::compareTo),
                entityResponse,
                siblingData);
        if (lastSyncResponse == null) {
          throw new RuntimeException(
              String.format("Last synchronized time is null for %s and its siblings", urn));
        }
        return lastSyncResponse;
      case LAST_OBSERVED_FIELD_NAME:
        final TestQueryResponse lastObserveResponse =
            compute(
                SystemAspectEvaluator::lastObserved,
                maxBy(Long::compareTo),
                entityResponse,
                siblingData);
        if (lastObserveResponse == null) {
          throw new RuntimeException(
              String.format("Last observed time is null for %s and its siblings", urn));
        }
        return lastObserveResponse;
      default:
        throw new RuntimeException(String.format("Unknown query %s", queryName));
    }
  }

  @Nullable
  private TestQueryResponse compute(
      Function<EntityResponse, Long> getAspectMetric,
      BinaryOperator<Long> op,
      @Nonnull EntityResponse entityResponse,
      @Nullable Map<Urn, EntityResponse> siblingData) {
    final Long entityResult = getAspectMetric.apply(entityResponse);
    Optional<Long> siblingResult = Optional.empty();
    if (siblingData != null) {
      siblingResult =
          siblingData.keySet().stream()
              .map(siblingData::get)
              .map(getAspectMetric)
              .filter(Objects::nonNull)
              .reduce(op);
    }

    if (entityResult != null) {
      return siblingResult
          .map(
              sibling ->
                  new TestQueryResponse(
                      Collections.singletonList(Long.toString(op.apply(entityResult, sibling)))))
          .orElseGet(
              () -> new TestQueryResponse(Collections.singletonList(entityResult.toString())));
    }

    return siblingResult
        .map(aLong -> new TestQueryResponse(Collections.singletonList(aLong.toString())))
        .orElse(null);
  }

  @Nonnull
  private static Stream<EnvelopedAspect> getAspectsForProcessing(
      @Nonnull EntityResponse entityResponse) {
    return entityResponse.getAspects().values().stream()
        .filter(aspect -> !ASPECTS_TO_IGNORE.contains(aspect.getName()));
  }

  @Nullable
  private static Long lastObserved(EntityResponse entityResponse) {
    return getAspectsForProcessing(entityResponse)
        .map(EnvelopedAspect::getSystemMetadata)
        .filter(Objects::nonNull)
        .map(SystemMetadata::getLastObserved)
        .filter(Objects::nonNull)
        .max(Long::compareTo)
        .orElse(null);
  }

  @Nullable
  private Long firstSynchronized(
      @Nonnull OperationContext opContext, EntityResponse entityResponse) {
    final EntitySpec entitySpec =
        opContext.getEntityRegistry().getEntitySpec(entityResponse.getUrn().getEntityType());
    if (entitySpec == null) {
      return null;
    }
    final String keyAspectName = entitySpec.getKeyAspectName();
    final EnvelopedAspect keyAspect = entityResponse.getAspects().get(keyAspectName);
    return keyAspect != null ? keyAspect.getCreated().getTime() : null;
  }
}
