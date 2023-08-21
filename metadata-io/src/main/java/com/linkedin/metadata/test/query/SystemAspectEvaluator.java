package com.linkedin.metadata.test.query;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Evaluator that supports resolving the '__firstSynchronized', '__lastSynchronized', '__lastUpdated'
 * system queries for a given URN.
 *
 * @implNote These queries do not support Timeseries Aspects.
 */
@Slf4j
@RequiredArgsConstructor
public class SystemAspectEvaluator extends BaseQueryEvaluator {

  private final EntityService entityService;

  private static final String FIRST_SYNCHRONIZED_FIELD_NAME = "__firstSynchronized";

  private static final String LAST_SYNCHRONIZED_FIELD_NAME = "__lastSynchronized";

  private static final String LAST_UPDATED_FIELD_NAME = "__lastUpdated";

  @Override
  public boolean isEligible(@Nonnull final String entityType, @Nonnull final TestQuery query) {
    if (query.getQueryParts().isEmpty()) {
      return false;
    }

    final String queryName = query.getQuery();
    return FIRST_SYNCHRONIZED_FIELD_NAME.equalsIgnoreCase(queryName) || LAST_SYNCHRONIZED_FIELD_NAME.equalsIgnoreCase(
        queryName) || LAST_UPDATED_FIELD_NAME.equalsIgnoreCase(queryName);
  }

  @Override
  @Nonnull
  public ValidationResult validateQuery(@Nonnull final String entityType, @Nonnull final TestQuery query)
      throws IllegalArgumentException {
    // Validate that query has only 1 part which is the property we want to compute
    // (__firstSynchronized or __lastSynchronized or __lastUpdated)
    final boolean result =
        query.getQueryParts().size() == 1 && (FIRST_SYNCHRONIZED_FIELD_NAME.equalsIgnoreCase(query.getQuery())
            || LAST_SYNCHRONIZED_FIELD_NAME.equalsIgnoreCase(query.getQuery())
            || LAST_UPDATED_FIELD_NAME.equalsIgnoreCase(query.getQuery()));
    return new ValidationResult(result, Collections.emptyList());
  }

  @Override
  @Nonnull
  public Map<Urn, Map<TestQuery, TestQueryResponse>> evaluate(@Nonnull final String entityType,
      @Nonnull final Set<Urn> urns, @Nonnull final Set<TestQuery> queries) {
    final Map<Urn, Map<TestQuery, TestQueryResponse>> result = new HashMap<>();
    for (TestQuery query : queries) {
      try {
        final EntitySpec entitySpec = entityService.getEntityRegistry().getEntitySpec(entityType);
        final Set<String> aspectSpecNames =
            query.getQuery().equalsIgnoreCase(FIRST_SYNCHRONIZED_FIELD_NAME) ? Set.of(entitySpec.getKeyAspectName())
                : entitySpec.getAspectSpecs().stream().map(AspectSpec::getName).collect(Collectors.toSet());

        entityService.getEntitiesV2(entityType, urns, aspectSpecNames).forEach((urn, response) -> {
          result.putIfAbsent(urn, new HashMap<>());
          try {
            result.get(urn).put(query, buildSystemQueryResponse(query, urn, response));
          } catch (RuntimeException e) {
            log.error("RuntimeException for urn: {} for query {}. Skipping running test for urn", urn, query, e);
          }
        });
      } catch (URISyntaxException e) {
        log.error("Error while fetching aspects for urns {}", urns, e);
        throw new RuntimeException(String.format("Error while fetching aspects for urns %s", urns));
      }
    }
    return result;
  }

  private TestQueryResponse buildSystemQueryResponse(@Nonnull final TestQuery query, @Nonnull final Urn urn,
      @Nonnull final EntityResponse entityResponse) {
    final String queryName = query.getQuery();
    switch (queryName) {
      case FIRST_SYNCHRONIZED_FIELD_NAME:
        final String keyAspectCreatedTime = computeFirstSynchronized(urn, entityResponse);
        return new TestQueryResponse(Collections.singletonList(keyAspectCreatedTime));
      case LAST_SYNCHRONIZED_FIELD_NAME:
        final Long lastIngested = SystemMetadataUtils.getLastIngested(entityResponse.getAspects());
        if (lastIngested == null) {
          throw new RuntimeException(String.format("last ingested time is null %s", urn));
        }
        final String lastSynchronizedTime = lastIngested.toString();
        return new TestQueryResponse(Collections.singletonList(lastSynchronizedTime));
      case LAST_UPDATED_FIELD_NAME:
        final String lastUpdatedTime = computeLastUpdated(urn, entityResponse);
        return new TestQueryResponse(Collections.singletonList(lastUpdatedTime));
      default:
        throw new RuntimeException(String.format("Unknown query %s", queryName));
    }
  }

  private static String computeLastUpdated(Urn urn, EntityResponse entityResponse) {
    return entityResponse.getAspects()
        .values()
        .stream()
        .map(aspect -> aspect.getCreated().getTime())
        .max(Long::compareTo)
        .orElseThrow(() -> {
          log.error("Unable to compute max aspect createdAt for urn: {}", urn);
          return new RuntimeException(String.format("Unable to compute max aspect createdAt for urn %s", urn));
        })
        .toString();
  }

  private String computeFirstSynchronized(Urn urn, EntityResponse entityResponse) {
    final String keyAspectName =
        entityService.getEntityRegistry().getEntitySpec(urn.getEntityType()).getKeyAspectName();
    final EnvelopedAspect keyAspect = entityResponse.getAspects().get(keyAspectName);
    if (keyAspect == null) {
      log.error("Unable to retrieve key aspect for urn: {}. Maybe this entity was recently deleted?", urn);
      throw new RuntimeException(String.format("Unable to retrieve key aspect for urn: %s", urn));
    }
    return keyAspect.getCreated().getTime().toString();
  }
}

