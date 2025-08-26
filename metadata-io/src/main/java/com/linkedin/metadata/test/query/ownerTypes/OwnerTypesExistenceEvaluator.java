package com.linkedin.metadata.test.query.ownerTypes;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.query.BaseQueryEvaluator;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluator that supports resolving 'ownerTypeExists' queries by stitching together the underlying
 * ownership types and the ownerTypes array. This is intended as an expedient workaround on the SaaS
 * side due to the lack of backfill job for this field on OSS to avoid an OSS release cycle and
 * deployment. Generally the standard ownerTypes query supported by the UI should be sufficient.
 * Meant to be combined with an EXISTS evaluator.
 */
@Slf4j
@RequiredArgsConstructor
public class OwnerTypesExistenceEvaluator extends BaseQueryEvaluator {

  private final EntityService<?> entityService;

  private static final Set<String> ASPECT_NAMES = ImmutableSet.of(OWNERSHIP_ASPECT_NAME);

  private static final String OWNER_TYPE_EXISTS_QUERY = "ownerTypesList";

  @Override
  public boolean isEligible(@Nonnull final String entityType, @Nonnull final TestQuery query) {
    return isOwnerTypesExistenceQuery(query);
  }

  private boolean isOwnerTypesExistenceQuery(@Nonnull TestQuery query) {
    return !query.getQueryParts().isEmpty() && OWNER_TYPE_EXISTS_QUERY.equals(query.getQuery());
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
        entityService
            .getEntitiesV2(opContext, entityType, urns, ASPECT_NAMES)
            .forEach(
                (urn, response) -> {
                  result.putIfAbsent(urn, new HashMap<>());
                  try {
                    result.get(urn).put(query, buildQueryResponse(extractOwnership(response)));
                  } catch (RuntimeException e) {
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

  @Nullable
  private Ownership extractOwnership(@Nullable final EntityResponse entityResponse) {
    if (entityResponse != null && entityResponse.getAspects().containsKey(OWNERSHIP_ASPECT_NAME)) {
      return new Ownership(
          entityResponse.getAspects().get(OWNERSHIP_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  @Nonnull
  private TestQueryResponse buildQueryResponse(@Nullable final Ownership ownership) {
    if (ownership == null) {
      return TestQueryResponse.empty();
    }

    final Set<String> results = new HashSet<>();
    if (ownership.getOwnerTypes() != null) {
      results.addAll(ownership.getOwnerTypes().keySet());
    }
    results.addAll(getNestedTypes(ownership));

    return new TestQueryResponse(new ArrayList<>(results));
  }

  @Nonnull
  @SuppressWarnings({"deprecation"})
  private Set<String> getNestedTypes(@Nonnull Ownership ownership) {
    Set<String> results = new HashSet<>();
    for (Owner owner : ownership.getOwners()) {
      results.add(
          Optional.ofNullable(owner.getTypeUrn())
              .map(Urn::toString)
              .orElseGet(
                  () ->
                      "urn:li:ownershipType:__system__"
                          + owner.getType().toString().toLowerCase()));
    }
    return results;
  }
}
