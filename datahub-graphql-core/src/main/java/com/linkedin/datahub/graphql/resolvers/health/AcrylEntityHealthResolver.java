package com.linkedin.datahub.graphql.resolvers.health;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.HealthStatus;
import com.linkedin.datahub.graphql.generated.HealthStatusType;
import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for generating the health badge for an asset, which depends on
 *
 * <p>1. Assertions status - whether the asset has active assertions 2. Incidents status - whether
 * the asset has active incidents
 */
@Slf4j
public class AcrylEntityHealthResolver implements DataFetcher<CompletableFuture<List<Health>>> {
  private static final String INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME = "entities.keyword";
  private static final String INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME = "state";

  private final EntityClient _entityClient;
  private final Config _config;

  public AcrylEntityHealthResolver(@Nonnull final EntityClient entityClient) {
    this(entityClient, new Config(true, true));
  }

  public AcrylEntityHealthResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final Config config) {
    _entityClient = entityClient;
    _config = config;
  }

  @Override
  public CompletableFuture<List<Health>> get(final DataFetchingEnvironment environment)
      throws Exception {
    final Entity parent = environment.getSource();
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final HealthStatuses statuses =
                computeHealthStatusForAsset(parent.getUrn(), environment.getContext());
            return statuses.healths;
          } catch (Exception e) {
            throw new RuntimeException("Failed to resolve asset's health status.", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Computes the "resolved health status" for an asset by
   *
   * <p>- fetching active (non-deleted) assertions - fetching latest assertion run for each -
   * checking whether any of the assertions latest runs are failing
   */
  private HealthStatuses computeHealthStatusForAsset(
      final String entityUrn, final QueryContext context) {
    final List<Health> healthStatuses = new ArrayList<>();

    if (_config.getIncidentsEnabled()) {
      final Health incidentsHealth = computeIncidentsHealthForAsset(entityUrn, context);
      if (incidentsHealth != null) {
        healthStatuses.add(incidentsHealth);
      }
    }

    if (_config.getAssertionsEnabled()) {
      final Health assertionsHealth = computeAssertionHealthForAsset(entityUrn, context);
      if (assertionsHealth != null) {
        healthStatuses.add(assertionsHealth);
      }
    }

    return new HealthStatuses(healthStatuses);
  }

  /**
   * Returns the resolved "incidents health", which is currently a static function of whether there
   * are any active incidents open on an asset
   *
   * @param entityUrn the asset to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the entity, null if one cannot be computed.
   */
  private Health computeIncidentsHealthForAsset(
      final String entityUrn, final QueryContext context) {
    try {
      final Filter filter = buildIncidentsEntityFilter(entityUrn, IncidentState.ACTIVE.toString());
      final SearchResult searchResult =
          _entityClient.filter(
              context.getOperationContext(), Constants.INCIDENT_ENTITY_NAME, filter, null, 0, 1);
      final Integer activeIncidentCount = searchResult.getNumEntities();
      if (activeIncidentCount > 0) {
        // There are active incidents.
        return new Health(
            HealthStatusType.INCIDENTS,
            HealthStatus.FAIL,
            String.format(
                "%s active incident%s", activeIncidentCount, activeIncidentCount > 1 ? "s" : ""),
            ImmutableList.of("ACTIVE_INCIDENTS"));
      }
      // Report pass if there are no active incidents.
      return new Health(HealthStatusType.INCIDENTS, HealthStatus.PASS, null, null);
    } catch (RemoteInvocationException e) {
      log.error("Failed to compute incident health status!", e);
      return null;
    }
  }

  private Filter buildIncidentsEntityFilter(final String entityUrn, final String state) {
    final Map<String, String> criterionMap = new HashMap<>();
    criterionMap.put(INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME, entityUrn);
    criterionMap.put(INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME, state);
    return QueryUtils.newFilter(criterionMap);
  }

  /**
   * Returns the resolved "assertions health", which is currently a static function of whether the
   * most recent run of all asset assertions has succeeded, as record by the AssertionsSummary
   * aspect.
   *
   * @param entityUrn the entity to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the asset, null if one cannot be computed.
   */
  @Nullable
  private Health computeAssertionHealthForAsset(
      final String entityUrn, final QueryContext context) {

    // SaaS only! In Acryl-DataHub we use the AssertionsSummary aspect to determine whether the
    // entity is passing or failing
    // it's assertions. This is more efficient, as we can skip expensive calls to elasticsearch!
    // Once OSS has the AssertionsSummaryHook, we can port this to OSS as well.
    final AssertionsSummary summary = getAssertionsSummary(entityUrn, context);

    if (summary != null && !isEmptyAssertionsSummary(summary)) {

      final int failingAssertionCount =
          summary.hasFailingAssertionDetails() ? summary.getFailingAssertionDetails().size() : 0;
      final int passingAssertionCount =
          summary.hasPassingAssertionDetails() ? summary.getPassingAssertionDetails().size() : 0;
      final int totalAssertionCount = failingAssertionCount + passingAssertionCount;

      final List<String> failingAssertionUrns =
          summary.hasFailingAssertionDetails()
              ? summary.getFailingAssertionDetails().stream()
                  .map(details -> details.getUrn().toString())
                  .collect(Collectors.toList())
              : Collections.emptyList();

      // Finally compute & return the health.
      final Health health = new Health();
      health.setType(HealthStatusType.ASSERTIONS);
      if (failingAssertionUrns.size() > 0) {
        health.setStatus(HealthStatus.FAIL);
        health.setMessage(
            String.format(
                "%s of %s assertions are failing",
                failingAssertionUrns.size(), totalAssertionCount));
        health.setCauses(failingAssertionUrns);
      } else {
        health.setStatus(HealthStatus.PASS);
        health.setMessage("All assertions are passing");
      }
      return health;
    }
    // No assertions passing or failing. Simply return null.
    return null;
  }

  @Nullable
  private AssertionsSummary getAssertionsSummary(
      @Nonnull final String urnStr, @Nonnull final QueryContext context) {
    try {
      final Urn urn = UrnUtils.getUrn(urnStr);
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              urn.getEntityType(),
              urn,
              ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME));

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)) {
        return new AssertionsSummary(
            entityResponse
                .getAspects()
                .get(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)
                .getValue()
                .data());
      }
      return null;
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to retrieve assertions summary for urn %s! Returning null assertions summary.",
              urnStr),
          e);
      return null;
    }
  }

  private boolean isEmptyAssertionsSummary(@Nonnull final AssertionsSummary summary) {
    return (!summary.hasPassingAssertionDetails() || summary.getPassingAssertionDetails().isEmpty())
        && (!summary.hasFailingAssertionDetails()
            || summary.getFailingAssertionDetails().isEmpty());
  }

  @Data
  @AllArgsConstructor
  public static class Config {
    private Boolean assertionsEnabled;
    private Boolean incidentsEnabled;
  }

  @AllArgsConstructor
  private static class HealthStatuses {
    private final List<Health> healths;
  }
}
