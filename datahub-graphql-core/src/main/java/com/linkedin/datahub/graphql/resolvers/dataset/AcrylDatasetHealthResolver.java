package com.linkedin.datahub.graphql.resolvers.dataset;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;


/**
 * Acryl version of the resolver used for resolving the Health state of a Dataset.
 *
 * Currently, the health status is calculated via the validation on a Dataset. If there are no validations found, the
 * health status will be undefined for the Dataset.
 */
@Slf4j
public class AcrylDatasetHealthResolver implements DataFetcher<CompletableFuture<List<Health>>> {

  private static final String INCIDENT_ENTITIES_SEARCH_INDEX_FIELD_NAME = "entities.keyword";
  private static final String INCIDENT_STATE_SEARCH_INDEX_FIELD_NAME = "state";

  private final EntityClient _entityClient;
  private final Config _config;

  private final Cache<String, CachedHealth> _statusCache;

  public AcrylDatasetHealthResolver(@Nonnull final EntityClient entityClient) {
    this(entityClient, new Config(true, true, true));
  }

  public AcrylDatasetHealthResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final Config config) {
    _entityClient = entityClient;
    // TODO: Decide what to do with this cache.
    // Disabling cache so that dataset health updates instantly after changes. (e.g. incidents raised).
    _statusCache = CacheBuilder.newBuilder()
        .maximumSize(0)
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build();
    _config = config;
  }

  @Override
  public CompletableFuture<List<Health>> get(final DataFetchingEnvironment environment) throws Exception {
    final Dataset parent = environment.getSource();
    return CompletableFuture.supplyAsync(() -> {
      try {
        final CachedHealth cachedStatus = _statusCache.get(parent.getUrn(), () -> (
            computeHealthStatusForDataset(parent.getUrn(), environment.getContext())));
        return cachedStatus.healths;
      } catch (Exception e) {
        throw new RuntimeException("Failed to resolve dataset's health status.", e);
      }
    });
  }

  /**
   * Computes the "resolved health status" for a Dataset by
   *
   *  - fetching active (non-deleted) assertions
   *  - fetching latest assertion run for each
   *  - checking whether any of the assertions latest runs are failing
   *
   */
  private CachedHealth computeHealthStatusForDataset(final String datasetUrn, final QueryContext context) {
    final List<Health> healthStatuses = new ArrayList<>();

    if (_config.getAssertionsEnabled()) {
      final Health assertionsHealth = computeAssertionHealthForDataset(datasetUrn, context);
      if (assertionsHealth != null) {
        healthStatuses.add(assertionsHealth);
      }
    }

    // Tests are saas-only.
    // We are hiding this for now because passing/failing all tests no longer has meaning
    // with the new set of default tests where passing may just mean you are not in the top 10% of size
    // if  (_config.getTestsEnabled()) {
    //   final Health testsHealth = computeTestsHealthForDataset(datasetUrn, context);
    //   if (testsHealth != null) {
    //     healthStatuses.add(testsHealth);
    //   }
    // }

    // Incidents are saas-only.
    if (_config.getIncidentsEnabled()) {
      final Health incidentsHealth = computeIncidentsHealthForDataset(datasetUrn, context);
      if (incidentsHealth != null) {
        healthStatuses.add(incidentsHealth);
      }
    }

    return new CachedHealth(healthStatuses);
  }

  /**
   * Returns the resolved "assertions health", which is currently a static function of whether the most recent run of
   * all dataset assertions has succeeded.
   *
   * @param datasetUrn the dataset to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the Dataset, null if one cannot be computed.
   */
  @Nullable
  private Health computeAssertionHealthForDataset(final String datasetUrn, final QueryContext context) {

    // SaaS only! In Acryl-DataHub we use the AssertionsSummary aspect to determine whether the entity is passing or failing
    // it's assertions. This is more efficient, as we can skip expensive calls to elasticsearch!
    // Once OSS has the AssertionsSummaryHook, we can port this to OSS as well.
    final AssertionsSummary summary = getAssertionsSummary(datasetUrn, context);

    if (summary != null && !isEmptyAssertionsSummary(summary)) {

      final int failingAssertionCount = summary.hasFailingAssertionDetails() ? summary.getFailingAssertionDetails().size() : 0;
      final int passingAssertionCount = summary.hasPassingAssertionDetails() ? summary.getPassingAssertionDetails().size() : 0;
      final int totalAssertionCount = failingAssertionCount + passingAssertionCount;

      final List<String> failingAssertionUrns = summary.hasFailingAssertionDetails()
          ? summary.getFailingAssertionDetails().stream()
            .map(details -> details.getUrn().toString())
            .collect(Collectors.toList())
          : Collections.emptyList();

      // Finally compute & return the health.
      final Health health = new Health();
      health.setType(HealthStatusType.ASSERTIONS);
      if (failingAssertionUrns.size() > 0) {
        health.setStatus(HealthStatus.FAIL);
        health.setMessage(String.format("Dataset is failing %s/%s assertions.", failingAssertionUrns.size(),
            totalAssertionCount));
        health.setCauses(failingAssertionUrns);
      } else {
        health.setStatus(HealthStatus.PASS);
        health.setMessage("Dataset is passing all assertions.");
      }
      return health;
    }
    // No assertions passing or failing. Simply return null.
    return null;
  }

  /**
   * Returns the resolved "incidents health", which is currently a static function of whether there are any active
   * incidents open on an asset
   *
   * @param entityUrn the dataset to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the entity, null if one cannot be computed.
   */
  private Health computeIncidentsHealthForDataset(final String entityUrn, final QueryContext context) {
    try {
      final Filter filter = buildIncidentsEntityFilter(entityUrn, IncidentState.ACTIVE.toString());
      final SearchResult searchResult = _entityClient.filter(
          Constants.INCIDENT_ENTITY_NAME,
          filter,
          null,
          0,
          1,
          context.getAuthentication());
      final Integer activeIncidentCount = searchResult.getNumEntities();
      if (activeIncidentCount > 0) {
        // There are active incidents.
        return new Health(
            HealthStatusType.INCIDENTS,
            HealthStatus.FAIL,
            String.format("This asset has %s active Incidents.", activeIncidentCount),
            ImmutableList.of("ACTIVE_INCIDENTS")
        );
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

  @Nullable
  private AssertionsSummary getAssertionsSummary(@Nonnull final String urnStr, @Nonnull final QueryContext context) {
    try {
      final Urn urn = UrnUtils.getUrn(urnStr);
      final EntityResponse entityResponse = _entityClient.getV2(urn.getEntityType(), urn, ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME),
          context.getAuthentication());

      if (entityResponse != null && entityResponse.getAspects().containsKey(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)) {
        return new AssertionsSummary(
            entityResponse.getAspects().get(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME).getValue().data());
      }
      return null;
    } catch (Exception e) {
      log.error(String.format("Failed to retrieve assertions summary for urn %s! Returning null assertions summary.", urnStr), e);
      return null;
    }
  }

  private boolean isEmptyAssertionsSummary(@Nonnull final AssertionsSummary summary) {
    return (!summary.hasPassingAssertionDetails() || summary.getPassingAssertionDetails().isEmpty())
        && (!summary.hasFailingAssertionDetails() || summary.getFailingAssertionDetails().isEmpty());
  }

  @Data
  @AllArgsConstructor
  public static class Config {
    private Boolean assertionsEnabled;
    private Boolean testsEnabled;
    private Boolean incidentsEnabled;
  }

  @AllArgsConstructor
  private static class CachedHealth {
    private final List<Health> healths;
  }
}
