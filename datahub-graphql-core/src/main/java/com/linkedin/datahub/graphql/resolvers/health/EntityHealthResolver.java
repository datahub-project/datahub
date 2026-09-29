package com.linkedin.datahub.graphql.resolvers.health;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.generated.IncidentState;
import com.linkedin.datahub.graphql.resolvers.load.EntityHealthBatchLoader;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.timeseries.GenericTable;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.DataLoader;

/**
 * Resolver for generating the health badge for an asset, which depends on
 *
 * <p>1. Assertions status - whether the asset has active assertions 2. Incidents status - whether
 * the asset has active incidents
 *
 * <p>All health interpretation (aggregation specs, filters, row parsing, {@link Health} assembly)
 * lives in {@link HealthComputationUtils}, shared with the batched {@link EntityHealthBatchLoader}
 * so both paths compute identical health.
 */
@Slf4j
public class EntityHealthResolver implements DataFetcher<CompletableFuture<List<Health>>> {

  private final EntityClient _entityClient;
  private final GraphClient _graphClient;
  private final TimeseriesAspectService _timeseriesAspectService;

  private final Config _config;

  // Null when constructed without feature flags (legacy/test path) — treated as "batch disabled".
  @Nullable private final FeatureFlags _featureFlags;

  public EntityHealthResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final TimeseriesAspectService timeseriesAspectService) {
    this(entityClient, graphClient, timeseriesAspectService, new Config(true, true, true));
  }

  public EntityHealthResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final TimeseriesAspectService timeseriesAspectService,
      @Nonnull final Config config) {
    this(entityClient, graphClient, timeseriesAspectService, config, null);
  }

  public EntityHealthResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final TimeseriesAspectService timeseriesAspectService,
      @Nonnull final Config config,
      @Nullable final FeatureFlags featureFlags) {
    _entityClient = entityClient;
    _graphClient = graphClient;
    _timeseriesAspectService = timeseriesAspectService;
    _config = config;
    _featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<List<Health>> get(final DataFetchingEnvironment environment)
      throws Exception {
    final Entity parent = environment.getSource();

    if (_featureFlags != null && _featureFlags.isEntityHealthBatchLoadEnabled()) {
      final EntityHealthBatchLoader.HealthQueryKey key =
          new EntityHealthBatchLoader.HealthQueryKey(
              UrnUtils.getUrn(parent.getUrn()),
              _config.getAssertionsEnabled(),
              _config.getIncidentsEnabled(),
              _config.getTestsEnabled());
      final DataLoader<EntityHealthBatchLoader.HealthQueryKey, List<Health>> loader =
          environment.getDataLoaderRegistry().getDataLoader(EntityHealthBatchLoader.LOADER_NAME);
      return loader.load(key);
    }

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

    if (_config.getTestsEnabled()) {
      final Health testsHealth = computeTestsHealthForAsset(entityUrn, context);
      if (testsHealth != null) {
        healthStatuses.add(testsHealth);
      }
    }

    return new HealthStatuses(healthStatuses);
  }

  private Health computeIncidentsHealthForAsset(
      final String entityUrn, final QueryContext context) {
    try {
      final Filter filter =
          HealthComputationUtils.incidentsEntityFilter(entityUrn, IncidentState.ACTIVE.toString());
      final SearchResult searchResult =
          _entityClient.filter(
              context.getOperationContext(),
              Constants.INCIDENT_ENTITY_NAME,
              filter,
              HealthComputationUtils.incidentsSort(),
              0,
              1);
      final int activeIncidentCount = searchResult.getNumEntities();
      Urn latestIncidentUrn = null;
      IncidentInfo latestIncidentInfo = null;
      if (activeIncidentCount > 0) {
        latestIncidentUrn = searchResult.getEntities().get(0).getEntity();
        latestIncidentInfo = getIncidentInfo(context, latestIncidentUrn);
      }
      return HealthComputationUtils.buildIncidentHealth(
          activeIncidentCount, latestIncidentUrn, latestIncidentInfo);
    } catch (RemoteInvocationException e) {
      log.error("Failed to compute incident health status!", e);
      return null;
    } catch (URISyntaxException e) {
      log.error("Failed to parse incident URN!", e);
      return null;
    }
  }

  @Nullable
  private IncidentInfo getIncidentInfo(final QueryContext context, final Urn incidentUrn)
      throws URISyntaxException, RemoteInvocationException {
    final EntityResponse entityResponse =
        _entityClient.getV2(
            context.getOperationContext(),
            Constants.INCIDENT_ENTITY_NAME,
            incidentUrn,
            Set.of(Constants.INCIDENT_INFO_ASPECT_NAME),
            false);

    if (entityResponse == null) {
      return null;
    }
    if (!entityResponse.getAspects().containsKey(Constants.INCIDENT_INFO_ASPECT_NAME)) {
      return null;
    }
    return new IncidentInfo(
        entityResponse.getAspects().get(Constants.INCIDENT_INFO_ASPECT_NAME).getValue().data());
  }

  /**
   * TODO: Replace this with the assertions summary aspect.
   *
   * <p>Returns the resolved "assertions health", which is currently a static function of whether
   * the most recent run of all asset assertions has succeeded.
   *
   * @param entityUrn the entity to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the asset, null if one cannot be computed.
   */
  @Nullable
  private Health computeAssertionHealthForAsset(
      final String entityUrn, final QueryContext context) {
    // Get active assertion urns
    final EntityRelationships relationships =
        _graphClient.getRelatedEntities(
            entityUrn,
            ImmutableSet.of(HealthComputationUtils.ASSERTS_RELATIONSHIP_NAME),
            RelationshipDirection.INCOMING,
            0,
            HealthComputationUtils.MAX_ACTIVE_ASSERTIONS,
            context.getActorUrn());

    if (relationships.getTotal() > 0) {
      // If there are assertions defined, then we should return a non-null health for this asset.
      final Set<String> activeAssertionUrns =
          relationships.getRelationships().stream()
              .map(relationship -> relationship.getEntity().toString())
              .collect(Collectors.toSet());

      final GenericTable assertionRunResults =
          getAssertionRunsTable(context.getOperationContext(), entityUrn);

      return HealthComputationUtils.buildAssertionsHealth(assertionRunResults, activeAssertionUrns);
    }
    return null;
  }

  private GenericTable getAssertionRunsTable(
      @Nonnull OperationContext opContext, final String asserteeUrn) {
    return _timeseriesAspectService.getAggregatedStats(
        opContext,
        Constants.ASSERTION_ENTITY_NAME,
        Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
        HealthComputationUtils.assertionAggregationSpecs(),
        HealthComputationUtils.assertionsFilter(asserteeUrn),
        HealthComputationUtils.assertionGroupingBuckets());
  }

  /**
   * Returns the resolved "tests health", which is a static function of whether there are any
   * failing governance tests on the asset.
   *
   * @param entityUrn the asset to compute health for
   * @param context the query context
   * @return an instance of {@link Health} for the entity, null if one cannot be computed.
   */
  @Nullable
  private Health computeTestsHealthForAsset(final String entityUrn, final QueryContext context) {
    try {
      final Urn urn = Urn.createFromString(entityUrn);
      final EntityResponse entityResponse =
          _entityClient.getV2(
              context.getOperationContext(),
              urn.getEntityType(),
              urn,
              ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME));

      return HealthComputationUtils.buildTestsHealth(
          HealthComputationUtils.extractTestResults(entityResponse));
    } catch (RemoteInvocationException e) {
      log.error("Failed to compute test health status!", e);
      return null;
    } catch (URISyntaxException e) {
      log.error("Failed to parse incident URN!", e);
      return null;
    }
  }

  @Data
  @AllArgsConstructor
  public static class Config {
    private Boolean assertionsEnabled;
    private Boolean incidentsEnabled;
    private Boolean testsEnabled;
  }

  @AllArgsConstructor
  private static class HealthStatuses {
    private final List<Health> healths;
  }
}
