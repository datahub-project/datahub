package com.linkedin.datahub.graphql.resolvers.load;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Health;
import com.linkedin.datahub.graphql.resolvers.health.HealthComputationUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.IncidentStats;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.test.TestResults;
import com.linkedin.timeseries.GenericTable;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * DataLoader that batches {@code health} resolution across every entity in a single GraphQL
 * request, replacing the per-entity fan-out in {@link
 * com.linkedin.datahub.graphql.resolvers.health.EntityHealthResolver}. All health interpretation is
 * shared with the per-entity resolver via {@link HealthComputationUtils}.
 *
 * <p>The heavy, N+1-prone dimensions are collapsed to one call per request:
 *
 * <ul>
 *   <li><b>Assertion runs</b> — a single {@code batchGetAggregatedStats} over the assertion-run
 *       timeseries index, keyed on {@code asserteeUrn} (one outer-terms-bucket ES query per
 *       sub-batch instead of one aggregation per entity).
 *   <li><b>Governance tests</b> — a single {@code batchGetV2} per entity type for the {@code
 *       testResults} aspect.
 *   <li><b>Active incidents</b> — a single {@link EntitySearchService#getActiveIncidentStats}
 *       aggregation for counts/latest-incident-urn, plus a single {@code batchGetV2} for the latest
 *       incidents' info.
 * </ul>
 *
 * <p>One dimension has no batch primitive today and is still computed once per entity, but
 * concurrently within this loader: active-assertion graph lookups ({@link
 * GraphClient#getRelatedEntities}).
 *
 * <p><b>Failure isolation:</b> a failure in one dimension degrades to an empty result for that
 * dimension (logged) rather than failing the whole page — both in the concurrent FETCH phase (each
 * future has {@code .exceptionally}) and in the synchronous ASSEMBLE phase (each per-key dimension
 * is wrapped). So the other dimensions, and every other entity, still return the health they could
 * compute.
 *
 * <p>The batch key carries the per-type health config (mirroring {@code
 * EntityHealthResolver.Config}) so entity types that disable a dimension coalesce into their own
 * batch and never trigger the assertion/test calls.
 */
@Slf4j
@RequiredArgsConstructor
public class EntityHealthBatchLoader {

  public static final String LOADER_NAME = "EntityHealth";

  private final EntityClient entityClient;
  private final GraphClient graphClient;
  private final TimeseriesAspectService timeseriesAspectService;
  private final EntitySearchService entitySearchService;

  /** Composite DataLoader key: which entity, and which health dimensions to compute for it. */
  @Value
  public static class HealthQueryKey {
    Urn urn;
    boolean assertionsEnabled;
    boolean incidentsEnabled;
    boolean testsEnabled;
  }

  public static DataLoader<HealthQueryKey, List<Health>> createDataLoader(
      final EntityClient entityClient,
      final GraphClient graphClient,
      final TimeseriesAspectService timeseriesAspectService,
      final EntitySearchService entitySearchService,
      final QueryContext queryContext) {
    final EntityHealthBatchLoader loader =
        new EntityHealthBatchLoader(
            entityClient, graphClient, timeseriesAspectService, entitySearchService);
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);
    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> loader.batchLoad(keys, (QueryContext) env.getContext()),
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  @WithSpan
  public List<List<Health>> batchLoad(final List<HealthQueryKey> keys, final QueryContext context) {
    final OperationContext opContext = context.getOperationContext();

    final List<Urn> assertionUrns = distinctUrns(keys, HealthQueryKey::isAssertionsEnabled);
    final List<Urn> incidentUrns = distinctUrns(keys, HealthQueryKey::isIncidentsEnabled);

    // Fire the independent batches concurrently. Each dimension is isolated: a failure degrades to
    // an empty result (logged) rather than failing the whole page.
    // A: batched assertion-run aggregation (asserteeUrn outer bucket) — the headline N+1 fix.
    final CompletableFuture<Map<Urn, GenericTable>> assertionRunsFuture =
        GraphQLConcurrencyUtils.supplyAsync(
                () -> batchAssertionRuns(opContext, assertionUrns),
                LOADER_NAME,
                "batchGetAggregatedStats:assertionRuns")
            .exceptionally(
                ex -> {
                  log.error("Assertion-run batch failed; dropping assertions health", ex);
                  return Collections.<Urn, GenericTable>emptyMap();
                });

    // B: active (non-deleted) assertion URNs — per-URN graph lookup, run concurrently.
    final CompletableFuture<Map<Urn, Set<String>>> activeAssertionsFuture =
        GraphQLConcurrencyUtils.supplyAsync(
                () -> fetchActiveAssertions(assertionUrns, context),
                LOADER_NAME,
                "fetchActiveAssertions")
            .exceptionally(
                ex -> {
                  log.error("Active-assertion lookup failed; dropping assertions health", ex);
                  return Collections.<Urn, Set<String>>emptyMap();
                });

    // C: batched test-results aspect per entity type.
    final CompletableFuture<Map<Urn, TestResults>> testResultsFuture =
        GraphQLConcurrencyUtils.supplyAsync(
                () -> batchTestResults(keys, opContext), LOADER_NAME, "batchGetV2:testResults")
            .exceptionally(
                ex -> {
                  log.error("Test-results batch failed; dropping tests health", ex);
                  return Collections.<Urn, TestResults>emptyMap();
                });

    // D: active-incident health — one getActiveIncidentStats aggregation + one batchGetV2 for info.
    final CompletableFuture<Map<Urn, Health>> incidentHealthFuture =
        GraphQLConcurrencyUtils.supplyAsync(
                () -> fetchIncidentHealth(incidentUrns, context),
                LOADER_NAME,
                "fetchIncidentHealth")
            .exceptionally(
                ex -> {
                  log.error("Incident health lookup failed; dropping incidents health", ex);
                  return Collections.<Urn, Health>emptyMap();
                });

    CompletableFuture.allOf(
            assertionRunsFuture, activeAssertionsFuture, testResultsFuture, incidentHealthFuture)
        .join();

    final Map<Urn, GenericTable> assertionRuns = assertionRunsFuture.join();
    final Map<Urn, Set<String>> activeAssertions = activeAssertionsFuture.join();
    final Map<Urn, TestResults> testResults = testResultsFuture.join();
    final Map<Urn, Health> incidentHealth = incidentHealthFuture.join();

    final List<List<Health>> results = new ArrayList<>(keys.size());
    for (HealthQueryKey key : keys) {
      results.add(
          assembleHealth(key, incidentHealth, assertionRuns, activeAssertions, testResults));
    }
    return results;
  }

  /**
   * Builds one entity's health list, isolating each dimension: a throw while assembling one
   * dimension (e.g. a malformed assertion-run row) drops only that dimension for this entity — it
   * does not fail the entity or the page.
   */
  private List<Health> assembleHealth(
      final HealthQueryKey key,
      final Map<Urn, Health> incidentHealth,
      final Map<Urn, GenericTable> assertionRuns,
      final Map<Urn, Set<String>> activeAssertions,
      final Map<Urn, TestResults> testResults) {
    final Urn urn = key.getUrn();
    final List<Health> healths = new ArrayList<>();

    if (key.isIncidentsEnabled()) {
      final Health h = incidentHealth.get(urn);
      if (h != null) {
        healths.add(h);
      }
    }
    if (key.isAssertionsEnabled()) {
      try {
        final Health h =
            HealthComputationUtils.buildAssertionsHealth(
                assertionRuns.get(urn), activeAssertions.getOrDefault(urn, Collections.emptySet()));
        if (h != null) {
          healths.add(h);
        }
      } catch (Exception e) {
        log.warn("Failed to assemble assertion health for {}; dropping dimension", urn, e);
      }
    }
    if (key.isTestsEnabled()) {
      try {
        final Health h = HealthComputationUtils.buildTestsHealth(testResults.get(urn));
        if (h != null) {
          healths.add(h);
        }
      } catch (Exception e) {
        log.warn("Failed to assemble tests health for {}; dropping dimension", urn, e);
      }
    }
    return healths;
  }

  // ---------------------------------------------------------------------------
  // Assertions
  // ---------------------------------------------------------------------------

  private Map<Urn, GenericTable> batchAssertionRuns(
      final OperationContext opContext, final List<Urn> assertionUrns) {
    if (assertionUrns.isEmpty()) {
      return Collections.emptyMap();
    }
    return timeseriesAspectService.batchGetAggregatedStats(
        opContext,
        Constants.ASSERTION_ENTITY_NAME,
        Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
        HealthComputationUtils.assertionAggregationSpecs(),
        assertionUrns,
        HealthComputationUtils.sharedAssertionsFilter(),
        HealthComputationUtils.assertionGroupingBuckets(),
        HealthComputationUtils.ASSERTEE_URN_FIELD);
  }

  private Map<Urn, Set<String>> fetchActiveAssertions(
      final List<Urn> assertionUrns, final QueryContext context) {
    if (assertionUrns.isEmpty()) {
      return Collections.emptyMap();
    }
    final Map<Urn, CompletableFuture<Set<String>>> futures = new LinkedHashMap<>();
    for (Urn urn : assertionUrns) {
      futures.put(
          urn,
          GraphQLConcurrencyUtils.supplyAsync(
                  () -> {
                    final EntityRelationships relationships =
                        graphClient.getRelatedEntities(
                            urn.toString(),
                            ImmutableSet.of(HealthComputationUtils.ASSERTS_RELATIONSHIP_NAME),
                            RelationshipDirection.INCOMING,
                            0,
                            HealthComputationUtils.MAX_ACTIVE_ASSERTIONS,
                            context.getActorUrn());
                    return relationships.getRelationships().stream()
                        .map(relationship -> relationship.getEntity().toString())
                        .collect(Collectors.toSet());
                  },
                  LOADER_NAME,
                  "getRelatedEntities")
              // Isolate per URN: one failed graph lookup drops only that entity's assertion
              // health, not every entity's.
              .exceptionally(
                  ex -> {
                    log.warn("Active-assertion lookup failed for {}; skipping", urn, ex);
                    return Collections.<String>emptySet();
                  }));
    }
    CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).join();
    final Map<Urn, Set<String>> result = new HashMap<>();
    futures.forEach((urn, future) -> result.put(urn, future.join()));
    return result;
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  private Map<Urn, TestResults> batchTestResults(
      final List<HealthQueryKey> keys, final OperationContext opContext) {
    final List<Urn> testUrns = distinctUrns(keys, HealthQueryKey::isTestsEnabled);
    if (testUrns.isEmpty()) {
      return Collections.emptyMap();
    }
    final Map<Urn, TestResults> result = new HashMap<>();
    // batchGetV2 is scoped to a single entity type, so partition by entity type.
    final Map<String, Set<Urn>> urnsByEntityType =
        testUrns.stream().collect(Collectors.groupingBy(Urn::getEntityType, Collectors.toSet()));
    for (Map.Entry<String, Set<Urn>> entry : urnsByEntityType.entrySet()) {
      try {
        final Map<Urn, EntityResponse> responses =
            entityClient.batchGetV2(
                opContext,
                entry.getKey(),
                entry.getValue(),
                ImmutableSet.of(Constants.TEST_RESULTS_ASPECT_NAME));
        for (Map.Entry<Urn, EntityResponse> response : responses.entrySet()) {
          final TestResults testResults =
              HealthComputationUtils.extractTestResults(response.getValue());
          if (testResults != null) {
            result.put(response.getKey(), testResults);
          }
        }
      } catch (Exception e) {
        log.error("Failed to batch-load test results for entity type {}", entry.getKey(), e);
      }
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // Incidents
  // ---------------------------------------------------------------------------

  private Map<Urn, Health> fetchIncidentHealth(
      final List<Urn> incidentUrns, final QueryContext context) {
    if (incidentUrns.isEmpty()) {
      return Collections.emptyMap();
    }
    final OperationContext opContext = context.getOperationContext();

    // Phase 1: one aggregation for count + latest incident urn per entity.
    final Map<Urn, IncidentStats> stats =
        entitySearchService.getActiveIncidentStats(opContext, new HashSet<>(incidentUrns));

    // Phase 2: one batchGetV2 for the latest incidents' info.
    final Set<Urn> latestIncidentUrns =
        stats.values().stream()
            .map(IncidentStats::getLatestIncidentUrn)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    final Map<Urn, IncidentInfo> incidentInfos =
        batchGetIncidentInfo(latestIncidentUrns, opContext);

    // Phase 3: assemble per-entity incident health.
    final Map<Urn, Health> result = new HashMap<>();
    for (Urn urn : incidentUrns) {
      final IncidentStats stat = stats.get(urn);
      final int count = stat != null ? stat.getActiveCount() : 0;
      final Urn latestUrn = stat != null ? stat.getLatestIncidentUrn() : null;
      final IncidentInfo info = latestUrn != null ? incidentInfos.get(latestUrn) : null;
      final Health h = HealthComputationUtils.buildIncidentHealth(count, latestUrn, info);
      if (h != null) {
        result.put(urn, h);
      }
    }
    return result;
  }

  private Map<Urn, IncidentInfo> batchGetIncidentInfo(
      final Set<Urn> incidentUrns, final OperationContext opContext) {
    if (incidentUrns.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      final Map<Urn, EntityResponse> responses =
          entityClient.batchGetV2(
              opContext,
              Constants.INCIDENT_ENTITY_NAME,
              incidentUrns,
              ImmutableSet.of(Constants.INCIDENT_INFO_ASPECT_NAME));
      final Map<Urn, IncidentInfo> result = new HashMap<>();
      responses.forEach(
          (urn, response) -> {
            if (response != null
                && response.getAspects().containsKey(Constants.INCIDENT_INFO_ASPECT_NAME)) {
              result.put(
                  urn,
                  new IncidentInfo(
                      response
                          .getAspects()
                          .get(Constants.INCIDENT_INFO_ASPECT_NAME)
                          .getValue()
                          .data()));
            }
          });
      return result;
    } catch (Exception e) {
      log.error("Failed to batch-fetch incident info", e);
      return Collections.emptyMap();
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static List<Urn> distinctUrns(
      final List<HealthQueryKey> keys, final Predicate<HealthQueryKey> dimensionEnabled) {
    return keys.stream()
        .filter(dimensionEnabled)
        .map(HealthQueryKey::getUrn)
        .distinct()
        .collect(Collectors.toList());
  }
}
