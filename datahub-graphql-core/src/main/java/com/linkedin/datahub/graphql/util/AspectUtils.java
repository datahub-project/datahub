package com.linkedin.datahub.graphql.util;

import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility methods for optimizing aspect fetching in GraphQL entity types by determining which
 * aspects need to be fetched based on requested fields.
 */
@Slf4j
public class AspectUtils {

  /**
   * Aspects that must be fetched for an entity type regardless of which fields were selected,
   * because hydrating the entity correctly depends on them. Optimized fetching can produce an empty
   * required set for a selection made up entirely of {@code @noAspects} fields (e.g. {@code
   * createDataHubFile { file { urn } }}); without these, hydration silently produces a wrong
   * result. Keyed by GraphQL type name; {@link #getOptimizedAspects} always folds them into the
   * optimized set.
   *
   * <p>Two kinds of dependency qualify, both of which are selection-independent:
   *
   * <ul>
   *   <li><b>Structural</b> — the mapper returns null or throws when the aspect is absent, so the
   *       entity fails to hydrate at all (and a non-nullable GraphQL field then fails the whole
   *       operation).
   *   <li><b>Authorization</b> — the mapper feeds the aspect into an access decision, so a missing
   *       aspect can change whether the entity is redacted. Note this does not require the mapper
   *       to null or throw.
   * </ul>
   *
   * <p>This is the single source of truth — add an entry here, rather than per-loader
   * always-include args, whenever a loader gains either kind of dependency.
   */
  private static final Map<String, Set<String>> HYDRATION_REQUIRED_ASPECTS =
      Map.of(
          // Structural: mapper returns null or throws without these.
          "DataHubFile", Set.of(Constants.DATAHUB_FILE_INFO_ASPECT_NAME),
          "DataHubPageTemplate", Set.of(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME),
          "Test", Set.of(Constants.TEST_INFO_ASPECT_NAME),
          "Incident", Set.of(Constants.INCIDENT_INFO_ASPECT_NAME),
          "DataContract", Set.of(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME),
          "DataHubConnection",
              Set.of(
                  Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                  Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME),
          // Authorization: DocumentMapper passes these to canViewDocument, which wrongly redacts a
          // bridge document when they are absent.
          "Document", Set.of(Constants.DOCUMENT_INFO_ASPECT_NAME, Constants.SUB_TYPES_ASPECT_NAME));

  private AspectUtils() {}

  /**
   * Aspects that must always be fetched for {@code entityTypeName} to hydrate correctly (structural
   * or authorization dependencies), or an empty set if none.
   */
  @Nonnull
  public static Set<String> getHydrationRequiredAspects(@Nonnull final String entityTypeName) {
    return HYDRATION_REQUIRED_ASPECTS.getOrDefault(entityTypeName, Set.of());
  }

  /**
   * GraphQL type names with registered hydration-required aspects. Exists so the registration guard
   * test can enforce an exact contract: an entry added here without a stated expectation in the
   * test fails, not just a dropped or drifted one.
   */
  @Nonnull
  public static Set<String> getHydrationRequiredTypes() {
    return HYDRATION_REQUIRED_ASPECTS.keySet();
  }

  /**
   * Computes the aspect selection for a single GraphQL field invocation from its selection set.
   * Resolvers must merge the result into {@link QueryContext} before enqueueing a DataLoader load:
   * DataLoader may suppress duplicate key contexts when the cache key (URN + {@link
   * AspectLoadContext} signature) already has a pending future, so enqueue-time merge is what keeps
   * aliased sibling selections in the request-scoped union.
   */
  @Nonnull
  public static AspectLoadContext computeLoadContext(
      @Nullable final AspectMappingRegistry registry,
      @Nonnull final String entityTypeName,
      @Nullable final Collection<SelectedField> selectedFields) {
    if (registry == null || selectedFields == null) {
      return AspectLoadContext.fetchAll();
    }
    return AspectLoadContext.fromRequiredAspects(
        registry.getRequiredAspects(entityTypeName, selectedFields));
  }

  /**
   * Computes the aspect selection for a single GraphQL field invocation from the query AST.
   *
   * <p>Prefer this over the {@link SelectedField} overload in entity resolvers: it reads only the
   * immediate selections via {@link SelectionSetAnalyzer} instead of materializing the entire
   * selection subtree on every resolved entity.
   */
  @Nonnull
  public static AspectLoadContext computeLoadContext(
      @Nullable final AspectMappingRegistry registry,
      @Nonnull final String entityTypeName,
      @Nullable final DataFetchingEnvironment environment) {
    if (registry == null || environment == null) {
      return AspectLoadContext.fetchAll();
    }
    return AspectLoadContext.fromRequiredAspects(
        registry.getRequiredAspectsForFieldNames(
            entityTypeName,
            SelectionSetAnalyzer.collectImmediateFieldNames(environment, entityTypeName)));
  }

  /**
   * Unions key-context {@link AspectLoadContext} values from a DataLoader batch. An empty
   * contribution list returns null.
   *
   * <p>A batch entry WITHOUT an {@link AspectLoadContext} means some load path did not state its
   * selection, so its needs are unknown — the union degrades to {@link
   * AspectLoadContext#fetchAll()} rather than silently underserving that load with whatever the
   * context-carrying entries happened to need.
   */
  @Nullable
  public static AspectLoadContext unionKeyContexts(@Nullable final List<Object> keyContexts) {
    if (keyContexts == null || keyContexts.isEmpty()) {
      return null;
    }
    AspectLoadContext union = null;
    for (Object keyContext : keyContexts) {
      if (!(keyContext instanceof AspectLoadContext)) {
        return AspectLoadContext.fetchAll();
      }
      AspectLoadContext loadContext = (AspectLoadContext) keyContext;
      union = union == null ? loadContext : union.union(loadContext);
    }
    return union;
  }

  /**
   * Widens the request-scoped aspect selection to {@link AspectLoadContext#fetchAll()} before a
   * direct {@code batchLoad}/{@code load} that bypasses DataLoader resolvers.
   *
   * <p>Direct callers do not contribute a selection-set {@link AspectLoadContext}. If the request
   * already accumulated a narrow union for {@code entityTypeName} (from another field), {@link
   * #getOptimizedAspects} would under-fetch. Merging fetch-all first keeps those paths correct.
   */
  public static void ensureFetchAllForDirectLoad(
      @Nonnull final QueryContext context, @Nonnull final String entityTypeName) {
    context.mergeAspectLoadContext(entityTypeName, AspectLoadContext.fetchAll());
  }

  /**
   * Determines optimal aspects to fetch based on the request-scoped aspect load context for {@code
   * entityTypeName}. Falls back to {@code defaultAspects} when no selection was accumulated (e.g.
   * direct {@code batchLoad} outside DataLoader, missing registry contributions).
   *
   * <p>Usage in entity type batchLoad: Set&lt;String&gt; aspects =
   * AspectUtils.getOptimizedAspects(context, name(), ALL_ASPECTS, DATASET_KEY_ASPECT_NAME);
   *
   * @param context the QueryContext carrying the per-entity-type {@link AspectLoadContext} union
   * @param entityTypeName the GraphQL type name (e.g., "Dataset", "CorpUser")
   * @param defaultAspects the full set of aspects to use as fallback
   * @param alwaysIncludeAspects aspects to always include (e.g., key aspects)
   * @return optimized aspect set, or defaultAspects if optimization isn't possible. The returned
   *     set may be immutable; callers must not mutate it.
   */
  @Nonnull
  public static Set<String> getOptimizedAspects(
      @Nonnull final QueryContext context,
      @Nonnull final String entityTypeName,
      @Nonnull final Set<String> defaultAspects,
      @Nonnull final String... alwaysIncludeAspects) {

    if (!isAspectOptimizationEnabled(context)) {
      log.debug("Aspect optimization disabled, fetching all aspects for {}", entityTypeName);
      recordFetchOutcome(context, entityTypeName, "disabled");
      return defaultAspects;
    }

    AspectLoadContext loadContext = context.getAspectLoadContext(entityTypeName);
    if (loadContext == null) {
      log.debug("AspectLoadContext not available for {}, fetching all aspects", entityTypeName);
      recordFetchOutcome(context, entityTypeName, "fallback");
      return defaultAspects;
    }

    Set<String> optimizedAspects = loadContext.resolve(defaultAspects, alwaysIncludeAspects);
    // On the fetch-all path resolve() returns defaultAspects, which already covers these.
    Set<String> hydrationRequired = HYDRATION_REQUIRED_ASPECTS.get(entityTypeName);
    if (!loadContext.isFetchAll() && hydrationRequired != null) {
      // resolve() documents that its result may be immutable, so copy rather than depend on the
      // current non-fetch-all path happening to return a mutable set.
      optimizedAspects = new HashSet<>(optimizedAspects);
      optimizedAspects.addAll(hydrationRequired);
    }

    if (optimizedAspects.isEmpty()) {
      // A selection made up entirely of @noAspects fields resolves to nothing when the loader also
      // passes no key aspect. batchGetV2 returns no row for a zero-aspect request, and loaders map
      // a missing row to a null entity, so the query silently loses the entity instead of merely
      // fetching less. Fall back to the defaults rather than issue an empty fetch.
      log.debug("Optimized aspect set for {} was empty, falling back to defaults", entityTypeName);
      recordFetchOutcome(context, entityTypeName, "fallback");
      return defaultAspects;
    }

    recordFetchOutcome(
        context, entityTypeName, loadContext.isFetchAll() ? "fetch_all" : "optimized");
    log.debug("Fetching optimized aspect set for {}: {}", entityTypeName, optimizedAspects);
    return optimizedAspects;
  }

  /**
   * Per-entity-type fetch-outcome counters ({@code aspect_fetch_<outcome>_<type>}) so operators can
   * see where optimization is active ({@code optimized}) versus falling back ({@code fetch_all},
   * {@code fallback}, {@code disabled}), and attribute hydration incidents to it quickly.
   * Best-effort: metrics must never affect hydration.
   */
  private static void recordFetchOutcome(
      @Nonnull final QueryContext context,
      @Nonnull final String entityTypeName,
      @Nonnull final String outcome) {
    try {
      if (context.getOperationContext() == null
          || context.getOperationContext().getMetricUtils() == null) {
        return;
      }
      context
          .getOperationContext()
          .getMetricUtils()
          .ifPresent(
              metricUtils ->
                  metricUtils.increment(
                      AspectUtils.class,
                      String.format("aspect_fetch_%s_%s", outcome, entityTypeName),
                      1));
    } catch (RuntimeException e) {
      log.debug("Failed to record aspect fetch outcome metric", e);
    }
  }

  /**
   * Honors the {@code graphQLAspectOptimizationEnabled} feature flag. Absent config (e.g. unit
   * tests with a mocked context) keeps optimization enabled, matching the default-on flag.
   */
  private static boolean isAspectOptimizationEnabled(@Nonnull final QueryContext context) {
    DataHubAppConfiguration appConfig = context.getDataHubAppConfig();
    if (appConfig == null || appConfig.getFeatureFlags() == null) {
      return true;
    }
    return appConfig.getFeatureFlags().isGraphQLAspectOptimizationEnabled();
  }
}
