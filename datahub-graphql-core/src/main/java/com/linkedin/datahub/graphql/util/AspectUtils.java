package com.linkedin.datahub.graphql.util;

import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import java.util.Collection;
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
   * Aspects that some entity mappers treat as mandatory: they return null or throw when the aspect
   * is absent, regardless of which fields were selected. Optimized fetching can produce an empty
   * required set for a selection made up entirely of {@code @noAspects} fields (e.g. {@code
   * createDataHubFile { file { urn } }}), which would starve these mappers and fail hydration of a
   * non-nullable field. Keyed by GraphQL type name; {@link #getOptimizedAspects} always folds these
   * into the optimized set so the mapper can run.
   *
   * <p>This is the single source of truth — add an entry here (rather than per-loader
   * always-include args) whenever a mapper hard-requires a non-key aspect.
   */
  private static final Map<String, Set<String>> MAPPER_REQUIRED_ASPECTS =
      Map.of(
          "DataHubFile", Set.of(Constants.DATAHUB_FILE_INFO_ASPECT_NAME),
          "DataHubPageTemplate", Set.of(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME),
          "Test", Set.of(Constants.TEST_INFO_ASPECT_NAME),
          "Incident", Set.of(Constants.INCIDENT_INFO_ASPECT_NAME),
          "DataContract", Set.of(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME),
          "DataHubConnection",
              Set.of(
                  Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME,
                  Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME),
          // DocumentMapper's authorization (canViewDocument) reads documentInfo + subTypes, so a
          // bridge document is wrongly redacted if a selection omits them. Auth runs on every
          // selection, so both must always be fetched.
          "Document", Set.of(Constants.DOCUMENT_INFO_ASPECT_NAME, Constants.SUB_TYPES_ASPECT_NAME));

  private AspectUtils() {}

  /** Aspects a mapper hard-requires for {@code entityTypeName}, or an empty set if none. */
  @Nonnull
  public static Set<String> getMapperRequiredAspects(@Nonnull final String entityTypeName) {
    return MAPPER_REQUIRED_ASPECTS.getOrDefault(entityTypeName, Set.of());
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
   * Unions key-context {@link AspectLoadContext} values from a DataLoader batch. Null or non-{@link
   * AspectLoadContext} entries are ignored; an empty contribution list returns null.
   */
  @Nullable
  public static AspectLoadContext unionKeyContexts(@Nullable final List<Object> keyContexts) {
    if (keyContexts == null || keyContexts.isEmpty()) {
      return null;
    }
    AspectLoadContext union = null;
    for (Object keyContext : keyContexts) {
      if (!(keyContext instanceof AspectLoadContext)) {
        continue;
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
   * AspectUtils.getOptimizedAspects(context, "Dataset", ALL_ASPECTS, "datasetKey");
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
      return defaultAspects;
    }

    AspectLoadContext loadContext = context.getAspectLoadContext(entityTypeName);
    if (loadContext == null) {
      log.debug("AspectLoadContext not available for {}, fetching all aspects", entityTypeName);
      return defaultAspects;
    }

    Set<String> optimizedAspects = loadContext.resolve(defaultAspects, alwaysIncludeAspects);
    if (!loadContext.isFetchAll()) {
      // Fold in aspects the mapper treats as mandatory. resolve() returns a mutable set on the
      // non-fetch-all path; on fetch-all it returns defaultAspects, which already includes them.
      Set<String> mapperRequired = MAPPER_REQUIRED_ASPECTS.get(entityTypeName);
      if (mapperRequired != null) {
        optimizedAspects.addAll(mapperRequired);
      }
    }
    log.debug("Fetching optimized aspect set for {}: {}", entityTypeName, optimizedAspects);
    return optimizedAspects;
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
