package com.linkedin.datahub.graphql.util;

import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import java.util.Collection;
import java.util.List;
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

  private AspectUtils() {}

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

    AspectLoadContext loadContext = context.getAspectLoadContext(entityTypeName);
    if (loadContext == null) {
      log.debug("AspectLoadContext not available for {}, fetching all aspects", entityTypeName);
      return defaultAspects;
    }

    Set<String> optimizedAspects = loadContext.resolve(defaultAspects, alwaysIncludeAspects);
    log.debug("Fetching optimized aspect set for {}: {}", entityTypeName, optimizedAspects);
    return optimizedAspects;
  }
}
