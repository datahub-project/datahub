package com.linkedin.datahub.graphql.util;

import com.linkedin.datahub.graphql.QueryContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility methods for optimizing aspect fetching in GraphQL entity types by determining which
 * aspects need to be fetched based on requested fields.
 */
@Slf4j
public class AspectUtils {

  private AspectUtils() {}

  /**
   * Determines optimal aspects to fetch based on GraphQL field selections. Falls back to
   * defaultAspects if optimization isn't possible (missing registry, unmapped fields, etc).
   *
   * Usage in entity type batchLoad:
   *   Set<String> aspects = AspectUtils.getOptimizedAspects(context, "Dataset", ALL_ASPECTS, "datasetKey");
   *
   * @param context the QueryContext containing AspectMappingRegistry and DataFetchingEnvironment
   * @param entityTypeName the GraphQL type name (e.g., "Dataset", "CorpUser")
   * @param defaultAspects the full set of aspects to use as fallback
   * @param alwaysIncludeAspects aspects to always include (e.g., key aspects)
   * @return optimized aspect set, or defaultAspects if optimization isn't possible
   */
  @Nonnull
  public static Set<String> getOptimizedAspects(
      @Nonnull final QueryContext context,
      @Nonnull final String entityTypeName,
      @Nonnull final Set<String> defaultAspects,
      @Nonnull final String... alwaysIncludeAspects) {

    // Check if we have the necessary context for optimization
    if (context.getDataFetchingEnvironment() == null
        || context.getAspectMappingRegistry() == null) {
      log.debug(
          "DataFetchingEnvironment or AspectMappingRegistry not available for {}, fetching all aspects",
          entityTypeName);
      return defaultAspects;
    }

    // Attempt to determine required aspects from GraphQL field selections
    Set<String> requiredAspects =
        context
            .getAspectMappingRegistry()
            .getRequiredAspects(
                entityTypeName, context.getDataFetchingEnvironment().getSelectionSet().getFields());

    // If we couldn't determine required aspects (e.g., unmapped field), fall back to all aspects
    if (requiredAspects == null) {
      log.debug(
          "Could not determine required aspects for {}, falling back to fetching all aspects",
          entityTypeName);
      return defaultAspects;
    }

    // Successfully optimized - build the minimal aspect set
    Set<String> optimizedAspects = new HashSet<>(requiredAspects);

    // Add any aspects that should always be included
    if (alwaysIncludeAspects != null && alwaysIncludeAspects.length > 0) {
      Collections.addAll(optimizedAspects, alwaysIncludeAspects);
    }

    log.info("Fetching optimized aspect set for {}: {}", entityTypeName, optimizedAspects);
    return optimizedAspects;
  }
}
