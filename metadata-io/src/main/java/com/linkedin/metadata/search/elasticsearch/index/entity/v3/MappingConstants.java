package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import java.util.Set;

/**
 * Constants used throughout the mapping generation process. This class centralizes all magic
 * strings and repeated patterns to improve maintainability.
 */
public final class MappingConstants {

  private MappingConstants() {
    // Utility class - prevent instantiation
  }

  public static final String ASPECT_FIELD_DELIMITER = ".";
  public static final String ASPECTS_FIELD_NAME = "_aspects";
  public static final String SEARCH_FIELD_NAME = "_search";

  /** Root fields owned by the V3 write/mapping path; contributors must not emit these keys. */
  public static final Set<String> STRATEGY_OWNED_ROOT_FIELDS =
      Set.of(
          "urn",
          "_entityType",
          ASPECTS_FIELD_NAME,
          SEARCH_FIELD_NAME,
          "runId",
          "structuredProperties",
          "systemCreated",
          "systemIndexModified");
}
