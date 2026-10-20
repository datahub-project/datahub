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
  public static final String URN_FIELD = "urn";
  public static final String RUN_ID_FIELD = "runId";
  public static final String ENTITY_TYPE_FIELD = "_entityType";

  /** Elasticsearch meta field used to select an index. Not stored from an aspect. */
  public static final String INDEX_FIELD = "_index";

  /**
   * Keyword fields the index path writes itself. Searchable aspect fields, including the
   * deprecation aspect's {@code deprecated} boolean, are resolved from the registry instead.
   */
  public static final Set<String> SYSTEM_KEYWORD_FIELDS =
      Set.of(URN_FIELD, RUN_ID_FIELD, INDEX_FIELD, ENTITY_TYPE_FIELD);

  /** Root fields owned by the V3 write/mapping path; contributors must not emit these keys. */
  public static final Set<String> STRATEGY_OWNED_ROOT_FIELDS =
      Set.of(
          URN_FIELD,
          ENTITY_TYPE_FIELD,
          ASPECTS_FIELD_NAME,
          "_system",
          RUN_ID_FIELD,
          "structuredProperties",
          "systemCreated",
          "systemIndexModified");
}
