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

  /** Parsed identity of {@link #URN_FIELD}, in its original case. */
  public static final String URN_NAME_FIELD = "urn_name";

  public static final String RUN_ID_FIELD = "runId";
  public static final String ENTITY_TYPE_FIELD = "_entityType";

  /**
   * Present only while a fill's tombstone is keeping a hard-deleted entity from being recreated.
   */
  public static final String TOMBSTONE_FIELD = "_v3Tombstone";

  /** Elasticsearch meta field used to select an index. Not stored from an aspect. */
  public static final String INDEX_FIELD = "_index";

  /** Elasticsearch meta field for the hit score. Not stored from an aspect. */
  public static final String SCORE_FIELD = "_score";

  /**
   * Keyword fields the index path writes itself. Searchable aspect fields, including the
   * deprecation aspect's {@code deprecated} boolean, are resolved from the registry instead.
   */
  public static final Set<String> SYSTEM_KEYWORD_FIELDS =
      Set.of(URN_FIELD, URN_NAME_FIELD, RUN_ID_FIELD, INDEX_FIELD, ENTITY_TYPE_FIELD);

  /**
   * Actor URN of the first write of every aspect, copied from {@code systemMetadata.aspectCreated}.
   * Entity audit stamps such as {@code createdBy} are not indexed on Search V3, so this is the
   * creator field on the V3 index.
   */
  public static final String SYSTEM_CREATED_ACTOR_FIELD = "_system.created.actor";

  /**
   * Time of the first write of every aspect, copied from {@code systemMetadata.aspectCreated}.
   * Entity audit stamps such as {@code createdAt} are not indexed on Search V3, so this is the
   * created time on the V3 index.
   */
  public static final String SYSTEM_CREATED_TIME_FIELD = "_system.created.time";

  /**
   * Time of the latest write of every aspect, copied from {@code systemMetadata.aspectModified}.
   * Entity audit stamps such as {@code lastModifiedAt} are not indexed on Search V3, so this is the
   * modified time on the V3 index.
   */
  public static final String SYSTEM_MODIFIED_TIME_FIELD = "_system.modified.time";

  /** Root fields owned by the V3 write/mapping path; contributors must not emit these keys. */
  public static final Set<String> STRATEGY_OWNED_ROOT_FIELDS =
      Set.of(
          URN_FIELD,
          URN_NAME_FIELD,
          ENTITY_TYPE_FIELD,
          ASPECTS_FIELD_NAME,
          TOMBSTONE_FIELD,
          "_system",
          RUN_ID_FIELD,
          "structuredProperties",
          "systemCreated",
          "systemIndexModified");
}
