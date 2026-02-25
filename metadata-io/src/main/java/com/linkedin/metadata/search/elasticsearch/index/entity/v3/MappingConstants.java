package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

/**
 * Constants used throughout the mapping generation process. This class centralizes all magic
 * strings and repeated patterns to improve maintainability.
 */
public final class MappingConstants {

  private MappingConstants() {
    // Utility class - prevent instantiation
  }

  // Field and aspect related constants
  public static final String ASPECT_FIELD_DELIMITER = ".";
  public static final String ASPECTS_FIELD_NAME = "_aspects";
}
