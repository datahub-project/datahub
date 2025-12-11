/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
