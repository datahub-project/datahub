/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.graph.elastic.utils;

/** Constants used for graph query operations and field names. */
public final class GraphQueryConstants {

  private GraphQueryConstants() {
    // Constants class, prevent instantiation
  }

  // Common constants that can be shared across implementations
  public static final String SOURCE = "source";
  public static final String DESTINATION = "destination";
  public static final String RELATIONSHIP_TYPE = "relationshipType";
  public static final String SOURCE_TYPE = SOURCE + ".entityType";
  public static final String SOURCE_URN = SOURCE + ".urn";
  public static final String DESTINATION_TYPE = DESTINATION + ".entityType";
  public static final String DESTINATION_URN = DESTINATION + ".urn";
  public static final String SEARCH_EXECUTIONS_METRIC = "num_elasticSearch_reads";
  public static final String CREATED_ON = "createdOn";
  public static final String CREATED_ACTOR = "createdActor";
  public static final String UPDATED_ON = "updatedOn";
  public static final String UPDATED_ACTOR = "updatedActor";
  public static final String PROPERTIES = "properties";
  public static final String UI = "UI";
}
