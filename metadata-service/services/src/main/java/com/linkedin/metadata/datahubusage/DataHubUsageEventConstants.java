/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.datahubusage;

public class DataHubUsageEventConstants {
  private DataHubUsageEventConstants() {}

  // Common fields
  public static final String TYPE = "type";
  public static final String TIMESTAMP = "timestamp";
  public static final String ACTOR_URN = "actorUrn";

  // Event specific fields
  public static final String ENTITY_URN = "entityUrn";
  public static final String ENTITY_TYPE = "entityType";
  public static final String QUERY = "query";
  public static final String LOGIN_SOURCE = "loginSource";
  public static final String USAGE_SOURCE = "usageSource";
  public static final String BACKEND_SOURCE = "backend";
  public static final String TRACE_ID = "traceId";
  public static final String ASPECT_NAME = "aspectName";
  public static final String EVENT_SOURCE = "eventSource";
  public static final String USER_AGENT = "userAgent";
  public static final String SOURCE_IP = "sourceIP";
}
