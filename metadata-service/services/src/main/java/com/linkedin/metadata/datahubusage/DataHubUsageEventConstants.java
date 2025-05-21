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
