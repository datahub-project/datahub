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
}
