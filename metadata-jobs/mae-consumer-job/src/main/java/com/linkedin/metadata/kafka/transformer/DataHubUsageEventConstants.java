package com.linkedin.metadata.kafka.transformer;

class DataHubUsageEventConstants {
  private DataHubUsageEventConstants() {
  }

  // Common fields
  static final String TYPE = "type";
  static final String TIMESTAMP = "timestamp";
  static final String ACTOR_URN = "actorUrn";

  // Event specific fields
  static final String ENTITY_URN = "entityUrn";
  static final String ENTITY_TYPE = "entityType";
}
