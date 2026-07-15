package com.linkedin.metadata.datahubusage.postgres;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PostgresUsageEventInsertRow {
  String id;
  long timestampMs;
  String eventType;
  String usageSource;
  String actorUrn;
  String entityUrn;
  String entityType;
  String browserId;
  String query;
  String section;
  String actionType;
  String aspectName;

  /** Full JSON document matching the Elasticsearch _source payload. */
  String documentJson;
}
