package com.linkedin.datahub.graphql;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/** Constants relating to GraphQL type system & execution. */
public class Constants {

  private Constants() {}

  public static final String URN_FIELD_NAME = "urn";
  public static final String URNS_FIELD_NAME = "urns";
  public static final String GMS_SCHEMA_FILE = "entity.graphql";
  public static final String SEARCH_SCHEMA_FILE = "search.graphql";
  public static final String APP_SCHEMA_FILE = "app.graphql";
  public static final String AUTH_SCHEMA_FILE = "auth.graphql";
  public static final String ANALYTICS_SCHEMA_FILE = "analytics.graphql";
  public static final String RECOMMENDATIONS_SCHEMA_FILE = "recommendation.graphql";
  public static final String INGESTION_SCHEMA_FILE = "ingestion.graphql";
  public static final String TIMELINE_SCHEMA_FILE = "timeline.graphql";
  public static final String TESTS_SCHEMA_FILE = "tests.graphql";
  public static final String STEPS_SCHEMA_FILE = "step.graphql";
  public static final String LINEAGE_SCHEMA_FILE = "lineage.graphql";
  public static final String PROPERTIES_SCHEMA_FILE = "properties.graphql";
  public static final String FORMS_SCHEMA_FILE = "forms.graphql";
  public static final String ASSERTIONS_SCHEMA_FILE = "assertions.graphql";
  public static final String COMMON_SCHEMA_FILE = "common.graphql";
  public static final String INCIDENTS_SCHEMA_FILE = "incident.graphql";
  public static final String CONTRACTS_SCHEMA_FILE = "contract.graphql";
  public static final String CONNECTIONS_SCHEMA_FILE = "connection.graphql";
  public static final String VERSION_SCHEMA_FILE = "versioning.graphql";
  public static final String QUERY_SCHEMA_FILE = "query.graphql";
  public static final String BROWSE_PATH_DELIMITER = "/";
  public static final String BROWSE_PATH_V2_DELIMITER = "␟";
  public static final String VERSION_STAMP_FIELD_NAME = "versionStamp";
  public static final String ENTITY_FILTER_NAME = "_entityType";

  public static final Set<String> DEFAULT_PERSONA_URNS =
      ImmutableSet.of(
          "urn:li:dataHubPersona:technicalUser",
          "urn:li:dataHubPersona:businessUser",
          "urn:li:dataHubPersona:dataLeader",
          "urn:li:dataHubPersona:dataSteward");
}
