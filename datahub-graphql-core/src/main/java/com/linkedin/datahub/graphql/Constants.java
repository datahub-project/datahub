package com.linkedin.datahub.graphql;

/**
 * Constants relating to GraphQL type system & execution.
 */
public class Constants {

    private Constants() { };

    public static final String URN_FIELD_NAME = "urn";
    public static final String GMS_SCHEMA_FILE = "entity.graphql";
    public static final String SEARCH_SCHEMA_FILE = "search.graphql";
    public static final String APP_SCHEMA_FILE = "app.graphql";
    public static final String AUTH_SCHEMA_FILE = "auth.graphql";
    public static final String ANALYTICS_SCHEMA_FILE = "analytics.graphql";
    public static final String RECOMMENDATIONS_SCHEMA_FILE = "recommendation.graphql";
    public static final String INGESTION_SCHEMA_FILE = "ingestion.graphql";
    public static final String TIMELINE_SCHEMA_FILE = "timeline.graphql";
    public static final String BROWSE_PATH_DELIMITER = "/";
    public static final String VERSION_STAMP_FIELD_NAME = "versionStamp";
}
