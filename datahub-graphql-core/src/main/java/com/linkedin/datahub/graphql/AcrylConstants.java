package com.linkedin.datahub.graphql;

/** Acryl-specific constants relating to GraphQL type system & execution. */
public class AcrylConstants {
  public static final String MONITORS_SCHEMA_FILE = "monitor.graphql";
  public static final String INTEGRATIONS_SCHEMA_FILE = "integration.graphql";
  public static final String INGESTION_SOURCE_EXECUTOR_CLI = "__datahub_cli_";
  public static final String CONSTRAINTS_SCHEMA_FILE = "constraints.graphql";
  public static final String ACTIONS_SCHEMA_FILE = "actions.graphql";
  public static final String ACTIONS_PIPELINE_SCHEMA_FILE = "actions_pipeline.graphql";
  public static final String ANOMALY_SCHEMA_FILE = "anomaly.graphql";
  public static final String NOTIFICATIONS_SCHEMA_FILE = "notifications.graphql";
  public static final String SUBSCRIPTIONS_SCHEMA_FILE = "subscriptions.graphql";
  public static final String AI_SCHEMA_FILE = "ai.graphql";
  public static final String SHARE_SCHEMA_FILE = "share.graphql";
  public static final String FORMS_ACRYL_SCHEMA_FILE = "forms.acryl.graphql";
  public static final String EXECUTOR_SCHEMA_FILE = "executor.graphql";
  public static final String REMOTE_EXECUTOR_SCHEMA_FILE = "remote_executor.saas.graphql";

  private AcrylConstants() {}
}
