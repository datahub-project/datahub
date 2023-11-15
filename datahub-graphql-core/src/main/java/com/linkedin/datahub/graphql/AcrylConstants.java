package com.linkedin.datahub.graphql;

/**
 * Acryl-specific constants relating to GraphQL type system & execution.
 */
public class AcrylConstants {

  public static final String CONNECTIONS_SCHEMA_FILE = "connection.graphql";
  public static final String MONITORS_SCHEMA_FILE = "monitor.graphql";
  public static final String INTEGRATIONS_SCHEMA_FILE = "integration.graphql";
  public static final String INGESTION_SOURCE_EXECUTOR_CLI = "__datahub_cli_";
  public static final String CONSTRAINTS_SCHEMA_FILE = "constraints.graphql";
  public static final String INCIDENTS_SCHEMA_FILE = "incident.graphql";
  public static final String ACTIONS_SCHEMA_FILE = "actions.graphql";
  public static final String ANOMALY_SCHEMA_FILE = "anomaly.graphql";
  public static final String ASSERTIONS_SCHEMA_FILE = "assertions.graphql";
  public static final String NOTIFICATIONS_SCHEMA_FILE = "notifications.graphql";
  public static final String SUBSCRIPTIONS_SCHEMA_FILE = "subscriptions.graphql";
  public static final String CONTRACTS_SCHEMA_FILE = "contract.graphql";

  private AcrylConstants() { };
}
