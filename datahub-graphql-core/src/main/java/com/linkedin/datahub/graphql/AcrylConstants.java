package com.linkedin.datahub.graphql;

/**
 * Acryl-specific constants relating to GraphQL type system & execution.
 */
public class AcrylConstants {

  public static final String CONNECTIONS_SCHEMA_FILE = "connection.graphql";
  public static final String MONITORS_SCHEMA_FILE = "monitor.graphql";
  public static final String INTEGRATIONS_SCHEMA_FILE = "integration.graphql";
  public static final String INGESTION_SOURCE_EXECUTOR_CLI = "__datahub_cli_";

  private AcrylConstants() { };
}
