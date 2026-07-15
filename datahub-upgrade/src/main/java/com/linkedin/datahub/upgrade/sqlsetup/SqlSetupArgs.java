package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.metadata.config.postgres.DatabaseType;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;

/**
 * Command-line / environment-driven SqlSetup operations (tables, database, users, CDC). Details for
 * optional PostgreSQL extensions use {@code postgres.*} via {@link
 * com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties}.
 */
@Value
@ToString(exclude = {"cdcPassword", "createUserPassword"})
public class SqlSetupArgs {
  boolean createTables;
  boolean createDatabase; // PostgreSQL only
  boolean createUser;
  boolean iamAuthEnabled;
  DatabaseType dbType; // mysql or postgres
  boolean cdcEnabled;
  String cdcUser;
  String cdcPassword;
  String createUserUsername;
  String createUserPassword; // If null, IAM authentication is used
  String host;
  int port;
  String databaseName;

  /**
   * PostgreSQL: normalized schema for metadata DDL (matches {@code postgres.schema}, typically
   * {@code public}); null for MySQL.
   */
  String postgresMetadataSchema;

  @Getter(lombok.AccessLevel.NONE)
  boolean createSchemaVersionIndex;

  public boolean createSchemaVersionIndex() {
    return createSchemaVersionIndex;
  }
}
