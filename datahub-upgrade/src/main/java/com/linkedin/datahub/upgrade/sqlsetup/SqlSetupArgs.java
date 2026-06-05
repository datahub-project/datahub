package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;

/**
 * Command-line / environment-driven SqlSetup operations (tables, database, users, CDC). Optional
 * PostgreSQL extension DDL ({@code postgres.*}, pgQueue) is nested in {@link #postgres} when the
 * entity store is PostgreSQL.
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

  /** Spring {@code postgres.*} config; null for MySQL or when extensions are disabled. */
  @Nullable PostgresSqlSetupProperties postgres;

  public boolean createSchemaVersionIndex() {
    return createSchemaVersionIndex;
  }
}
