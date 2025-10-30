package com.linkedin.datahub.upgrade.sqlsetup;

import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.Value;

@Value
@AllArgsConstructor
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
}
