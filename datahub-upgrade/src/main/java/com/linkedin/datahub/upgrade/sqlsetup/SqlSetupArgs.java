package com.linkedin.datahub.upgrade.sqlsetup;

import lombok.Data;

@Data
public class SqlSetupArgs {
  public boolean createTables = true;
  public boolean createDatabase = true; // PostgreSQL only
  public boolean createUser = false;
  public boolean iamAuthEnabled = false;
  public DatabaseType dbType = DatabaseType.MYSQL; // mysql or postgres
  public boolean cdcEnabled = false;
  public String cdcUser = "datahub_cdc";
  public String cdcPassword = "datahub_cdc";
  public String createUserUsername;
  public String createUserPassword;
  public String host = "localhost";
  public int port;
  public String databaseName = "datahub";
  public String createUserIamRole; // IAM role for new user creation (required if IAM auth enabled)
}
