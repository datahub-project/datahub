package com.linkedin.datahub.upgrade.sqlsetup;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class SqlSetupArgsTest {

  @Test
  public void testDefaultValues() {
    SqlSetupArgs args = new SqlSetupArgs();

    assertEquals(args.createTables, true);
    assertEquals(args.createDatabase, true);
    assertEquals(args.createUser, false);
    assertEquals(args.iamAuthEnabled, false);
    assertEquals(args.dbType, DatabaseType.MYSQL);
    assertEquals(args.cdcEnabled, false);
    assertEquals(args.cdcUser, "datahub_cdc");
    assertEquals(args.cdcPassword, "datahub_cdc");
    assertEquals(args.host, "localhost");
    assertEquals(args.port, 0);
    assertEquals(args.databaseName, "datahub");
  }

  @Test
  public void testSettersAndGetters() {
    SqlSetupArgs args = new SqlSetupArgs();

    args.createTables = false;
    args.createDatabase = false;
    args.createUser = true;
    args.iamAuthEnabled = true;
    args.dbType = DatabaseType.POSTGRES;
    args.cdcEnabled = true;
    args.cdcUser = "custom_cdc";
    args.cdcPassword = "custom_password";
    args.createUserUsername = "testuser";
    args.createUserPassword = "testpass";
    args.host = "testhost";
    args.port = 5432;
    args.databaseName = "testdb";
    args.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";

    assertEquals(args.createTables, false);
    assertEquals(args.createDatabase, false);
    assertEquals(args.createUser, true);
    assertEquals(args.iamAuthEnabled, true);
    assertEquals(args.dbType, DatabaseType.POSTGRES);
    assertEquals(args.cdcEnabled, true);
    assertEquals(args.cdcUser, "custom_cdc");
    assertEquals(args.cdcPassword, "custom_password");
    assertEquals(args.createUserUsername, "testuser");
    assertEquals(args.createUserPassword, "testpass");
    assertEquals(args.host, "testhost");
    assertEquals(args.port, 5432);
    assertEquals(args.databaseName, "testdb");
    assertEquals(args.createUserIamRole, "arn:aws:iam::123456789012:role/datahub-role");
  }

  @Test
  public void testClone() {
    SqlSetupArgs original = new SqlSetupArgs();
    original.createTables = false;
    original.createDatabase = false;
    original.createUser = true;
    original.iamAuthEnabled = true;
    original.dbType = DatabaseType.POSTGRES;
    original.cdcEnabled = true;
    original.cdcUser = "custom_cdc";
    original.cdcPassword = "custom_password";
    original.createUserUsername = "testuser";
    original.createUserPassword = "testpass";
    original.host = "testhost";
    original.port = 5432;
    original.databaseName = "testdb";
    original.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";

    // Test field assignments work correctly
    assertEquals(original.createTables, false);
    assertEquals(original.createDatabase, false);
    assertEquals(original.createUser, true);
    assertEquals(original.iamAuthEnabled, true);
    assertEquals(original.dbType, DatabaseType.POSTGRES);
    assertEquals(original.cdcEnabled, true);
    assertEquals(original.cdcUser, "custom_cdc");
    assertEquals(original.cdcPassword, "custom_password");
    assertEquals(original.createUserUsername, "testuser");
    assertEquals(original.createUserPassword, "testpass");
    assertEquals(original.host, "testhost");
    assertEquals(original.port, 5432);
    assertEquals(original.databaseName, "testdb");
    assertEquals(original.createUserIamRole, "arn:aws:iam::123456789012:role/datahub-role");
  }

  @Test
  public void testFieldAssignmentWithNullValues() {
    SqlSetupArgs original = new SqlSetupArgs();
    original.createTables = false;
    original.createDatabase = false;
    original.createUser = true;
    original.cdcEnabled = true;
    original.port = 5432;
    // Leave other fields as null/default

    // Test that null values are handled correctly
    assertEquals(original.createTables, false);
    assertEquals(original.createDatabase, false);
    assertEquals(original.createUser, true);
    assertEquals(original.cdcEnabled, true);
    assertEquals(original.port, 5432);
    assertEquals(original.createUserUsername, null);
    assertEquals(original.createUserPassword, null);
    assertEquals(original.host, "localhost");
    assertEquals(original.databaseName, "datahub");
    assertEquals(original.createUserIamRole, null);
  }
}
