package com.linkedin.metadata.sqlsetup.postgres.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import org.testng.annotations.Test;

public class SqlMigrationScriptLoaderTest {

  @Test
  public void discoverOrdersVersionedBeforeRepeatable() throws Exception {
    List<SqlMigrationScript> scripts =
        SqlMigrationScriptLoader.discover(
            getClass().getClassLoader(), "sqlsetup/testfixture/migrations");
    assertEquals(scripts.size(), 3);
    assertEquals(scripts.get(0).getType(), SqlMigrationType.VERSIONED);
    assertEquals(scripts.get(0).getVersion(), "V001");
    assertEquals(scripts.get(1).getVersion(), "V002");
    assertEquals(scripts.get(2).getType(), SqlMigrationType.REPEATABLE);
    assertTrue(scripts.get(2).getVersion().startsWith("R__"));
  }
}
