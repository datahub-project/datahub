package com.linkedin.metadata.sqlsetup.postgres.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import java.util.Map;
import org.testng.annotations.Test;

public class PostgresSqlUtilsTest {

  @Test
  public void quotePgIdentifierEscapesQuotes() {
    assertEquals(PostgresSqlUtils.quotePgIdentifier("queue"), "\"queue\"");
    assertEquals(PostgresSqlUtils.quotePgIdentifier("my\"schema"), "\"my\"\"schema\"");
  }

  @Test
  public void applyTokenReplacements() {
    String sql =
        PostgresSqlUtils.applyTokenReplacements(
            "SET search_path = __SCHEMA__, public", Map.of("__SCHEMA__", "\"queue\""));
    assertEquals(sql, "SET search_path = \"queue\", public");
  }

  @Test
  public void assertNoUnreplacedTokensFails() {
    assertThrows(
        SqlMigrationException.class,
        () -> PostgresSqlUtils.assertNoUnreplacedTokens("SELECT __LEFT__"));
  }
}
