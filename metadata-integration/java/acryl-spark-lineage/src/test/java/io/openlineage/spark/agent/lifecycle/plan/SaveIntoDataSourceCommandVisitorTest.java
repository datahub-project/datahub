package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Covers the catalog-based Delta {@code saveAsTable} table-name recovery restored in PR #14911:
 * when a Delta write carries neither a "path" nor a "table" option, the visitor falls back to
 * parsing the CREATE TABLE statement from the query execution's SQL text. These tests pin the
 * parsing heuristic (quoting/db-qualifier stripping, IF NOT EXISTS handling, non-matching SQL).
 */
class SaveIntoDataSourceCommandVisitorTest {

  @Test
  void parsesPlainCreateTableName() {
    assertEquals(
        "events",
        SaveIntoDataSourceCommandVisitor.parseCreateTableName("CREATE TABLE events USING delta"));
  }

  @Test
  void stripsDatabaseQualifierAndQuoting() {
    assertEquals(
        "events",
        SaveIntoDataSourceCommandVisitor.parseCreateTableName(
            "CREATE TABLE `my_db`.`events` USING delta"));
  }

  @Test
  void skipsIfNotExistsQualifierAndReturnsRealName() {
    assertEquals(
        "events",
        SaveIntoDataSourceCommandVisitor.parseCreateTableName(
            "create table if not exists events using delta"));
  }

  @Test
  void skipsIfNotExistsAndStripsDatabaseQualifier() {
    assertEquals(
        "events",
        SaveIntoDataSourceCommandVisitor.parseCreateTableName(
            "CREATE TABLE IF NOT EXISTS my_db.events USING delta"));
  }

  @Test
  void returnsNullForNonCreateTableSql() {
    assertNull(
        SaveIntoDataSourceCommandVisitor.parseCreateTableName("INSERT INTO events VALUES (1)"));
  }

  @Test
  void returnsNullForNullSql() {
    assertNull(SaveIntoDataSourceCommandVisitor.parseCreateTableName(null));
  }
}
