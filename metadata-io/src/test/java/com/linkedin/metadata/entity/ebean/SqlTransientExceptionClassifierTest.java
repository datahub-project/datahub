package com.linkedin.metadata.entity.ebean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import jakarta.persistence.PersistenceException;
import java.sql.SQLException;
import java.util.Set;
import org.testng.annotations.Test;

public class SqlTransientExceptionClassifierTest {

  private static final Set<String> BACKOFF_SQL_STATES = Set.of("40001", "40P01");
  private static final Set<Integer> BACKOFF_VENDOR_CODES = Set.of(1213);

  @Test
  public void testFindSqlError_NestedCause() {
    SQLException sql = new SQLException("deadlock", "40001", 1213);
    PersistenceException wrapped = new PersistenceException("tx failed", sql);

    SQLException found = SqlTransientExceptionClassifier.findSqlError(wrapped);
    assertNotNull(found);
    assertEquals(found.getSQLState(), "40001");
    assertEquals(found.getErrorCode(), 1213);
  }

  @Test
  public void testFindSqlError_NextExceptionChain() {
    SQLException next = new SQLException("deadlock detected", "40P01", 0);
    SQLException primary = new SQLException("wrapper", "08006", 0);
    primary.setNextException(next);
    RuntimeException wrapped = new RuntimeException(primary);

    SQLException found = SqlTransientExceptionClassifier.findSqlError(wrapped);
    assertNotNull(found);
    assertEquals(found.getSQLState(), "08006");

    SQLException matched =
        SqlTransientExceptionClassifier.findBackoffSqlError(
            wrapped, BACKOFF_SQL_STATES, BACKOFF_VENDOR_CODES);
    assertNotNull(matched);
    assertEquals(matched.getSQLState(), "40P01");
    assertTrue(
        SqlTransientExceptionClassifier.isBackoffEligible(
            wrapped, BACKOFF_SQL_STATES, BACKOFF_VENDOR_CODES));
  }

  @Test
  public void testFindSqlError_NoSqlException() {
    assertNull(SqlTransientExceptionClassifier.findSqlError(new RuntimeException("boom")));
  }

  @Test
  public void testIsBackoffEligible_SqlState40001() {
    SQLException sql = new SQLException("serialization failure", "40001");
    assertTrue(
        SqlTransientExceptionClassifier.isBackoffEligible(
            new PersistenceException(sql), BACKOFF_SQL_STATES, BACKOFF_VENDOR_CODES));
  }

  @Test
  public void testIsBackoffEligible_SqlState40P01() {
    SQLException sql = new SQLException("deadlock", "40P01");
    assertTrue(
        SqlTransientExceptionClassifier.isBackoffEligible(
            sql, BACKOFF_SQL_STATES, BACKOFF_VENDOR_CODES));
  }

  @Test
  public void testIsBackoffEligible_VendorCode1213() {
    SQLException sql = new SQLException("Deadlock found", null, 1213);
    assertTrue(
        SqlTransientExceptionClassifier.isBackoffEligible(
            new PersistenceException(sql), BACKOFF_SQL_STATES, BACKOFF_VENDOR_CODES));
  }

  @Test
  public void testIsBackoffEligible_NonMatch() {
    SQLException sql = new SQLException("duplicate", "23000", 1062);
    assertFalse(
        SqlTransientExceptionClassifier.isBackoffEligible(
            new PersistenceException(sql), BACKOFF_SQL_STATES, BACKOFF_VENDOR_CODES));
  }

  @Test
  public void testIsBackoffEligible_NextExceptionMatch() {
    SQLException next = new SQLException("deadlock", "40001", 1213);
    SQLException primary = new SQLException("see next", "HY000", 0);
    primary.setNextException(next);

    PersistenceException wrapped = new PersistenceException(primary);
    assertTrue(
        SqlTransientExceptionClassifier.isBackoffEligible(
            wrapped, BACKOFF_SQL_STATES, BACKOFF_VENDOR_CODES));
    SQLException matched =
        SqlTransientExceptionClassifier.findBackoffSqlError(
            wrapped, BACKOFF_SQL_STATES, BACKOFF_VENDOR_CODES);
    assertNotNull(matched);
    assertEquals(matched.getSQLState(), "40001");
    assertEquals(matched.getErrorCode(), 1213);
  }
}
