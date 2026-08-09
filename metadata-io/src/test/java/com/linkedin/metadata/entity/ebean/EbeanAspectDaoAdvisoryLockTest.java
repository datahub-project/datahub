package com.linkedin.metadata.entity.ebean;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

/**
 * Unit coverage for the MySQL advisory-lock name derivation. The stateless release relies on the
 * name being a deterministic function of the urn: {@code lockUrnsForWrite} and {@code
 * releaseUrnsForWrite} both recompute it (no stored/registered names), so a drift here would make
 * release miss the acquired lock and leak it. End-to-end GET_LOCK/RELEASE_LOCK behavior on a real
 * connection is a MySQL Testcontainers IT.
 */
public class EbeanAspectDaoAdvisoryLockTest {

  @Test
  public void lockNameIsDeterministicSoAcquireAndReleaseMatch() {
    String urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)";
    assertEquals(
        EbeanAspectDao.mysqlLockName(urn),
        EbeanAspectDao.mysqlLockName(urn),
        "acquire and release must derive the same lock name from a urn");
  }

  @Test
  public void lockNameWithinMysqlSixtyFourCharCap() {
    String longUrn = "urn:li:dataset:(urn:li:dataPlatform:mysql," + "x".repeat(300) + ",PROD)";
    assertTrue(
        EbeanAspectDao.mysqlLockName(longUrn).length() <= 64,
        "MySQL caps GET_LOCK names at 64 chars");
  }

  @Test
  public void distinctUrnsGetDistinctNames() {
    assertNotEquals(
        EbeanAspectDao.mysqlLockName("urn:li:corpuser:a"),
        EbeanAspectDao.mysqlLockName("urn:li:corpuser:b"));
  }
}
