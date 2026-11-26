package com.linkedin.metadata.entity.ebean;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.ebean.DuplicateKeyException;
import jakarta.persistence.PersistenceException;
import java.sql.SQLException;
import org.testng.annotations.Test;

/**
 * Behavioral coverage for the duplicate-key detection that routes a concurrent version-0 insert to
 * a conflict (retry) rather than a hard error. Dialect CAS SQL and end-to-end conflict behavior are
 * not yet covered here — the H2 / MySQL / Postgres CAS and concurrency integration tests land with
 * Stage 2 (scoped retry).
 */
public class EbeanAspectDaoOptimisticLockingTest {

  @Test
  public void detectsEbeanDuplicateKeyException() {
    PersistenceException e = new DuplicateKeyException("dup", new RuntimeException());
    assertTrue(EbeanAspectDao.isDuplicateKeyCause(e));
  }

  @Test
  public void detectsPostgresUniqueViolationSqlState() {
    PersistenceException e = new PersistenceException(new SQLException("dup", "23505"));
    assertTrue(EbeanAspectDao.isDuplicateKeyCause(e));
  }

  @Test
  public void detectsMysqlDuplicateEntryErrorCode() {
    // SQLState 23000 is any integrity violation; only vendor code 1062 is a duplicate entry.
    PersistenceException e = new PersistenceException(new SQLException("dup", "23000", 1062));
    assertTrue(EbeanAspectDao.isDuplicateKeyCause(e));
  }

  @Test
  public void detectsDuplicateKeyDeepInCauseChain() {
    PersistenceException e =
        new PersistenceException(new RuntimeException(new SQLException("dup", "23505")));
    assertTrue(EbeanAspectDao.isDuplicateKeyCause(e));
  }

  @Test
  public void ignoresGenericIntegrityViolation() {
    // Foreign-key / check violations share SQLState 23000 but must not be treated as duplicates.
    PersistenceException e = new PersistenceException(new SQLException("fk", "23000"));
    assertFalse(EbeanAspectDao.isDuplicateKeyCause(e));
  }
}
