package com.linkedin.metadata.entity.ebean.optimistic;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.Database;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * PostgreSQL Testcontainers coverage for optimistic-locking CAS SQL and behavior.
 *
 * <p>Included in {@code testng-postgresql.xml} / {@code :metadata-io:testPostgresql}.
 */
public class EbeanOptimisticLockingPostgresIT extends EbeanOptimisticLockingDialectIT {

  private PostgreSQLContainer<?> postgres;
  private Database database;

  @BeforeClass
  public void init() {
    postgres = PostgresTestUtils.startPostgres();
    PostgresTestUtils.IntegrationNamespace ns =
        PostgresTestUtils.newIntegrationNamespace("ebean_ol_it");
    database =
        PostgresTestUtils.createEbeanPrimaryDatabase(
            postgres, PostgresTestUtils.uniqueServerName("ebean_ol_postgres_it"), ns);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  @Override
  protected Database database() {
    return database;
  }

  @Override
  protected EbeanAspectDao.Dialect expectedDialect() {
    return EbeanAspectDao.Dialect.POSTGRES;
  }
}
