package com.linkedin.metadata.entity.ebean.optimistic;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.MysqlTestUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.Database;
import org.testcontainers.containers.MySQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * MySQL Testcontainers coverage for optimistic-locking CAS SQL and behavior.
 *
 * <p>Included in {@code testng-mysql.xml} / {@code :metadata-io:testMysql}.
 */
public class EbeanOptimisticLockingMysqlIT extends EbeanOptimisticLockingDialectIT {

  private MySQLContainer<?> mysql;
  private Database database;

  @BeforeClass
  public void init() {
    mysql = MysqlTestUtils.startMysql();
    database =
        MysqlTestUtils.createEbeanPrimaryDatabase(
            mysql, MysqlTestUtils.uniqueServerName("ebean_ol_mysql_it"));
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
    return EbeanAspectDao.Dialect.MYSQL;
  }
}
