package com.linkedin.metadata.entity.ebean;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.EbeanEntityServiceTest;
import io.ebean.Database;
import io.ebean.test.LoggedSql;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanAspectDaoTest {

  private EbeanAspectDao testDao;

  @BeforeMethod
  public void setupTest() {
    Database server = EbeanTestUtils.createTestServer(EbeanEntityServiceTest.class.getSimpleName());
    testDao = new EbeanAspectDao(server, EbeanConfiguration.testDefault);
  }

  @Test
  public void testGetNextVersionForUpdate() {
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        (txContext) -> {
          testDao.getNextVersions(Map.of("urn:li:corpuser:test", Set.of("status")));
          return "";
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        LoggedSql.stop().stream()
            .filter(str -> !str.contains("INFORMATION_SCHEMA.TABLES"))
            .toList();
    assertEquals(sql.size(), 2, String.format("Found: %s", sql));
    assertTrue(
        sql.get(0).contains("for update;"), String.format("Did not find `for update` in %s ", sql));
  }

  @Test
  public void testGetLatestAspectsForUpdate() {
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        (txContext) -> {
          testDao.getLatestAspects(Map.of("urn:li:corpuser:test", Set.of("status")), true);
          return "";
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        LoggedSql.stop().stream()
            .filter(str -> !str.contains("INFORMATION_SCHEMA.TABLES"))
            .toList();
    assertEquals(sql.size(), 1, String.format("Found: %s", sql));
    assertTrue(
        sql.get(0).contains("for update;"), String.format("Did not find `for update` in %s ", sql));
  }
}
