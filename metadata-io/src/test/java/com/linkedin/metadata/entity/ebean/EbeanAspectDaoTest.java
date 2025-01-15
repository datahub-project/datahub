package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
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
    Database server = EbeanTestUtils.createTestServer(EbeanAspectDaoTest.class.getSimpleName());
    testDao = new EbeanAspectDao(server, EbeanConfiguration.testDefault);
  }

  @Test
  public void testGetNextVersionForUpdate() {
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        (txContext) -> {
          testDao.getNextVersions(
              Map.of("urn:li:corpuser:testGetNextVersionForUpdate", Set.of("status")));
          return "";
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        LoggedSql.stop().stream()
            .filter(str -> str.contains("testGetNextVersionForUpdate"))
            .toList();
    assertEquals(sql.size(), 2, String.format("Found: %s", sql));
    assertTrue(
        sql.get(0).contains("for update;"), String.format("Did not find `for update` in %s ", sql));
  }

  @Test
  public void testGetLatestAspectsForUpdate() throws JsonProcessingException {
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        (txContext) -> {
          testDao.getLatestAspects(
              Map.of("urn:li:corpuser:testGetLatestAspectsForUpdate", Set.of("status")), true);
          return "";
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        LoggedSql.stop().stream()
            .filter(str -> str.contains("testGetLatestAspectsForUpdate"))
            .toList();
    assertEquals(
        sql.size(), 1, String.format("Found: %s", new ObjectMapper().writeValueAsString(sql)));
    assertTrue(
        sql.get(0).contains("for update;"), String.format("Did not find `for update` in %s ", sql));
  }

  @Test
  public void testbatchGetForUpdate() throws JsonProcessingException {
    LoggedSql.start();

    testDao.runInTransactionWithRetryUnlocked(
        (txContext) -> {
          testDao.batchGet(
              Set.of(
                  new EntityAspectIdentifier(
                      "urn:li:corpuser:testbatchGetForUpdate1",
                      DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                      ASPECT_LATEST_VERSION),
                  new EntityAspectIdentifier(
                      "urn:li:corpuser:testbatchGetForUpdate2",
                      DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                      ASPECT_LATEST_VERSION)),
              true);
          return "";
        },
        mock(AspectsBatch.class),
        0);

    // Get the captured SQL statements
    List<String> sql =
        LoggedSql.stop().stream()
            .filter(
                str ->
                    str.contains("testbatchGetForUpdate1")
                        && str.contains("testbatchGetForUpdate2"))
            .toList();
    assertEquals(
        sql.size(), 1, String.format("Found: %s", new ObjectMapper().writeValueAsString(sql)));
    assertTrue(
        sql.get(0).contains("FOR UPDATE;"), String.format("Did not find `for update` in %s ", sql));
  }
}
