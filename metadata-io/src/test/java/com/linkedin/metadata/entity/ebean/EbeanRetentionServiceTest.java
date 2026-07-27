package com.linkedin.metadata.entity.ebean;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import io.ebean.Database;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanRetentionServiceTest {

  private Database server;
  private EbeanRetentionService<?> retentionService;

  @BeforeMethod
  public void setup() {
    server = EbeanTestUtils.createTestServer(EbeanRetentionServiceTest.class.getSimpleName());
    retentionService = new EbeanRetentionService<>(mock(EntityService.class), server, 2);
  }

  @AfterMethod
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(server);
  }

  @Test
  public void testKeysetPagination_SkipsVersionZeroOnlyPairs() {
    insertAspect("urn:li:corpuser:a", "status", 0);
    insertAspect("urn:li:corpuser:b", "status", 0);
    insertAspect("urn:li:corpuser:b", "status", 1);
    insertAspect("urn:li:corpuser:c", "corpUserInfo", 0);
    insertAspect("urn:li:corpuser:c", "corpUserInfo", 2);

    List<EbeanAspectV2> page =
        retentionService.getPagedAspectsByKeyset(null, null, null, "", "", 10, null);

    assertEquals(page.size(), 2);
    assertEquals(
        page.stream().map(r -> r.getUrn() + "|" + r.getAspect()).collect(Collectors.toList()),
        List.of("urn:li:corpuser:b|status", "urn:li:corpuser:c|corpUserInfo"));
    assertEquals(page.get(0).getVersion(), 1L);
    assertEquals(page.get(1).getVersion(), 2L);
  }

  @Test
  public void testKeysetPagination_ResumesAfterLastKey() {
    insertAspect("urn:li:corpuser:a", "status", 0);
    insertAspect("urn:li:corpuser:a", "status", 1);
    insertAspect("urn:li:corpuser:b", "status", 0);
    insertAspect("urn:li:corpuser:b", "status", 1);
    insertAspect("urn:li:corpuser:c", "status", 0);
    insertAspect("urn:li:corpuser:c", "status", 1);

    List<EbeanAspectV2> first =
        retentionService.getPagedAspectsByKeyset(null, null, null, "", "", 2, null);
    assertEquals(first.size(), 2);
    assertEquals(first.get(0).getUrn(), "urn:li:corpuser:a");
    assertEquals(first.get(1).getUrn(), "urn:li:corpuser:b");

    EbeanAspectV2 last = first.get(first.size() - 1);
    List<EbeanAspectV2> second =
        retentionService.getPagedAspectsByKeyset(
            null, null, null, last.getUrn(), last.getAspect(), 2, null);
    assertEquals(second.size(), 1);
    assertEquals(second.get(0).getUrn(), "urn:li:corpuser:c");

    List<EbeanAspectV2> third =
        retentionService.getPagedAspectsByKeyset(
            null, null, null, second.get(0).getUrn(), second.get(0).getAspect(), 2, null);
    assertTrue(third.isEmpty());
  }

  @Test
  public void testKeysetPagination_EntityAndAspectFilters() {
    insertAspect("urn:li:corpuser:a", "status", 0);
    insertAspect("urn:li:corpuser:a", "status", 1);
    insertAspect("urn:li:corpuser:a", "corpUserInfo", 0);
    insertAspect("urn:li:corpuser:a", "corpUserInfo", 1);
    insertAspect("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)", "status", 0);
    insertAspect("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)", "status", 1);

    List<EbeanAspectV2> corpuserOnly =
        retentionService.getPagedAspectsByKeyset(null, "corpuser", null, "", "", 10, null);
    assertEquals(corpuserOnly.size(), 2);

    List<EbeanAspectV2> statusOnly =
        retentionService.getPagedAspectsByKeyset(null, null, "status", "", "", 10, null);
    assertEquals(statusOnly.size(), 2);
    assertTrue(statusOnly.stream().allMatch(r -> "status".equals(r.getAspect())));

    List<EbeanAspectV2> exactUrn =
        retentionService.getPagedAspectsByKeyset(
            "urn:li:corpuser:a", null, "corpUserInfo", "", "", 10, null);
    assertEquals(exactUrn.size(), 1);
    assertEquals(exactUrn.get(0).getAspect(), "corpUserInfo");
  }

  @Test
  public void testKeysetPagination_MinVersionCountHaving() {
    insertAspect("urn:li:corpuser:few", "status", 0);
    insertAspect("urn:li:corpuser:few", "status", 1);
    insertAspect("urn:li:corpuser:many", "status", 0);
    insertAspect("urn:li:corpuser:many", "status", 1);
    insertAspect("urn:li:corpuser:many", "status", 2);
    insertAspect("urn:li:corpuser:many", "status", 3);

    List<EbeanAspectV2> page =
        retentionService.getPagedAspectsByKeyset(null, null, null, "", "", 10, 2);
    assertEquals(page.size(), 1);
    assertEquals(page.get(0).getUrn(), "urn:li:corpuser:many");
    assertEquals(page.get(0).getVersion(), 3L);
  }

  @Test
  public void testBatchApplyRetentionEntities_UsesKeysetNotOffset() {
    for (int i = 0; i < 5; i++) {
      String urn = "urn:li:corpuser:user" + i;
      insertAspect(urn, "status", 0);
      insertAspect(urn, "status", 1);
      insertAspect(urn, "status", 2);
    }

    List<EbeanAspectV2> firstPage =
        retentionService.getPagedAspectsByKeyset(null, null, null, "", "", 2, 2);
    assertEquals(firstPage.size(), 2);
    assertEquals(firstPage.get(0).getUrn(), "urn:li:corpuser:user0");
    assertEquals(firstPage.get(1).getUrn(), "urn:li:corpuser:user1");

    BulkApplyRetentionArgs args = new BulkApplyRetentionArgs();
    args.start = 2;
    args.count = 10;
    args.attemptWithVersion = 2;
    // Advances via keyset skip (no SQL OFFSET) and processes remaining candidates
    BulkApplyRetentionResult result = retentionService.batchApplyRetentionEntities(args);
    assertEquals(result.argStart, 2);
    // 5 candidates with count>2; start=2 leaves user2..user4 → 3 handled (empty policies still
    // counted)
    assertEquals(result.rowsHandled, 3);
  }

  private void insertAspect(String urn, String aspect, long version) {
    EbeanAspectV2 row =
        new EbeanAspectV2(
            urn,
            aspect,
            version,
            "{}",
            new Timestamp(System.currentTimeMillis()),
            "urn:li:corpuser:datahub",
            null,
            null);
    server.save(row);
  }
}
