package com.linkedin.metadata.entity.ebean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionArgs;
import com.linkedin.metadata.entity.retention.BulkApplyRetentionResult;
import com.linkedin.metadata.entity.retention.RetentionBatchEntry;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.RetentionTestUtils;
import com.linkedin.metadata.entity.retention.SimpleRetentionKey;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.retention.Retention;
import com.linkedin.retention.VersionBasedRetention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanRetentionServiceTest {

  private Database server;
  private EbeanRetentionService<?> retentionService;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    server = EbeanTestUtils.createTestServer(EbeanRetentionServiceTest.class.getSimpleName());
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    retentionService =
        new EbeanRetentionService<>(
            mock(EntityService.class),
            server,
            2,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server),
            mock(SystemEntityClient.class));
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
        retentionService.getPagedAspectsByKeyset(opContext, null, null, null, "", "", 10, null);

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
        retentionService.getPagedAspectsByKeyset(opContext, null, null, null, "", "", 2, null);
    assertEquals(first.size(), 2);
    assertEquals(first.get(0).getUrn(), "urn:li:corpuser:a");
    assertEquals(first.get(1).getUrn(), "urn:li:corpuser:b");

    EbeanAspectV2 last = first.get(first.size() - 1);
    List<EbeanAspectV2> second =
        retentionService.getPagedAspectsByKeyset(
            opContext, null, null, null, last.getUrn(), last.getAspect(), 2, null);
    assertEquals(second.size(), 1);
    assertEquals(second.get(0).getUrn(), "urn:li:corpuser:c");

    List<EbeanAspectV2> third =
        retentionService.getPagedAspectsByKeyset(
            opContext,
            null,
            null,
            null,
            second.get(0).getUrn(),
            second.get(0).getAspect(),
            2,
            null);
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
        retentionService.getPagedAspectsByKeyset(
            opContext, null, "corpuser", null, "", "", 10, null);
    assertEquals(corpuserOnly.size(), 2);

    List<EbeanAspectV2> statusOnly =
        retentionService.getPagedAspectsByKeyset(opContext, null, null, "status", "", "", 10, null);
    assertEquals(statusOnly.size(), 2);
    assertTrue(statusOnly.stream().allMatch(r -> "status".equals(r.getAspect())));

    List<EbeanAspectV2> exactUrn =
        retentionService.getPagedAspectsByKeyset(
            opContext, "urn:li:corpuser:a", null, "corpUserInfo", "", "", 10, null);
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
        retentionService.getPagedAspectsByKeyset(opContext, null, null, null, "", "", 10, 2);
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
        retentionService.getPagedAspectsByKeyset(opContext, null, null, null, "", "", 2, 2);
    assertEquals(firstPage.size(), 2);
    assertEquals(firstPage.get(0).getUrn(), "urn:li:corpuser:user0");
    assertEquals(firstPage.get(1).getUrn(), "urn:li:corpuser:user1");

    BulkApplyRetentionArgs args = new BulkApplyRetentionArgs();
    args.opContext = opContext;
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

  @Test
  public void testApplyRetentionBatch_prunesOldVersionsKeepsLatest_returnsCommitted() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    String urn = "urn:li:corpuser:batchA";
    insertAspect(urn, "status", 0);
    insertAspect(urn, "status", 1);
    insertAspect(urn, "status", 2);

    // maxVersions=1, largestVersion=2 => delete version < 2 AND version != 0 => only v1 pruned.
    RetentionService.RetentionContext ctx =
        RetentionService.RetentionContext.builder()
            .urn(UrnUtils.getUrn(urn))
            .aspectName("status")
            .retentionPolicy(
                Optional.of(
                    new Retention().setVersion(new VersionBasedRetention().setMaxVersions(1))))
            .maxVersion(Optional.of(2L))
            .build();
    SimpleRetentionKey key = new SimpleRetentionKey(urn, "status");

    List<RetentionKey> committed =
        retentionService.applyRetentionBatchWithPolicyDefaults(
            opContext, List.of(new RetentionBatchEntry(key, ctx)));

    assertEquals(committed.size(), 1);
    // Latest (v0) always survives; only the old below-threshold version is gone.
    assertEquals(versionsFor(urn, "status"), List.of(0L, 2L));
  }

  @Test
  public void testApplyRetentionBatch_appliesEachContextIndependently() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    String urnA = "urn:li:corpuser:batchB";
    String urnB = "urn:li:corpuser:batchC";
    for (String u : List.of(urnA, urnB)) {
      insertAspect(u, "status", 0);
      insertAspect(u, "status", 1);
      insertAspect(u, "status", 2);
    }

    List<RetentionService.RetentionContext> contexts =
        List.of(retentionContext(urnA), retentionContext(urnB));
    List<RetentionKey> keys =
        List.of(new SimpleRetentionKey(urnA, "status"), new SimpleRetentionKey(urnB, "status"));
    List<RetentionBatchEntry> entries = new ArrayList<>(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      entries.add(new RetentionBatchEntry(keys.get(i), contexts.get(i)));
    }

    List<RetentionKey> committed =
        retentionService.applyRetentionBatchWithPolicyDefaults(opContext, entries);

    assertEquals(committed.size(), 2);
    assertEquals(versionsFor(urnA, "status"), List.of(0L, 2L));
    assertEquals(versionsFor(urnB, "status"), List.of(0L, 2L));
  }

  @Test
  public void testApplyRetentionBatch_poisonContextIsolated_siblingStillCommits() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    String good = "urn:li:corpuser:batchGood";
    String poison = "urn:li:corpuser:batchPoison";
    for (String u : List.of(good, poison)) {
      insertAspect(u, "status", 0);
      insertAspect(u, "status", 1);
      insertAspect(u, "status", 2);
    }

    EbeanRetentionService<?> svc = spy(retentionService);
    doAnswer(
            inv -> {
              RetentionService.RetentionContext ctx = inv.getArgument(1);
              if (ctx.getUrn().toString().equals(poison)) {
                throw new RuntimeException("forced delete failure");
              }
              return inv.callRealMethod();
            })
        .when(svc)
        .executeRetentionDeleteForContext(any(), any());

    SimpleRetentionKey goodKey = new SimpleRetentionKey(good, "status");
    SimpleRetentionKey poisonKey = new SimpleRetentionKey(poison, "status");
    List<RetentionKey> committed =
        svc.applyRetentionBatchWithPolicyDefaults(
            opContext,
            List.of(
                new RetentionBatchEntry(goodKey, retentionContext(good)),
                new RetentionBatchEntry(poisonKey, retentionContext(poison))));

    assertEquals(committed.size(), 1);
    assertEquals(committed.get(0), goodKey);

    assertEquals(versionsFor(good, "status"), List.of(0L, 2L));
    assertEquals(versionsFor(poison, "status"), List.of(0L, 1L, 2L));
  }

  @Test
  public void testApplyRetentionBatch_noPolicyContexts_returnsSameKeyInstancesAsInputs()
      throws java.net.URISyntaxException {
    // The drainer passes parallel (keys, contexts) lists and matches returned committed keys
    // against the original input keys via HashSet.contains. The Ebean override rebuilds each
    // context with a resolved policy but MUST echo back the SAME keys it received (at the
    // committed index) — not reconstructed ones — else the drainer's successes.contains(key) match
    // fails and committed keys re-drain forever. This pins that contract at the unit level.
    EntityService<?> entityService = mock(EntityService.class);
    // getRetention -> SystemEntityClient.batchGetV2 -> getEntitiesV2 returns empty -> getRetention
    // returns new Retention() (empty)
    when(entityService.getEntitiesV2(any(), any(), any(), any(), anyBoolean()))
        .thenReturn(Collections.emptyMap());
    EbeanRetentionService<?> svc =
        new EbeanRetentionService<>(
            entityService,
            server,
            2,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server),
            RetentionTestUtils.systemEntityClient(
                entityService, mock(EventProducer.class), mock(MetricUtils.class)));

    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    String urnA = "urn:li:corpuser:noPolicyA";
    String urnB = "urn:li:corpuser:noPolicyB";
    insertAspect(urnA, "status", 0);
    insertAspect(urnB, "status", 0);

    // No retentionPolicy -> forces the rebuild path in applyRetentionBatchWithPolicyDefaults.
    RetentionService.RetentionContext ctxA =
        RetentionService.RetentionContext.builder()
            .urn(UrnUtils.getUrn(urnA))
            .aspectName("status")
            .maxVersion(Optional.of(0L))
            .build();
    RetentionService.RetentionContext ctxB =
        RetentionService.RetentionContext.builder()
            .urn(UrnUtils.getUrn(urnB))
            .aspectName("status")
            .maxVersion(Optional.of(0L))
            .build();
    SimpleRetentionKey keyA = new SimpleRetentionKey(urnA, "status");
    SimpleRetentionKey keyB = new SimpleRetentionKey(urnB, "status");

    List<RetentionKey> committed =
        svc.applyRetentionBatchWithPolicyDefaults(
            opContext,
            List.of(new RetentionBatchEntry(keyA, ctxA), new RetentionBatchEntry(keyB, ctxB)));

    assertEquals(committed.size(), 2);
    // Same instance (not a rebuilt copy) -> drainer's successes.contains(originalKey) will match.
    assertSame(committed.get(0), keyA);
    assertSame(committed.get(1), keyB);
    // And a HashSet built from the returned list contains the original input keys.
    assertTrue(new HashSet<>(committed).contains(keyA));
    assertTrue(new HashSet<>(committed).contains(keyB));
  }

  private RetentionService.RetentionContext retentionContext(String urn) {
    return RetentionService.RetentionContext.builder()
        .urn(UrnUtils.getUrn(urn))
        .aspectName("status")
        .retentionPolicy(
            Optional.of(new Retention().setVersion(new VersionBasedRetention().setMaxVersions(1))))
        .maxVersion(Optional.of(2L))
        .build();
  }

  private List<Long> versionsFor(String urn, String aspect) {
    return server
        .find(EbeanAspectV2.class)
        .where()
        .eq("urn", urn)
        .eq("aspect", aspect)
        .orderBy()
        .asc("version")
        .findList()
        .stream()
        .map(EbeanAspectV2::getVersion)
        .collect(Collectors.toList());
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
