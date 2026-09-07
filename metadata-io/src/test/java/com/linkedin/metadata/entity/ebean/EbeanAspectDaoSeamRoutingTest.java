package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.TxScope;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Guards the per-request DB-routing seam. An extension module can replace {@link
 * AspectTableResolver} and {@link ScopedTransactionFactory} with {@code @Primary} beans that pick a
 * different underlying database based on the {@link OperationContext}, so a call site in {@link
 * EbeanAspectDao} that touches the {@code Database} field directly, or that hands the seam some
 * context other than the caller's, silently reads or writes against the wrong database — no
 * exception, no log. These tests therefore assert reference <em>identity</em> of the propagated
 * context (not equality), for one method per routing shape used in the class, and additionally
 * assert that the surrounding seam wrapping did not break the operation's real behavior against a
 * live H2 database.
 */
public class EbeanAspectDaoSeamRoutingTest {

  private static final String URN_READ = "urn:li:corpuser:seamRead";
  private static final String URN_WRITE = "urn:li:corpuser:seamWrite";
  private static final String URN_BATCH_A = "urn:li:corpuser:seamBatchA";
  private static final String URN_BATCH_B = "urn:li:corpuser:seamBatchB";
  private static final String URN_RANGE = "urn:li:corpuser:seamVersionRange";

  private Database server;
  private AspectTableResolver tableResolverSpy;
  private ScopedTransactionFactory txnFactorySpy;
  private EbeanAspectDao testDao;

  // Separate instances per routing shape: assertSame against a distinct context proves the DAO
  // forwarded its own parameter, which a single shared context could not distinguish from the DAO
  // reusing some other context it happens to hold.
  private OperationContext readContext;
  private OperationContext writeContext;
  private OperationContext rawSqlContext;

  @BeforeMethod
  public void setupTest() {
    server = EbeanTestUtils.createTestServer(EbeanAspectDaoSeamRoutingTest.class.getSimpleName());
    // Spies over the production defaults: behavior stays byte-identical to the OSS pass-through
    // while invocation arguments are captured.
    tableResolverSpy = spy(new PlainAspectTableResolver());
    txnFactorySpy = spy(new PassThroughScopedTransactionFactory(server));
    testDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            mock(MetricUtils.class),
            List.of(),
            null,
            tableResolverSpy,
            txnFactorySpy);
    readContext = TestOperationContexts.systemContextNoValidate();
    writeContext = TestOperationContexts.systemContextNoValidate();
    rawSqlContext = TestOperationContexts.systemContextNoValidate();
  }

  @AfterMethod
  public void cleanup() {
    EbeanTestUtils.shutdownDatabase(server);
  }

  @Test
  public void readRoutesCallerContextThroughScope() {
    insertRow(URN_READ, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION, "read-metadata");

    EntityAspect found =
        testDao.getAspect(readContext, URN_READ, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);

    assertNotNull(found, "scope-wrapped read must still return the row");
    assertEquals(found.getMetadata(), "read-metadata");

    ArgumentCaptor<OperationContext> scoped = ArgumentCaptor.forClass(OperationContext.class);
    verify(txnFactorySpy).runInScope(scoped.capture(), any());
    assertSame(scoped.getValue(), readContext, "runInScope must receive the caller's context");

    ArgumentCaptor<OperationContext> opened = ArgumentCaptor.forClass(OperationContext.class);
    verify(txnFactorySpy).scope(opened.capture());
    assertSame(opened.getValue(), readContext, "scope() must receive the caller's context");

    // Repeat with a different context on the same DAO instance: a call site that pinned or cached
    // the first context it ever saw would pass the assertions above but fail here.
    clearInvocations(txnFactorySpy);
    assertNotNull(
        testDao.getAspect(writeContext, URN_READ, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
    verify(txnFactorySpy).runInScope(scoped.capture(), any());
    assertSame(
        scoped.getValue(),
        writeContext,
        "each call must route on its own context, not a cached one");
  }

  @Test
  public void transactionalWriteRoutesCallerContextThroughBegin() {
    Optional<String> outcome =
        testDao.runInTransactionWithRetry(
            writeContext,
            txContext -> {
              server.insert(
                  newRow(URN_WRITE, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION, "written-in-tx"),
                  txContext.tx());
              return TransactionResult.commit("committed");
            },
            1);

    ArgumentCaptor<OperationContext> begun = ArgumentCaptor.forClass(OperationContext.class);
    verify(txnFactorySpy).begin(begun.capture(), any(TxScope.class));
    assertSame(begun.getValue(), writeContext, "begin() must receive the caller's context");

    assertEquals(outcome, Optional.of("committed"));
    // The seam handed back a real, committable transaction, not one that discarded the write.
    EntityAspect persisted =
        testDao.getAspect(writeContext, URN_WRITE, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    assertNotNull(persisted, "work done in the seam-provided transaction must be committed");
    assertEquals(persisted.getMetadata(), "written-in-tx");
  }

  @Test
  public void rawSqlBatchGetRoutesCallerContextThroughTableResolver() {
    insertRow(URN_BATCH_A, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION, "batch-a");
    insertRow(URN_BATCH_B, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION, "batch-b");

    EntityAspectIdentifier keyA =
        new EntityAspectIdentifier(URN_BATCH_A, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);
    EntityAspectIdentifier keyB =
        new EntityAspectIdentifier(URN_BATCH_B, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION);

    Map<EntityAspectIdentifier, EntityAspect> found =
        testDao.batchGet(rawSqlContext, Set.of(keyA, keyB), false);

    assertEquals(found.size(), 2, "hand-built IN batch-get must still hydrate both rows");
    assertEquals(found.get(keyA).getMetadata(), "batch-a");
    assertEquals(found.get(keyB).getMetadata(), "batch-b");

    ArgumentCaptor<OperationContext> resolved = ArgumentCaptor.forClass(OperationContext.class);
    verify(tableResolverSpy, atLeastOnce())
        .aspectTable(resolved.capture(), eq(EbeanAspectV2.TABLE_NAME));
    assertTrue(
        resolved.getAllValues().stream().allMatch(ctx -> ctx == rawSqlContext),
        "every table-name resolution in the raw SQL must use the caller's context");
  }

  @Test
  public void rawSqlVersionRangeRoutesCallerContextAndAggregatesCorrectly() {
    insertRow(URN_RANGE, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION, "v0");
    insertRow(URN_RANGE, STATUS_ASPECT_NAME, 7L, "v7");

    Pair<Long, Long> range = testDao.getVersionRange(rawSqlContext, URN_RANGE, STATUS_ASPECT_NAME);

    assertEquals(range.getFirst().longValue(), 0L);
    assertEquals(range.getSecond().longValue(), 7L);

    ArgumentCaptor<OperationContext> resolved = ArgumentCaptor.forClass(OperationContext.class);
    verify(tableResolverSpy).aspectTable(resolved.capture(), eq(EbeanAspectV2.TABLE_NAME));
    assertSame(resolved.getValue(), rawSqlContext);

    ArgumentCaptor<OperationContext> scoped = ArgumentCaptor.forClass(OperationContext.class);
    verify(txnFactorySpy).runInScope(scoped.capture(), any());
    assertSame(scoped.getValue(), rawSqlContext);
  }

  private void insertRow(String urn, String aspect, long version, String metadata) {
    // Written outside the DAO so the seam spies record only the invocations under test.
    server.save(newRow(urn, aspect, version, metadata));
  }

  private static EbeanAspectV2 newRow(String urn, String aspect, long version, String metadata) {
    EbeanAspectV2 row = new EbeanAspectV2();
    row.setKey(new EbeanAspectV2.PrimaryKey(urn, aspect, version));
    row.setMetadata(metadata);
    row.setCreatedBy("urn:li:corpuser:test");
    row.setCreatedFor(null);
    row.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    row.setSystemMetadata(null);
    return row;
  }
}
