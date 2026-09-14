package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.EbeanTestUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.Transaction;
import io.ebean.TxScope;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The OSS default must be a true pass-through: {@link EbeanAspectDao} now routes every DB-touching
 * call through this factory, so any behavior it adds or drops (a transaction that isn't really
 * begun, a scope that quietly commits, a helper that swallows the work's exception) changes the
 * semantics of the whole write path. Asserted against a live H2 database rather than by inspecting
 * fields.
 */
public class PassThroughScopedTransactionFactoryTest {

  private Database server;
  private PassThroughScopedTransactionFactory factory;
  private OperationContext opContext;

  @BeforeMethod
  public void setupTest() {
    server =
        EbeanTestUtils.createTestServer(
            PassThroughScopedTransactionFactoryTest.class.getSimpleName());
    factory = new PassThroughScopedTransactionFactory(server);
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @AfterMethod
  public void cleanup() {
    EbeanTestUtils.shutdownDatabase(server);
  }

  @Test
  public void beginReturnsRealTransactionHonouringCommitAndRollback() {
    final String committedUrn = "urn:li:corpuser:passThroughCommitted";
    final String rolledBackUrn = "urn:li:corpuser:passThroughRolledBack";

    try (Transaction tx = factory.begin(opContext, TxScope.requiresNew())) {
      assertSame(
          server.currentTransaction(),
          tx,
          "begin() must put a real transaction in the thread scope, which is what the DAO's "
              + "transaction-scoped locks depend on");
      server.insert(newRow(committedUrn, "committed"), tx);
      tx.commit();
    }
    assertNotNull(findRow(committedUrn), "commit on the returned transaction must persist the row");

    try (Transaction tx = factory.begin(opContext, TxScope.requiresNew())) {
      server.insert(newRow(rolledBackUrn, "rolled-back"), tx);
      tx.rollback();
    }
    assertNull(findRow(rolledBackUrn), "rollback on the returned transaction must discard the row");
  }

  @Test
  public void scopeCloseHasNoTransactionalSideEffect() {
    assertNull(server.currentTransaction());
    try (ScopedTransactionFactory.Scope scope = factory.scope(opContext)) {
      assertNotNull(scope);
    }
    assertNull(
        server.currentTransaction(), "scope() must not open a transaction when none was active");

    final String urn = "urn:li:corpuser:passThroughScopeOuterTx";
    try (Transaction outer = server.beginTransaction()) {
      server.insert(newRow(urn, "before-scope"), outer);

      try (ScopedTransactionFactory.Scope scope = factory.scope(opContext)) {
        assertNotNull(scope);
      }

      assertSame(
          server.currentTransaction(),
          outer,
          "closing a scope must not detach the caller's transaction");
      // Still usable after the scope closed, so the scope did not end it.
      server.insert(newRow(urn + "2", "after-scope"), outer);
      outer.rollback();
    }
    assertNull(findRow(urn), "scope close must not have committed the caller's transaction");
    assertNull(findRow(urn + "2"));
  }

  @Test
  public void runInScopeReturnsWorkResultAndPropagatesFailure() {
    assertEquals(factory.runInScope(opContext, () -> "work-result"), "work-result");

    // A try-with-resources helper that caught the work's exception to close its scope would leave
    // the write path silently succeeding on a failed operation.
    assertThrows(
        IllegalStateException.class,
        () ->
            factory.runInScope(
                opContext,
                () -> {
                  throw new IllegalStateException("work failed");
                }));
  }

  @Test
  public void inStreamScopeClosesTheStreamAndReturnsConsumerResult() {
    AtomicBoolean closed = new AtomicBoolean(false);

    Long count =
        factory.inStreamScope(
            opContext,
            () -> Stream.of("a", "b", "c").onClose(() -> closed.set(true)),
            stream -> stream.count());

    assertEquals(count.longValue(), 3L);
    assertTrue(closed.get(), "the source stream must be closed when the scope exits");
  }

  private EbeanAspectV2 findRow(String urn) {
    List<EbeanAspectV2> rows =
        server
            .find(EbeanAspectV2.class)
            .where()
            .eq(EbeanAspectV2.URN_COLUMN, urn)
            .eq(EbeanAspectV2.ASPECT_COLUMN, STATUS_ASPECT_NAME)
            .findList();
    return rows.isEmpty() ? null : rows.get(0);
  }

  private static EbeanAspectV2 newRow(String urn, String metadata) {
    EbeanAspectV2 row = new EbeanAspectV2();
    row.setKey(new EbeanAspectV2.PrimaryKey(urn, STATUS_ASPECT_NAME, ASPECT_LATEST_VERSION));
    row.setMetadata(metadata);
    row.setCreatedBy("urn:li:corpuser:test");
    row.setCreatedFor(null);
    row.setCreatedOn(new Timestamp(System.currentTimeMillis()));
    row.setSystemMetadata(null);
    return row;
  }
}
