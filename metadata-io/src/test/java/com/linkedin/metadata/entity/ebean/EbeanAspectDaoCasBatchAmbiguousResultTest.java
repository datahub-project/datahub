package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.ConditionalAspectUpdate;
import com.linkedin.metadata.entity.TransactionContext;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.Transaction;
import jakarta.persistence.PersistenceException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.function.Supplier;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Connector/J no longer returns {@link Statement#SUCCESS_NO_INFO} for rewritten MySQL batches, so
 * the runtime latch cannot be exercised against a live container. This test injects a -2 {@code
 * executeBatch} result through the JDBC seam.
 */
public class EbeanAspectDaoCasBatchAmbiguousResultTest {

  private Database server;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    server =
        EbeanTestUtils.createTestServer(
            EbeanAspectDaoCasBatchAmbiguousResultTest.class.getSimpleName());
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @AfterMethod
  public void cleanup() {
    EbeanTestUtils.shutdownDatabase(server);
  }

  @Test
  public void successNoInfoLatchesBatchingOff() throws Exception {
    PreparedStatement ps = mock(PreparedStatement.class);
    when(ps.executeBatch())
        .thenReturn(new int[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO});
    Connection conn = mock(Connection.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);

    Transaction tx = mock(Transaction.class);
    when(tx.isBatchMode()).thenReturn(false);
    when(tx.connection()).thenReturn(conn);

    TransactionContext txContext = mock(TransactionContext.class);
    when(txContext.tx()).thenReturn(tx);

    ScopedTransactionFactory txnFactory = mock(ScopedTransactionFactory.class);
    when(txnFactory.runInScope(any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Supplier<Object> work = invocation.getArgument(1);
              return work.get();
            });

    EbeanAspectDao dao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.builder()
                .optimisticLockingEnabled(true)
                .scopedRetryEnabled(true)
                .optimisticWriteBatchEnabled(true)
                .optimisticWriteBatchMinSize(1)
                .build(),
            mock(MetricUtils.class),
            List.of(),
            null,
            new PlainAspectTableResolver(),
            txnFactory,
            true);
    dao.setConnectionValidated(true);

    assertTrue(dao.isOptimisticWriteBatchEnabled());

    List<ConditionalAspectUpdate> updates =
        List.of(
            new ConditionalAspectUpdate(aspect("urn:li:corpuser:a"), "1"),
            new ConditionalAspectUpdate(aspect("urn:li:corpuser:b"), "1"));

    expectThrows(
        PersistenceException.class,
        () -> dao.updateAspectsConditionalBatch(opContext, txContext, updates));

    assertFalse(
        dao.isOptimisticWriteBatchEnabled(),
        "SUCCESS_NO_INFO must latch CAS batching off for this process");
  }

  private SystemAspect aspect(String urn) {
    return new EbeanSystemAspect(
        null,
        UrnUtils.getUrn(urn),
        STATUS_ASPECT_NAME,
        opContext.getEntityRegistry().getEntitySpec(CORP_USER_ENTITY_NAME),
        opContext.getEntityRegistry().getAspectSpecs().get(STATUS_ASPECT_NAME),
        new Status(),
        new SystemMetadata().setVersion("2"),
        AuditStampUtils.createDefaultAuditStamp(),
        null,
        null,
        null);
  }
}
