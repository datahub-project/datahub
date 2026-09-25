package com.linkedin.metadata.entity.ebean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.util.exception.DatabaseTransactionConflictException;
import com.datahub.util.exception.RetryLimitReached;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.TransactionRetryConfiguration;
import com.linkedin.metadata.entity.TransactionContext;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.entity.storage.PrimaryStorageResolver;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.Transaction;
import io.ebean.TxScope;
import jakarta.persistence.PersistenceException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanAspectDaoTransactionRetryTest {

  private static final int MAX_RETRIES = 3;

  private Database mockServer;
  private Transaction mockTransaction;
  private MetricUtils metricUtils;
  private OperationContext opContext;
  private TestableEbeanAspectDao dao;

  @BeforeMethod
  public void setUp() {
    mockServer = mock(Database.class);
    mockTransaction = mock(Transaction.class);
    when(mockServer.beginTransaction(any(TxScope.class))).thenReturn(mockTransaction);

    PrimaryStorageResolver resolver = mock(PrimaryStorageResolver.class);
    when(resolver.resolveEbeanPrimary()).thenReturn(mockServer);

    metricUtils = mock(MetricUtils.class);

    EbeanConfiguration config =
        EbeanConfiguration.builder()
            .transactionRetry(
                TransactionRetryConfiguration.builder()
                    .backoffSqlStates("40001,40P01")
                    .backoffVendorCodes("1213")
                    .initialBackoffMs(50)
                    .maxBackoffMs(1000)
                    .build())
            .build();

    dao = new TestableEbeanAspectDao(resolver, config, metricUtils);
    dao.setConnectionValidated(true);
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @Test
  public void testDeadlockExhaustion_attemptsFourTimes_sleepsThreeTimes_throwsConflict() {
    Function<TransactionContext, TransactionResult<String>> block =
        tx -> {
          throw deadlockPersistenceException("40001", 1213);
        };

    DatabaseTransactionConflictException thrown =
        expectThrows(
            DatabaseTransactionConflictException.class,
            () -> dao.runInTransactionWithRetryUnlocked(opContext, block, null, MAX_RETRIES));

    assertEquals(thrown.getCode(), DatabaseTransactionConflictException.CODE);
    assertEquals(thrown.getSqlState(), "40001");
    assertEquals(thrown.getRetryAfterSeconds(), 1L);
    verify(mockServer, times(4)).beginTransaction(any(TxScope.class));
    assertEquals(dao.getSleepCallCount(), 3);
    assertEquals(dao.getSleepDelays().size(), 3);
  }

  @Test
  public void testDeadlockExhaustion_propagatesConfiguredRetryAfterSeconds() {
    PrimaryStorageResolver resolver = mock(PrimaryStorageResolver.class);
    when(resolver.resolveEbeanPrimary()).thenReturn(mockServer);
    EbeanConfiguration config =
        EbeanConfiguration.builder()
            .transactionRetry(
                TransactionRetryConfiguration.builder()
                    .backoffSqlStates("40001,40P01")
                    .backoffVendorCodes("1213")
                    .retryAfterSeconds(5)
                    .build())
            .build();
    TestableEbeanAspectDao customDao = new TestableEbeanAspectDao(resolver, config, metricUtils);
    customDao.setConnectionValidated(true);

    Function<TransactionContext, TransactionResult<String>> block =
        tx -> {
          throw deadlockPersistenceException("40001", 1213);
        };

    DatabaseTransactionConflictException thrown =
        expectThrows(
            DatabaseTransactionConflictException.class,
            () -> customDao.runInTransactionWithRetryUnlocked(opContext, block, null, MAX_RETRIES));

    assertEquals(thrown.getRetryAfterSeconds(), 5L);
  }

  @Test
  public void testDeadlockExhaustion_recordsBackoffAndExhaustedMetrics() {
    Function<TransactionContext, TransactionResult<String>> block =
        tx -> {
          throw deadlockPersistenceException("40001", 1213);
        };

    expectThrows(
        DatabaseTransactionConflictException.class,
        () -> dao.runInTransactionWithRetryUnlocked(opContext, block, null, MAX_RETRIES));

    // Dropwizard names use this.getClass() — TestableEbeanAspectDao in this suite.
    String txFailed =
        com.codahale.metrics.MetricRegistry.name(TestableEbeanAspectDao.class, "txFailed");
    String txFailedAfterRetries =
        com.codahale.metrics.MetricRegistry.name(
            TestableEbeanAspectDao.class, "txFailedAfterRetries");

    // null batch → path=delete (EntityServiceImpl delete call site)
    verify(metricUtils, times(3))
        .incrementMicrometer(eq("ebean.tx.transient_backoff"), eq(1.0), eq("path"), eq("delete"));
    verify(metricUtils, times(1))
        .incrementMicrometer(eq("ebean.tx.transient_exhausted"), eq(1.0), eq("path"), eq("delete"));
    verify(metricUtils, times(4)).increment(eq(txFailed), eq(1.0));
    verify(metricUtils, times(1)).increment(eq(txFailedAfterRetries), eq(1.0));
  }

  @Test
  public void testDeadlockExhaustion_withBatch_recordsIngestPathMetrics() {
    Function<TransactionContext, TransactionResult<String>> block =
        tx -> {
          throw deadlockPersistenceException("40001", 1213);
        };
    AspectsBatch batch = mock(AspectsBatch.class);

    expectThrows(
        DatabaseTransactionConflictException.class,
        () -> dao.runInTransactionWithRetryUnlocked(opContext, block, batch, MAX_RETRIES));

    verify(metricUtils, times(3))
        .incrementMicrometer(eq("ebean.tx.transient_backoff"), eq(1.0), eq("path"), eq("ingest"));
    verify(metricUtils, times(1))
        .incrementMicrometer(eq("ebean.tx.transient_exhausted"), eq(1.0), eq("path"), eq("ingest"));
  }

  @Test
  public void testDeadlockExhaustion_vendorCodeOnly_throwsConflict() {
    Function<TransactionContext, TransactionResult<String>> block =
        tx -> {
          throw deadlockPersistenceException(null, 1213);
        };

    DatabaseTransactionConflictException thrown =
        expectThrows(
            DatabaseTransactionConflictException.class,
            () -> dao.runInTransactionWithRetryUnlocked(opContext, block, null, MAX_RETRIES));

    assertEquals(thrown.getCode(), DatabaseTransactionConflictException.CODE);
    assertEquals(dao.getSleepCallCount(), 3);
  }

  @Test
  public void testNonDeadlockExhaustion_throwsRetryLimitReached_notConflict() {
    Function<TransactionContext, TransactionResult<String>> block =
        tx -> {
          throw new PersistenceException(new SQLException("integrity violation", "23000", 1062));
        };

    RetryLimitReached thrown =
        expectThrows(
            RetryLimitReached.class,
            () -> dao.runInTransactionWithRetryUnlocked(opContext, block, null, MAX_RETRIES));

    assertTrue(!(thrown instanceof DatabaseTransactionConflictException));
    verify(mockServer, times(4)).beginTransaction(any(TxScope.class));
    assertEquals(dao.getSleepCallCount(), 0);

    String txFailed =
        com.codahale.metrics.MetricRegistry.name(TestableEbeanAspectDao.class, "txFailed");
    String txFailedAfterRetries =
        com.codahale.metrics.MetricRegistry.name(
            TestableEbeanAspectDao.class, "txFailedAfterRetries");

    verify(metricUtils, times(4)).increment(eq(txFailed), eq(1.0));
    verify(metricUtils, times(1)).increment(eq(txFailedAfterRetries), eq(1.0));
    verify(metricUtils, times(0))
        .incrementMicrometer(eq("ebean.tx.transient_backoff"), eq(1.0), eq("path"), eq("delete"));
    verify(metricUtils, times(0))
        .incrementMicrometer(eq("ebean.tx.transient_exhausted"), eq(1.0), eq("path"), eq("delete"));
  }

  private static PersistenceException deadlockPersistenceException(
      String sqlState, int vendorCode) {
    return new PersistenceException(new SQLException("deadlock", sqlState, vendorCode));
  }

  /** Records sleep calls without delaying tests. */
  private static final class TestableEbeanAspectDao extends EbeanAspectDao {
    private final List<Long> sleepDelays = new ArrayList<>();

    TestableEbeanAspectDao(
        PrimaryStorageResolver resolver, EbeanConfiguration config, MetricUtils metricUtils) {
      super(
          resolver,
          config,
          metricUtils,
          List.of(),
          null,
          new PlainAspectTableResolver(),
          new PassThroughScopedTransactionFactory(resolver.resolveEbeanPrimary()));
    }

    @Override
    protected void sleepBeforeRetry(long backoffMs) {
      sleepDelays.add(backoffMs);
    }

    int getSleepCallCount() {
      return sleepDelays.size();
    }

    List<Long> getSleepDelays() {
      return sleepDelays;
    }
  }
}
