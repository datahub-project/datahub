package com.linkedin.metadata.entity.ebean;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.service.UpdateIndicesService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * End-to-end regression test for the ingest deadlock (RetryLimitReached under concurrent
 * overlapping batches): two threads repeatedly ingest aspect batches over overlapping URN sets
 * through the real {@link EntityServiceImpl} path against PostgreSQL.
 *
 * <p>Before single-wave lock acquisition ({@code AspectDao.lockLatestRows}), each ingest
 * transaction locked rows in multiple statements — {@code exists()} inside {@code
 * withAdditionalChanges}, then {@code getLatestAspects} — so concurrent overlapping batches could
 * interlock crosswise and exhaust the retry budget. With the fix, every batch's locks are acquired
 * in a single up-front statement and concurrent ingests serialize instead of deadlocking. See
 * {@link EbeanAspectDaoConcurrentLockPostgresIT} for the statement-level characterization.
 */
public class EntityServiceConcurrentIngestPostgresIT {

  private static final int TOTAL_URNS = 150;
  private static final int OVERLAP_START = 50; // thread A: [0,100), thread B: [50,150)
  private static final int ROUNDS = 10;

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private PostgreSQLContainer<?> postgres;
  private Database database;
  private EntityServiceImpl entityService;
  private List<Urn> urns;

  @BeforeClass
  public void init() {
    postgres = PostgresTestUtils.startPostgres();
    database =
        PostgresTestUtils.createEbeanPrimaryDatabase(
            postgres,
            PostgresTestUtils.uniqueServerName("concurrent_ingest_it"),
            PostgresTestUtils.newIntegrationNamespace("concurrent_ingest").getSchema());

    EbeanAspectDao aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(database),
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    entityService =
        new EntityServiceImpl(aspectDao, mock(EventProducer.class), false, preProcessHooks, true);
    entityService.setUpdateIndicesService(mock(UpdateIndicesService.class));
    entityService.setRetentionService(null);

    urns = new ArrayList<>(TOTAL_URNS);
    for (int i = 0; i < TOTAL_URNS; i++) {
      urns.add(
          UrnUtils.getUrn(
              String.format(
                  "urn:li:dataset:(urn:li:dataPlatform:concurrent,ingest_it_%d,PROD)", i)));
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    EbeanTestUtils.shutdownDatabase(database);
  }

  @Test(timeOut = 600_000)
  public void concurrentOverlappingIngests_succeedWithoutRetryExhaustion() throws Exception {
    List<Urn> urnsA = urns.subList(0, TOTAL_URNS - OVERLAP_START);
    List<Urn> urnsB = urns.subList(OVERLAP_START, TOTAL_URNS);

    CyclicBarrier barrier = new CyclicBarrier(2);
    AtomicInteger successfulIngests = new AtomicInteger();
    List<Throwable> failures = new ArrayList<>();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> workerA =
          executor.submit(() -> ingestLoop(urnsA, barrier, successfulIngests, failures));
      Future<?> workerB =
          executor.submit(() -> ingestLoop(urnsB, barrier, successfulIngests, failures));
      workerA.get(540, TimeUnit.SECONDS);
      workerB.get(540, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    assertTrue(
        failures.isEmpty(),
        "Concurrent overlapping ingests failed (deadlock/retry exhaustion?): " + failures);
    assertEquals(successfulIngests.get(), 2 * ROUNDS);

    // Sanity: aspects actually landed
    Status status =
        (Status)
            entityService.getLatestAspect(
                opContext, urns.get(OVERLAP_START), "status"); // in both threads' ranges
    assertEquals(status.isRemoved(), Boolean.FALSE);
  }

  private void ingestLoop(
      List<Urn> targetUrns,
      CyclicBarrier barrier,
      AtomicInteger successfulIngests,
      List<Throwable> failures) {
    for (int round = 0; round < ROUNDS; round++) {
      try {
        barrier.await(60, TimeUnit.SECONDS);
      } catch (Exception e) {
        synchronized (failures) {
          failures.add(e);
        }
        return;
      }
      try {
        List<ChangeItemImpl> items = new ArrayList<>(targetUrns.size());
        for (Urn urn : targetUrns) {
          items.add(
              ChangeItemImpl.builder()
                  .urn(urn)
                  .aspectName("status")
                  .recordTemplate(new Status().setRemoved(false))
                  .systemMetadata(AspectGenerationUtils.createSystemMetadata())
                  .auditStamp(AspectGenerationUtils.createAuditStamp())
                  .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null)));
        }
        AspectsBatchImpl batch =
            AspectsBatchImpl.builder()
                .retrieverContext(opContext.getRetrieverContext())
                .items(items)
                .build(opContext);
        entityService.ingestAspects(opContext, batch, true, true);
        successfulIngests.incrementAndGet();
      } catch (Throwable t) {
        synchronized (failures) {
          failures.add(t);
        }
      }
    }
  }
}
