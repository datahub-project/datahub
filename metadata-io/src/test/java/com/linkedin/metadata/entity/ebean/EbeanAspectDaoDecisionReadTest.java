package com.linkedin.metadata.entity.ebean;

import static com.linkedin.metadata.Constants.CORP_USER_EDITABLE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.test.LoggedSql;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** The reads a ceiling delete decides on: PRIMARY and row-locked in both locking modes (H2). */
public class EbeanAspectDaoDecisionReadTest {
  private static final AtomicInteger SERVER_SEQUENCE = new AtomicInteger();
  private static final String CORP_USER_KEY = "corpUserKey";
  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @DataProvider(name = "lockingModes")
  public Object[][] lockingModes() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "lockingModes")
  public void decisionReadReturnsTheLatestRowAndLocksIt(boolean optimisticLocking) {
    final EbeanAspectDao dao = dao(optimisticLocking);
    final String urn = "urn:li:corpuser:p1-decision-read-" + optimisticLocking;
    storeStatus(dao, urn, 0L, true, "1");

    final AtomicReference<SystemAspect> read = new AtomicReference<>();
    LoggedSql.start();
    dao.runInTransactionWithRetry(
        opContext,
        txContext -> {
          read.set(dao.getLatestAspectForDecision(opContext, urn, STATUS_ASPECT_NAME));
          return TransactionResult.commit("");
        },
        0);
    final List<String> sql = loggedSqlMentioning(urn);

    assertTrue(new Status(read.get().getRecordTemplate().data()).isRemoved());
    assertEquals(read.get().getSystemMetadata().getRunId(), "run-a");
    assertTrue(
        sql.stream().anyMatch(statement -> statement.contains("for update")),
        "the decision read must lock the row, also under optimistic locking; got " + sql);
  }

  /** Why decision reads exist: the existing write-intent read does NOT lock under OL. */
  @Test
  public void writeIntentReadUnderOptimisticLockingTakesNoLockButTheDecisionReadDoes() {
    final EbeanAspectDao dao = dao(true);
    final String urn = "urn:li:corpuser:p1-decision-vs-for-update";
    storeStatus(dao, urn, 0L, false, "1");

    LoggedSql.start();
    dao.runInTransactionWithRetry(
        opContext,
        txContext -> {
          dao.getLatestAspects(opContext, Map.of(urn, Set.of(STATUS_ASPECT_NAME)), true);
          return TransactionResult.commit("");
        },
        0);
    final List<String> forUpdateRead = loggedSqlMentioning(urn);
    LoggedSql.start();
    dao.runInTransactionWithRetry(
        opContext,
        txContext -> {
          dao.getLatestAspectForDecision(opContext, urn, STATUS_ASPECT_NAME);
          return TransactionResult.commit("");
        },
        0);
    final List<String> decisionRead = loggedSqlMentioning(urn);

    assertTrue(
        forUpdateRead.stream().noneMatch(statement -> statement.contains("for update")),
        forUpdateRead.toString());
    assertTrue(
        decisionRead.stream().anyMatch(statement -> statement.contains("for update")),
        decisionRead.toString());
  }

  @Test
  public void decisionReadOfAnAbsentRowIsNull() {
    final EbeanAspectDao dao = dao(false);

    final AtomicReference<SystemAspect> read = new AtomicReference<>();
    dao.runInTransactionWithRetry(
        opContext,
        txContext -> {
          read.set(
              dao.getLatestAspectForDecision(
                  opContext, "urn:li:corpuser:p1-decision-absent", STATUS_ASPECT_NAME));
          return TransactionResult.commit("");
        },
        0);

    assertNull(read.get());
  }

  @Test
  public void readOnlyDaoDecisionReadTakesNoLock() {
    final EbeanAspectDao dao = dao(false);
    final String urn = "urn:li:corpuser:p1-decision-read-only";
    storeStatus(dao, urn, 0L, false, "1");
    dao.setWritable(false);

    LoggedSql.start();
    final SystemAspect read = dao.getLatestAspectForDecision(opContext, urn, STATUS_ASPECT_NAME);
    final List<String> sql = loggedSqlMentioning(urn);

    assertFalse(new Status(read.getRecordTemplate().data()).isRemoved());
    assertTrue(
        sql.stream().noneMatch(statement -> statement.contains("for update")), sql.toString());
  }

  @Test(dataProvider = "lockingModes")
  public void entityDecisionReadLocksEveryLatestRowAndSkipsHistory(boolean optimisticLocking) {
    final EbeanAspectDao dao = dao(optimisticLocking);
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:p1-entity-decision-" + optimisticLocking);
    storeStatus(dao, urn.toString(), 0L, true, "2");
    storeStatus(dao, urn.toString(), 1L, false, "1");
    storeRow(dao, urn.toString(), CORP_USER_KEY, 0L, "{\"username\":\"p1-entity-decision\"}", "1");

    final AtomicReference<Map<String, SystemAspect>> read = new AtomicReference<>();
    LoggedSql.start();
    dao.runInTransactionWithRetry(
        opContext,
        txContext -> {
          read.set(dao.getLatestAspectsForDecision(opContext, urn));
          return TransactionResult.commit("");
        },
        0);
    final List<String> sql = loggedSqlMentioning(urn.toString());

    assertEquals(read.get().keySet(), Set.of(STATUS_ASPECT_NAME, CORP_USER_KEY));
    assertTrue(
        new Status(read.get().get(STATUS_ASPECT_NAME).getRecordTemplate().data()).isRemoved());
    assertTrue(
        sql.stream().anyMatch(statement -> statement.contains("for update")),
        "the entity decision read must lock, also under optimistic locking; got " + sql);
  }

  @Test
  public void versionRangeDeleteRemovesOnlyTheRowsInTheRange() {
    final EbeanAspectDao dao = dao(false);
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:p1-version-range");
    storeStatus(dao, urn.toString(), 0L, true, "4");
    storeStatus(dao, urn.toString(), 1L, false, "1");
    storeStatus(dao, urn.toString(), 2L, true, "2");
    storeStatus(dao, urn.toString(), 3L, false, "3");
    storeRow(
        dao, urn.toString(), CORP_USER_EDITABLE_INFO_ASPECT_NAME, 1L, "{\"aboutMe\":\"a\"}", "1");

    final int deleted = dao.deleteAspectVersionRange(opContext, urn, STATUS_ASPECT_NAME, 1L, 2L);

    assertEquals(deleted, 2);
    assertNotNull(dao.getAspect(opContext, urn.toString(), STATUS_ASPECT_NAME, 0L));
    assertNull(dao.getAspect(opContext, urn.toString(), STATUS_ASPECT_NAME, 1L));
    assertNull(dao.getAspect(opContext, urn.toString(), STATUS_ASPECT_NAME, 2L));
    assertNotNull(dao.getAspect(opContext, urn.toString(), STATUS_ASPECT_NAME, 3L));
    assertNotNull(
        dao.getAspect(opContext, urn.toString(), CORP_USER_EDITABLE_INFO_ASPECT_NAME, 1L));
  }

  private EbeanAspectDao dao(boolean optimisticLocking) {
    final Database server =
        EbeanTestUtils.createTestServer(
            EbeanAspectDaoDecisionReadTest.class.getSimpleName()
                + "_"
                + SERVER_SEQUENCE.incrementAndGet());
    final EbeanAspectDao dao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            mock(MetricUtils.class),
            List.of(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server),
            optimisticLocking);
    dao.setWritable(true);
    return dao;
  }

  private static void storeStatus(
      EbeanAspectDao dao, String urn, long rowVersion, boolean removed, String version) {
    storeRow(
        dao,
        urn,
        STATUS_ASPECT_NAME,
        rowVersion,
        RecordUtils.toJsonString(new Status().setRemoved(removed)),
        version);
  }

  private static void storeRow(
      EbeanAspectDao dao, String urn, String aspect, long rowVersion, String json, String version) {
    dao.getServer()
        .save(
            new EbeanAspectV2(
                urn,
                aspect,
                rowVersion,
                json,
                new Timestamp(1_000L),
                "urn:li:corpuser:datahub",
                null,
                RecordUtils.toJsonString(
                    new SystemMetadata().setRunId("run-a").setVersion(version))));
  }

  /** Snapshot first: LoggedSql is process-global and other suites may append concurrently. */
  private static List<String> loggedSqlMentioning(String marker) {
    return Arrays.stream(LoggedSql.stop().toArray(new String[0]))
        .filter(statement -> statement.contains(marker))
        .map(statement -> statement.toLowerCase(Locale.ROOT))
        .collect(Collectors.toList());
  }
}
