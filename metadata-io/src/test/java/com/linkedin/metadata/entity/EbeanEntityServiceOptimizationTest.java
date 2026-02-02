package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.entity.EntityServiceTest.TEST_AUDIT_STAMP;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.service.UpdateIndicesService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.test.LoggedSql;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EbeanEntityServiceOptimizationTest {
  /*
   Counts for ORM optimization calculations
  */
  // Default Aspect Generation Step
  // 1. *Key,
  // 2. browsePathsV2 & dataPlatformInstance
  // 3. dataPlatformInfo
  private static final int defaultAspectsGeneration = 3;
  // Next Version Calculation
  // 1. *Key
  // 2. browsePathsV2 & dataPlatformInstance
  private static final int defaultAspectsNextVersion = 2;
  // Final default select
  private static final int nonExistingBaseCount =
      defaultAspectsGeneration + defaultAspectsNextVersion;

  // Existing
  // 1. *Key
  private static final int existingDefaultAspectsGeneration = 1;
  // Retention lookup (disabled for test)
  // 1. dataHubRetentionConfig (if enabled add 1 for read)
  private static final int existingRetention = 0;
  // Final default select existing
  private static final int existingBaseCount = existingDefaultAspectsGeneration + existingRetention;

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private EntityServiceImpl entityService;
  private Database server;

  @BeforeMethod
  public void setupTest() {
    server =
        EbeanTestUtils.createTestServer(EbeanEntityServiceOptimizationTest.class.getSimpleName());

    AspectDao aspectDao =
        new EbeanAspectDao(server, EbeanConfiguration.testDefault, null, List.of(), null);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    entityService =
        new EntityServiceImpl(aspectDao, mock(EventProducer.class), false, preProcessHooks, true);
    entityService.setUpdateIndicesService(mock(UpdateIndicesService.class));
    entityService.setRetentionService(null);
  }

  @Test
  public void testEmptyORMOptimization() {
    // empty batch
    assertSQL(
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .build(opContext),
        0,
        0,
        0,
        "empty",
        "");
  }

  @Test
  public void testUpsertOptimization() {
    Urn testUrn1 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:opt,testOptimization,PROD)");
    final String mustInclude = "urn:li:dataPlatform:opt";

    // single insert (non-existing)
    assertSQL(
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .one(
                ChangeItemImpl.builder()
                    .urn(testUrn1)
                    .aspectName(STATUS_ASPECT_NAME)
                    .recordTemplate(new Status().setRemoved(false))
                    .changeType(ChangeType.UPSERT)
                    .auditStamp(TEST_AUDIT_STAMP)
                    .build(opContext.getAspectRetriever()),
                opContext.getRetrieverContext())
            .build(opContext),
        nonExistingBaseCount + 1,
        1,
        0,
        "initial: single insert",
        mustInclude);

    // single update (existing from previous - no-op)
    // 1. nextVersion
    // 2. current value
    assertSQL(
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .one(
                ChangeItemImpl.builder()
                    .urn(testUrn1)
                    .aspectName(STATUS_ASPECT_NAME)
                    .recordTemplate(new Status().setRemoved(false))
                    .changeType(ChangeType.UPSERT)
                    .auditStamp(TEST_AUDIT_STAMP)
                    .build(opContext.getAspectRetriever()),
                opContext.getRetrieverContext())
            .build(opContext),
        existingBaseCount + 2,
        0,
        1,
        "existing: single no-op",
        mustInclude);

    // multiple (existing from previous - multiple no-ops)
    assertSQL(
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                List.of(
                    ChangeItemImpl.builder()
                        .urn(testUrn1)
                        .aspectName(STATUS_ASPECT_NAME)
                        .recordTemplate(new Status().setRemoved(false))
                        .changeType(ChangeType.UPSERT)
                        .auditStamp(TEST_AUDIT_STAMP)
                        .build(opContext.getAspectRetriever()),
                    ChangeItemImpl.builder()
                        .urn(testUrn1)
                        .aspectName(STATUS_ASPECT_NAME)
                        .recordTemplate(new Status().setRemoved(false))
                        .changeType(ChangeType.UPSERT)
                        .auditStamp(TEST_AUDIT_STAMP)
                        .build(opContext.getAspectRetriever())))
            .build(opContext),
        existingBaseCount + 2,
        0,
        1,
        "existing: multiple no-ops. expected no additional interactions vs single no-op",
        mustInclude);

    // single update (existing from previous - with actual change)
    // 1. nextVersion
    // 2. current value
    assertSQL(
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .one(
                ChangeItemImpl.builder()
                    .urn(testUrn1)
                    .aspectName(STATUS_ASPECT_NAME)
                    .recordTemplate(new Status().setRemoved(true))
                    .changeType(ChangeType.UPSERT)
                    .auditStamp(TEST_AUDIT_STAMP)
                    .build(opContext.getAspectRetriever()),
                opContext.getRetrieverContext())
            .build(opContext),
        existingBaseCount + 2,
        1,
        1,
        "existing: single change",
        mustInclude);

    // multiple update (existing from previous - with 2 actual changes)
    // 1. nextVersion
    // 2. current value
    assertSQL(
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                List.of(
                    ChangeItemImpl.builder()
                        .urn(testUrn1)
                        .aspectName(STATUS_ASPECT_NAME)
                        .recordTemplate(new Status().setRemoved(false))
                        .changeType(ChangeType.UPSERT)
                        .auditStamp(TEST_AUDIT_STAMP)
                        .build(opContext.getAspectRetriever()),
                    ChangeItemImpl.builder()
                        .urn(testUrn1)
                        .aspectName(STATUS_ASPECT_NAME)
                        .recordTemplate(new Status().setRemoved(true))
                        .changeType(ChangeType.UPSERT)
                        .auditStamp(TEST_AUDIT_STAMP)
                        .build(opContext.getAspectRetriever())))
            .build(opContext),
        existingBaseCount + 2,
        1,
        1,
        "existing: multiple change. expected no additional statements over single change",
        mustInclude);
  }

  private void assertSQL(
      @Nonnull AspectsBatch batch,
      int expectedSelectCount,
      int expectedInsertCount,
      int expectedUpdateCount,
      @Nullable String description,
      @Nonnull String mustInclude) {

    // Clear any existing logged statements
    LoggedSql.stop();

    // Start fresh logging
    LoggedSql.start();

    try {
      entityService.ingestProposal(opContext, batch, false);
      // First collect all SQL statements that start with "txn[]"
      List<String> allSqlStatements = new ArrayList<>();
      for (String sqlGroup : new ArrayList<>(LoggedSql.collect())) {
        // Split by "txn[]" but preserve the prefix
        String[] parts = sqlGroup.split("(?=txn\\[\\])");
        for (String part : parts) {
          if (part.startsWith("txn[]")) {
            allSqlStatements.add(part);
          }
        }
      }

      // Then process them to fold comments into previous lines
      List<String> txnLog = new ArrayList<>();
      for (String sql : allSqlStatements) {
        if (sql.startsWith("txn[]  -- ") && !txnLog.isEmpty()) {
          // Append this comment to the previous statement
          int lastIndex = txnLog.size() - 1;
          String current = txnLog.get(lastIndex);
          txnLog.set(lastIndex, current + "\n" + sql);
        } else {
          // Add as a new statement
          txnLog.add(sql);
        }
      }
      // Get the captured SQL statements
      Map<String, List<String>> statementMap =
          txnLog.stream()
              .filter(sql -> sql.contains(mustInclude))
              .collect(
                  Collectors.groupingBy(
                      sql -> {
                        if (sql.startsWith("txn[] insert")) {
                          return "INSERT";
                        } else if (sql.startsWith("txn[] select")) {
                          return "SELECT";
                        } else if (sql.startsWith("txn[] update")) {
                          return "UPDATE";
                        } else {
                          return "UNKNOWN";
                        }
                      }));

      assertEquals(
          statementMap.getOrDefault("UNKNOWN", List.of()).size(),
          0,
          String.format(
              "(%s) Expected all SQL statements to be categorized:\n%s",
              description, formatUnknownStatements(statementMap.get("UNKNOWN"))));
      assertEquals(
          statementMap.getOrDefault("SELECT", List.of()).size(),
          expectedSelectCount,
          String.format(
              "(%s) Expected SELECT SQL count mismatch filtering for (%s):\n%s",
              description, mustInclude, formatUnknownStatements(statementMap.get("SELECT"))));
      assertEquals(
          statementMap.getOrDefault("INSERT", List.of()).size(),
          expectedInsertCount,
          String.format(
              "(%s) Expected INSERT SQL count mismatch filtering for (%s):\n%s",
              description, mustInclude, formatUnknownStatements(statementMap.get("INSERT"))));
      assertEquals(
          statementMap.getOrDefault("UPDATE", List.of()).size(),
          expectedUpdateCount,
          String.format(
              "(%s), Expected UPDATE SQL count mismatch filtering for (%s):\n%s",
              description, mustInclude, formatUnknownStatements(statementMap.get("UPDATE"))));
    } finally {
      // Ensure logging is stopped even if assertions fail
      LoggedSql.stop();
    }
  }

  private static String formatUnknownStatements(List<String> statements) {
    if (statements == null || statements.isEmpty()) {
      return "  No unknown statements";
    }

    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < statements.size(); i++) {
      builder.append("  ").append(i + 1).append(". ").append(statements.get(i)).append("\n");
    }
    return builder.toString();
  }

  @AfterMethod
  public void cleanup() {
    // Shutdown Database instance to prevent thread pool and connection leaks
    // This includes the "gma.heartBeat" thread and connection pools
    EbeanTestUtils.shutdownDatabase(server);
  }
}
