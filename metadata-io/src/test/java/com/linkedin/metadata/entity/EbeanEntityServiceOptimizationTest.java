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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

// single threaded to prevent sql logging collisions
@Test(singleThreaded = true)
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

  @BeforeMethod
  public void setupTest() {
    Database server =
        EbeanTestUtils.createTestServer(EbeanEntityServiceOptimizationTest.class.getSimpleName());
    AspectDao aspectDao = new EbeanAspectDao(server, EbeanConfiguration.testDefault);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    entityService =
        new EntityServiceImpl(aspectDao, mock(EventProducer.class), false, preProcessHooks, true);
    entityService.setUpdateIndicesService(mock(UpdateIndicesService.class));
  }

  @Test
  public void testEmptyORMOptimization() {
    // empty batch
    assertSQL(
        AspectsBatchImpl.builder().retrieverContext(opContext.getRetrieverContext()).build(),
        0,
        0,
        0,
        "empty");
  }

  @Test
  public void testUpsertOptimization() {
    Urn testUrn1 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,testUpsertOptimization,PROD)");

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
            .build(),
        nonExistingBaseCount + 1,
        1,
        0,
        "initial: single insert");

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
            .build(),
        existingBaseCount + 2,
        0,
        1,
        "existing: single no-op");

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
            .build(),
        existingBaseCount + 2,
        0,
        1,
        "existing: multiple no-ops. expected no additional interactions vs single no-op");

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
            .build(),
        existingBaseCount + 2,
        1,
        1,
        "existing: single change");

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
            .build(),
        existingBaseCount + 2,
        1,
        1,
        "existing: multiple change. expected no additional statements over single change");
  }

  private void assertSQL(
      @Nonnull AspectsBatch batch,
      int expectedSelectCount,
      int expectedInsertCount,
      int expectedUpdateCount,
      @Nullable String description) {

    // Clear any existing logged statements
    LoggedSql.stop();

    // Start fresh logging
    LoggedSql.start();

    try {
      entityService.ingestProposal(opContext, batch, false);
      // Get the captured SQL statements
      Map<String, List<String>> statementMap =
          LoggedSql.stop().stream()
              // only consider transaction statements
              .filter(sql -> sql.startsWith("txn[]") && !sql.startsWith("txn[]  -- "))
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
              "(%s) Expected all SQL statements to be categorized: %s",
              description, statementMap.get("UNKNOWN")));
      assertEquals(
          statementMap.getOrDefault("SELECT", List.of()).size(),
          expectedSelectCount,
          String.format(
              "(%s) Expected SELECT SQL count mismatch: %s",
              description, statementMap.get("SELECT")));
      assertEquals(
          statementMap.getOrDefault("INSERT", List.of()).size(),
          expectedInsertCount,
          String.format(
              "(%s) Expected INSERT SQL count mismatch: %s",
              description, statementMap.get("INSERT")));
      assertEquals(
          statementMap.getOrDefault("UPDATE", List.of()).size(),
          expectedUpdateCount,
          String.format(
              "(%s), Expected UPDATE SQL count mismatch: %s",
              description, statementMap.get("UPDATE")));
    } finally {
      // Ensure logging is stopped even if assertions fail
      LoggedSql.stop();
    }
  }
}
