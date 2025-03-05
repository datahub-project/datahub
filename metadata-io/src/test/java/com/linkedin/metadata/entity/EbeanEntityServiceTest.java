package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.entity.ebean.EbeanAspectDao.TX_ISOLATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.DataGenerator;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.ebean.Transaction;
import io.ebean.TxScope;
import jakarta.persistence.EntityNotFoundException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Triple;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A class that knows how to configure {@link EntityServiceTest} to run integration tests against a
 * relational database.
 *
 * <p>This class also contains all the test methods where realities of an underlying storage leak
 * into the {@link EntityServiceImpl} in the form of subtle behavior differences. Ideally that
 * should never happen, and it'd be great to address captured differences.
 */
public class EbeanEntityServiceTest
    extends EntityServiceTest<EbeanAspectDao, EbeanRetentionService> {

  public EbeanEntityServiceTest() throws EntityRegistryException {}

  @BeforeClass
  public void beforeClass() {
    _mockProducer = mock(EventProducer.class);
    _mockUpdateIndicesService = mock(UpdateIndicesService.class);
  }

  @BeforeMethod
  public void setupTest() {
    reset(_mockProducer);
    reset(_mockUpdateIndicesService);

    Database server = EbeanTestUtils.createTestServer(EbeanEntityServiceTest.class.getSimpleName());

    _aspectDao = new EbeanAspectDao(server, EbeanConfiguration.testDefault);

    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    _entityServiceImpl =
        new EntityServiceImpl(_aspectDao, _mockProducer, false, preProcessHooks, true);
    _entityServiceImpl.setUpdateIndicesService(_mockUpdateIndicesService);
    _retentionService = new EbeanRetentionService(_entityServiceImpl, server, 1000);
    _entityServiceImpl.setRetentionService(_retentionService);

    opContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> _testEntityRegistry,
            () ->
                RetrieverContext.builder()
                    .aspectRetriever(
                        EntityServiceAspectRetriever.builder()
                            .entityService(_entityServiceImpl)
                            .entityRegistry(_testEntityRegistry)
                            .build())
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(
                            () -> _testEntityRegistry))
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .build(),
            null,
            opContext ->
                ((EntityServiceAspectRetriever) opContext.getAspectRetriever())
                    .setSystemOperationContext(opContext),
            null);
  }

  @Test
  public void testNoRowsUpdatedErrorHandling() throws Exception {
    // Setup test data
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:testUser");
    SystemMetadata systemMetadata = AspectGenerationUtils.createSystemMetadata();
    CorpUserInfo writeAspect = AspectGenerationUtils.createCorpUserInfo("email@test.com");
    String aspectName = PegasusUtils.getAspectNameFromSchema(writeAspect.schema());

    // Create database and spy on aspectDao
    Database server = EbeanTestUtils.createTestServer(EbeanEntityServiceTest.class.getSimpleName());
    EbeanAspectDao aspectDao = spy(new EbeanAspectDao(server, EbeanConfiguration.testDefault));

    // Prevent actual saves
    EntityAspect mockEntityAspect = mock(EntityAspect.class);
    when(mockEntityAspect.getMetadata()).thenReturn("");
    doReturn(Optional.of(mockEntityAspect)).when(aspectDao).updateAspect(any(), any());
    doReturn(Optional.of(mockEntityAspect)).when(aspectDao).insertAspect(any(), any(), anyLong());

    // Create spied transaction context that throws on commitAndContinue
    AtomicReference<TransactionContext> capturedTxContext = new AtomicReference<>();
    AtomicReference<TransactionResult<?>> capturedResult = new AtomicReference<>();

    doAnswer(
            invocation -> {
              Function<TransactionContext, TransactionResult<?>> block = invocation.getArgument(0);
              Integer maxTransactionRetry = invocation.getArgument(2);

              TransactionContext txContext = spy(TransactionContext.empty(maxTransactionRetry));
              capturedTxContext.set(txContext);

              doThrow(new EntityNotFoundException("No rows updated"))
                  .when(txContext)
                  .commitAndContinue();

              TransactionResult<?> result = block.apply(txContext);
              capturedResult.set(result);
              return result.getResults();
            })
        .when(aspectDao)
        .runInTransactionWithRetry(any(), any(), anyInt());

    // Create the service with our spied dao
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(false);
    EntityServiceImpl entityService =
        new EntityServiceImpl(aspectDao, _mockProducer, false, preProcessHooks, true);

    // Create the test batch
    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn)
                .aspectName(aspectName)
                .recordTemplate(writeAspect)
                .systemMetadata(systemMetadata)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null)));

    AspectsBatchImpl batch =
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build();

    // Execute the test
    List<UpdateAspectResult> results = entityService.ingestAspects(opContext, batch, false, true);

    // Verify results
    assertEquals(results.size(), 0, "Expected no results for rolled back transaction");

    // Verify transaction behavior
    verify(aspectDao).runInTransactionWithRetry(any(), eq(batch), anyInt());
    verify(capturedTxContext.get()).commitAndContinue();

    // Verify the transaction result was a rollback
    TransactionResult<?> result = capturedResult.get();
    assertNotNull(result, "Expected a transaction result");
    assertFalse(result.isCommitOrRollback(), "Expected a rollback result");
  }

  @Override
  @Test
  public void testIngestListLatestAspects() throws AssertionError {

    // TODO: If you're modifying this test - match your changes in sibling implementations.

    // TODO: Move this test into the base class,
    //  If you can find a way for Cassandra and relational databases to share result ordering rules.

    Urn entityUrn1 = UrnUtils.getUrn("urn:li:corpuser:test1");
    Urn entityUrn2 = UrnUtils.getUrn("urn:li:corpuser:test2");
    Urn entityUrn3 = UrnUtils.getUrn("urn:li:corpuser:test3");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata();

    String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserInfo().schema());

    // Ingest CorpUserInfo Aspect #1
    CorpUserInfo writeAspect1 = AspectGenerationUtils.createCorpUserInfo("email@test.com");

    // Ingest CorpUserInfo Aspect #2
    CorpUserInfo writeAspect2 = AspectGenerationUtils.createCorpUserInfo("email2@test.com");

    // Ingest CorpUserInfo Aspect #3
    CorpUserInfo writeAspect3 = AspectGenerationUtils.createCorpUserInfo("email3@test.com");

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null)),
            ChangeItemImpl.builder()
                .urn(entityUrn2)
                .aspectName(aspectName)
                .recordTemplate(writeAspect2)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null)),
            ChangeItemImpl.builder()
                .urn(entityUrn3)
                .aspectName(aspectName)
                .recordTemplate(writeAspect3)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null)));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // List aspects
    ListResult<RecordTemplate> batch1 =
        _entityServiceImpl.listLatestAspects(
            opContext, entityUrn1.getEntityType(), aspectName, 0, 2);

    assertEquals(batch1.getNextStart(), 2);
    assertEquals(batch1.getPageSize(), 2);
    assertEquals(batch1.getTotalCount(), 3);
    assertEquals(batch1.getTotalPageCount(), 2);
    assertEquals(batch1.getValues().size(), 2);
    assertTrue(DataTemplateUtil.areEqual(writeAspect1, batch1.getValues().get(0)));
    assertTrue(DataTemplateUtil.areEqual(writeAspect2, batch1.getValues().get(1)));

    ListResult<RecordTemplate> batch2 =
        _entityServiceImpl.listLatestAspects(
            opContext, entityUrn1.getEntityType(), aspectName, 2, 2);
    assertEquals(batch2.getValues().size(), 1);
    assertTrue(DataTemplateUtil.areEqual(writeAspect3, batch2.getValues().get(0)));
  }

  @Override
  @Test
  public void testIngestListUrns() throws AssertionError {

    // TODO: If you're modifying this test - match your changes in sibling implementations.

    // TODO: Move this test into the base class,
    //  If you can find a way for Cassandra and relational databases to share result ordering rules.

    Urn entityUrn1 = UrnUtils.getUrn("urn:li:corpuser:test1");
    Urn entityUrn2 = UrnUtils.getUrn("urn:li:corpuser:test2");
    Urn entityUrn3 = UrnUtils.getUrn("urn:li:corpuser:test3");

    SystemMetadata metadata1 = AspectGenerationUtils.createSystemMetadata();

    String aspectName = PegasusUtils.getAspectNameFromSchema(new CorpUserKey().schema());

    // Ingest CorpUserInfo Aspect #1
    RecordTemplate writeAspect1 = AspectGenerationUtils.createCorpUserKey(entityUrn1);

    // Ingest CorpUserInfo Aspect #2
    RecordTemplate writeAspect2 = AspectGenerationUtils.createCorpUserKey(entityUrn2);

    // Ingest CorpUserInfo Aspect #3
    RecordTemplate writeAspect3 = AspectGenerationUtils.createCorpUserKey(entityUrn3);

    List<ChangeItemImpl> items =
        List.of(
            ChangeItemImpl.builder()
                .urn(entityUrn1)
                .aspectName(aspectName)
                .recordTemplate(writeAspect1)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null)),
            ChangeItemImpl.builder()
                .urn(entityUrn2)
                .aspectName(aspectName)
                .recordTemplate(writeAspect2)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null)),
            ChangeItemImpl.builder()
                .urn(entityUrn3)
                .aspectName(aspectName)
                .recordTemplate(writeAspect3)
                .systemMetadata(metadata1)
                .auditStamp(TEST_AUDIT_STAMP)
                .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null)));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(items)
            .build(),
        true,
        true);

    // List aspects urns
    ListUrnsResult batch1 =
        _entityServiceImpl.listUrns(opContext, entityUrn1.getEntityType(), 0, 2);

    assertEquals(batch1.getStart().intValue(), 0);
    assertEquals(batch1.getCount().intValue(), 2);
    assertEquals(batch1.getTotal().intValue(), 3);
    assertEquals(batch1.getEntities().size(), 2);
    assertEquals(entityUrn1.toString(), batch1.getEntities().get(0).toString());
    assertEquals(entityUrn2.toString(), batch1.getEntities().get(1).toString());

    ListUrnsResult batch2 =
        _entityServiceImpl.listUrns(opContext, entityUrn1.getEntityType(), 2, 2);

    assertEquals(batch2.getStart().intValue(), 2);
    assertEquals(batch2.getCount().intValue(), 1);
    assertEquals(batch2.getTotal().intValue(), 3);
    assertEquals(batch2.getEntities().size(), 1);
    assertEquals(entityUrn3.toString(), batch2.getEntities().get(0).toString());
  }

  @Override
  @Test
  public void testNestedTransactions() throws AssertionError {
    Database server = _aspectDao.getServer();

    try (Transaction transaction =
        server.beginTransaction(TxScope.requiresNew().setIsolation(TX_ISOLATION))) {
      transaction.setBatchMode(true);
      // Work 1
      try (Transaction transaction2 =
          server.beginTransaction(TxScope.requiresNew().setIsolation(TX_ISOLATION))) {
        transaction2.setBatchMode(true);
        // Work 2
        transaction2.commit();
      }
      transaction.commit();
    } catch (Exception e) {
      System.out.printf("Top level catch %s%n", e);
      e.printStackTrace();
      throw e;
    }
    System.out.println("done");
  }

  @Test
  public void testSystemMetadataDuplicateKey() throws Exception {
    Urn entityUrn = UrnUtils.getUrn("urn:li:corpuser:duplicateKeyTest");
    SystemMetadata systemMetadata = AspectGenerationUtils.createSystemMetadata();
    ChangeItemImpl item =
        ChangeItemImpl.builder()
            .urn(entityUrn)
            .aspectName(STATUS_ASPECT_NAME)
            .recordTemplate(new Status().setRemoved(true))
            .systemMetadata(systemMetadata)
            .auditStamp(TEST_AUDIT_STAMP)
            .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null));
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(List.of(item))
            .build(),
        false,
        true);

    // List aspects urns
    EnvelopedAspect envelopedAspect =
        _entityServiceImpl.getLatestEnvelopedAspect(
            opContext, CORP_USER_ENTITY_NAME, entityUrn, STATUS_ASPECT_NAME);

    assertNotNull(envelopedAspect);
    assertEquals(envelopedAspect.getVersion(), 0L, "Expected version 0");
    assertEquals(
        envelopedAspect.getSystemMetadata().getVersion(),
        "1",
        "Expected version 0 with systemMeta version 1");

    // Corrupt the version 0 systemMeta
    try (Transaction transaction =
        ((EbeanAspectDao) _entityServiceImpl.aspectDao)
            .getServer()
            .beginTransaction(TxScope.requiresNew().setIsolation(TX_ISOLATION))) {
      TransactionContext transactionContext = TransactionContext.empty(transaction, 3);
      _entityServiceImpl.aspectDao.insertAspect(
          transactionContext,
          EntityAspect.EntitySystemAspect.builder()
              .forInsert(
                  EntityAspect.builder()
                      .urn(entityUrn.toString())
                      .aspect(STATUS_ASPECT_NAME)
                      .metadata(RecordUtils.toJsonString(new Status().setRemoved(false)))
                      .createdBy(TEST_AUDIT_STAMP.getActor().toString())
                      .createdOn(new Timestamp(0L))
                      .systemMetadata(RecordUtils.toJsonString(systemMetadata))
                      .build(),
                  opContext.getEntityRegistry()),
          1);
      transaction.commit();
    }

    // Run another update
    _entityServiceImpl.ingestAspects(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                List.of(
                    ChangeItemImpl.builder()
                        .urn(entityUrn)
                        .aspectName(STATUS_ASPECT_NAME)
                        .recordTemplate(new Status().setRemoved(false))
                        .systemMetadata(systemMetadata)
                        .auditStamp(TEST_AUDIT_STAMP)
                        .build(TestOperationContexts.emptyActiveUsersAspectRetriever(null))))
            .build(),
        false,
        true);
    EnvelopedAspect envelopedAspect2 =
        _entityServiceImpl.getLatestEnvelopedAspect(
            opContext, CORP_USER_ENTITY_NAME, entityUrn, STATUS_ASPECT_NAME);

    assertNotNull(envelopedAspect2);
    assertEquals(envelopedAspect2.getVersion(), 0L, "Expected version 0");
    assertEquals(
        envelopedAspect2.getSystemMetadata().getVersion(),
        "3",
        "Expected version 0 with systemMeta version 3 accounting for the the collision");
  }

  @Test
  public void dataGeneratorThreadingTest() {
    DataGenerator dataGenerator = new DataGenerator(opContext, _entityServiceImpl);
    List<String> aspects = List.of("status", "globalTags", "glossaryTerms");
    List<List<MetadataChangeProposal>> testData =
        dataGenerator.generateMCPs("dataset", 25, aspects).collect(Collectors.toList());

    // Expected no duplicates aspects
    List<String> duplicates =
        testData.stream()
            .flatMap(Collection::stream)
            .map(mcp -> Triple.of(mcp.getEntityUrn().toString(), mcp.getAspectName(), 0L))
            .collect(Collectors.groupingBy(Triple::toString))
            .entrySet()
            .stream()
            .filter(e -> e.getValue().size() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    assertEquals(duplicates.size(), 0, duplicates.toString());
  }

  /**
   * This test is designed to detect multi-threading persistence exceptions like duplicate key,
   * exceptions that exceed retry limits or unnecessary versions.
   */
  @Test // ensure same thread as h2
  public void multiThreadingTest() {
    DataGenerator dataGenerator = new DataGenerator(opContext, _entityServiceImpl);
    Database server = ((EbeanAspectDao) _entityServiceImpl.aspectDao).getServer();

    // Add data
    List<String> aspects = List.of("status", "globalTags", "glossaryTerms");
    List<List<MetadataChangeProposal>> testData =
        dataGenerator.generateMCPs("dataset", 25, aspects).collect(Collectors.toList());

    executeThreadingTest(userContext, _entityServiceImpl, testData, 15);

    // Expected aspects
    Set<Triple<String, String, Long>> generatedAspectIds =
        testData.stream()
            .flatMap(Collection::stream)
            .map(mcp -> Triple.of(mcp.getEntityUrn().toString(), mcp.getAspectName(), 0L))
            .collect(Collectors.toSet());

    // Actual inserts
    Set<Triple<String, String, Long>> actualAspectIds =
        server.sqlQuery("select urn, aspect, version from metadata_aspect_v2").findList().stream()
            .map(
                row ->
                    Triple.of(
                        row.getString("urn"), row.getString("aspect"), row.getLong("version")))
            .collect(Collectors.toSet());

    // Assert State
    Set<Triple<String, String, Long>> additions =
        actualAspectIds.stream()
            .filter(id -> !generatedAspectIds.contains(id))
            // Exclude default aspects
            .filter(
                id ->
                    !Set.of("browsePaths", "browsePathsV2", "dataPlatformInstance")
                        .contains(id.getMiddle()))
            .filter(id -> !id.getMiddle().endsWith("Key"))
            .collect(Collectors.toSet());
    assertEquals(
        additions.size(), 0, String.format("Expected no additional aspects. Found: %s", additions));

    Set<Triple<String, String, Long>> missing =
        generatedAspectIds.stream()
            .filter(id -> !actualAspectIds.contains(id))
            .collect(Collectors.toSet());
    assertEquals(
        missing.size(),
        0,
        String.format(
            "Expected all generated aspects to be inserted. Missing Examples: %s",
            missing.stream().limit(10).collect(Collectors.toSet())));
  }

  /**
   * Don't blame multi-threading for what might not be a threading issue. Perform the
   * multi-threading test with 1 thread.
   */
  @Test
  public void singleThreadingTest() {
    DataGenerator dataGenerator = new DataGenerator(opContext, _entityServiceImpl);
    Database server = ((EbeanAspectDao) _entityServiceImpl.aspectDao).getServer();

    // Add data
    List<String> aspects = List.of("status", "globalTags", "glossaryTerms");
    List<List<MetadataChangeProposal>> testData =
        dataGenerator.generateMCPs("dataset", 25, aspects).collect(Collectors.toList());

    executeThreadingTest(userContext, _entityServiceImpl, testData, 1);

    // Expected aspects
    Set<Triple<String, String, Long>> generatedAspectIds =
        testData.stream()
            .flatMap(Collection::stream)
            .map(mcp -> Triple.of(mcp.getEntityUrn().toString(), mcp.getAspectName(), 0L))
            .collect(Collectors.toSet());

    // Actual inserts
    Set<Triple<String, String, Long>> actualAspectIds =
        server.sqlQuery("select urn, aspect, version from metadata_aspect_v2").findList().stream()
            .map(
                row ->
                    Triple.of(
                        row.getString("urn"), row.getString("aspect"), row.getLong("version")))
            .collect(Collectors.toSet());

    // Assert State
    Set<Triple<String, String, Long>> additions =
        actualAspectIds.stream()
            .filter(id -> !generatedAspectIds.contains(id))
            // Exclude default aspects
            .filter(
                id ->
                    !Set.of("browsePaths", "browsePathsV2", "dataPlatformInstance")
                        .contains(id.getMiddle()))
            .filter(id -> !id.getMiddle().endsWith("Key"))
            .collect(Collectors.toSet());
    assertEquals(
        additions.size(), 0, String.format("Expected no additional aspects. Found: %s", additions));

    Set<Triple<String, String, Long>> missing =
        generatedAspectIds.stream()
            .filter(id -> !actualAspectIds.contains(id))
            .collect(Collectors.toSet());
    assertEquals(
        missing.size(),
        0,
        String.format("Expected all generated aspects to be inserted. Missing: %s", missing));
  }

  private static void executeThreadingTest(
      OperationContext operationContext,
      EntityServiceImpl entityService,
      List<List<MetadataChangeProposal>> testData,
      int threadCount) {
    Database server = ((EbeanAspectDao) entityService.aspectDao).getServer();
    server.sqlUpdate("truncate metadata_aspect_v2");

    int count =
        Objects.requireNonNull(
                server.sqlQuery("select count(*) as cnt from metadata_aspect_v2").findOne())
            .getInteger("cnt");
    assertEquals(count, 0, "Expected exactly 0 rows at the start.");

    // Create ingest proposals in parallel, mimic the smoke-test ingestion
    final LinkedBlockingQueue<List<MetadataChangeProposal>> queue =
        new LinkedBlockingQueue<>(threadCount * 2);

    // Spin up workers
    List<Thread> writeThreads =
        IntStream.range(0, threadCount)
            .mapToObj(
                threadId ->
                    new Thread(new MultiThreadTestWorker(operationContext, queue, entityService)))
            .collect(Collectors.toList());
    writeThreads.forEach(Thread::start);

    testData.forEach(
        mcps -> {
          try {
            queue.put(mcps);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });

    // Terminate workers with empty mcp
    IntStream.range(0, threadCount)
        .forEach(
            threadId -> {
              try {
                queue.put(List.of());
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    // Wait for threads to finish
    writeThreads.forEach(
        thread -> {
          try {
            thread.join(10000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static class MultiThreadTestWorker implements Runnable {
    private final OperationContext operationContext;
    private final EntityServiceImpl entityService;
    private final LinkedBlockingQueue<List<MetadataChangeProposal>> queue;

    public MultiThreadTestWorker(
        OperationContext operationContext,
        LinkedBlockingQueue<List<MetadataChangeProposal>> queue,
        EntityServiceImpl entityService) {
      this.operationContext = operationContext;
      this.queue = queue;
      this.entityService = entityService;
    }

    public void run() {
      try {
        while (true) {
          List<MetadataChangeProposal> mcps = queue.take();
          if (mcps.isEmpty()) {
            break;
          }
          final AuditStamp auditStamp = new AuditStamp();
          auditStamp.setActor(Urn.createFromString(Constants.DATAHUB_ACTOR));
          auditStamp.setTime(System.currentTimeMillis());
          AspectsBatchImpl batch =
              AspectsBatchImpl.builder()
                  .mcps(mcps, auditStamp, operationContext.getRetrieverContext())
                  .build();
          entityService.ingestProposal(operationContext, batch, false);
        }
      } catch (InterruptedException | URISyntaxException ie) {
        throw new RuntimeException(ie);
      }
    }
  }
}
