package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYNC_INDEX_UPDATE_HEADER_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticSourceTokenMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.hooks.SemanticRefreshMarkerHook;
import com.linkedin.metadata.aspect.hooks.SemanticRefreshSideEffect;
import com.linkedin.metadata.aspect.patch.builder.SemanticContentPatchBuilder;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticDocumentProvenance;
import com.linkedin.metadata.search.semantic.profile.ConfiguredSemanticProfileRegistry;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRefresh;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A source edit marks embedding profiles stale through the post-commit marker patch, which is
 * replayed here the way the MCE consumer ingests it.
 */
public class SemanticRefreshEndToEndTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:document:refreshed");
  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:datahub");
  private static final List<String> OPERATIONS =
      List.of("CREATE", "CREATE_ENTITY", "UPSERT", "UPDATE", "PATCH", "DELETE");

  private Database server;
  private EntityServiceImpl entityService;
  private UpdateIndicesService updateIndicesService;
  private OperationContext opContext;
  private final List<AspectsBatch> deferred = new ArrayList<>();

  @BeforeMethod
  public void setup() {
    deferred.clear();
    server = EbeanTestUtils.createTestServer(getClass().getSimpleName());
    EbeanAspectDao aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server));
    EventProducer producer = mock(EventProducer.class);
    when(producer.produceMetadataChangeLog(any(OperationContext.class), any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    entityService =
        spy(
            new EntityServiceImpl(
                aspectDao,
                producer,
                new PreProcessHooks(),
                new EntityServiceConfiguration().setAlwaysEmitChangeLog(false),
                mock(MetricUtils.class)));
    updateIndicesService = mock(UpdateIndicesService.class);
    entityService.setUpdateIndicesService(updateIndicesService);
    doAnswer(
            invocation -> {
              deferred.add(invocation.getArgument(1));
              return Stream.empty();
            })
        .when(entityService)
        .ingestProposalAsync(any(OperationContext.class), any(AspectsBatch.class));

    EntityRegistry registry = withSemanticRefresh(TestOperationContexts.defaultEntityRegistry());
    opContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> registry,
            () ->
                RetrieverContext.builder()
                    .aspectRetriever(
                        EntityServiceAspectRetriever.builder()
                            .entityService(entityService)
                            .entityRegistry(registry)
                            .build())
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(() -> registry))
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .build(),
            null,
            context ->
                ((EntityServiceAspectRetriever) context.getAspectRetriever())
                    .setSystemOperationContext(context),
            null);
  }

  @Test
  public void testATextEditMarksTheProfileStaleOnceTheMarkerPatchLands() {
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    write(
        SEMANTIC_CONTENT_ASPECT_NAME,
        profiles(SemanticDocumentProvenance.sha256Hex("body")),
        1_500L);
    assertTrue(storedProfile().isCurrent());
    deferred.clear();

    write(DOCUMENT_INFO_ASPECT_NAME, document("edited", "title"), 2_000L);

    assertEquals(deferred.size(), 1);
    assertTrue(storedProfile().isCurrent());
    entityService.ingestProposal(opContext, deferred.get(0), false);

    SemanticContent stored = storedContent();
    SemanticSourceToken token =
        stored
            .getRefreshMarker()
            .getSources()
            .get(SemanticProfileRefresh.watchKey(DOCUMENT_INFO_ASPECT_NAME, "contents.text"));
    assertEquals(token.getCreatedTime(), Long.valueOf(1_000L));
    assertEquals(token.getVersion(), Long.valueOf(2L));
    assertFalse(stored.getEmbeddings().get("documentText").isCurrent());
  }

  @Test
  public void testVectorsWrittenWithTheTokenOfTheTextTheyWereBuiltFromStayCurrentUntilAnEdit() {
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    deferred.clear();
    write(SEMANTIC_CONTENT_ASPECT_NAME, profilesRead("body", textToken(1_000L, 1L)), 1_500L);
    assertTrue(storedProfile().isCurrent());

    write(DOCUMENT_INFO_ASPECT_NAME, document("edited", "title"), 2_000L);
    entityService.ingestProposal(opContext, deferred.get(0), false);

    assertFalse(storedProfile().isCurrent());
  }

  @Test
  public void testVectorsBuiltFromAnEditStayCurrentWhenTheyLandBeforeTheEditsMarkerPatch() {
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    write(SEMANTIC_CONTENT_ASPECT_NAME, profilesRead("body", textToken(1_000L, 1L)), 1_500L);
    deferred.clear();
    write(DOCUMENT_INFO_ASPECT_NAME, document("edited", "title"), 2_000L);
    assertEquals(deferred.size(), 1);

    write(SEMANTIC_CONTENT_ASPECT_NAME, profilesRead("edited", textToken(1_000L, 2L)), 2_500L);
    entityService.ingestProposal(opContext, deferred.get(0), false);

    assertTrue(storedProfile().isCurrent());
  }

  @Test
  public void testAMarkerPatchForAMissingAspectCreatesNothing() {
    Urn absent = UrnUtils.getUrn("urn:li:document:absent");
    MetadataChangeProposal marker =
        new SemanticContentPatchBuilder()
            .urn(absent)
            .setRefreshSource(
                SemanticProfileRefresh.watchKey(DOCUMENT_INFO_ASPECT_NAME, "contents.text"),
                new SemanticSourceToken().setCreatedTime(1L).setVersion(1L))
            .ifAspectExists()
            .build();

    entityService.ingestProposal(
        opContext,
        AspectsBatchImpl.builder()
            .mcps(
                List.of(marker),
                new AuditStamp().setTime(1L).setActor(ACTOR),
                opContext.getRetrieverContext())
            .build(opContext),
        false);

    assertNull(entityService.getLatestAspect(opContext, absent, SEMANTIC_CONTENT_ASPECT_NAME));
    assertFalse(entityService.exists(opContext, absent, true));
  }

  @Test
  public void testATitleEditLeavesTheProfileCurrent() {
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    write(
        SEMANTIC_CONTENT_ASPECT_NAME,
        profiles(SemanticDocumentProvenance.sha256Hex("body")),
        1_500L);
    deferred.clear();

    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "renamed"), 2_000L);

    assertTrue(deferred.isEmpty());
    assertTrue(storedProfile().isCurrent());
  }

  @Test
  public void testASynchronousSourceWriteAppliesItsMarkerInTheRequest() {
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    write(
        SEMANTIC_CONTENT_ASPECT_NAME,
        profiles(SemanticDocumentProvenance.sha256Hex("body")),
        1_500L);
    deferred.clear();

    writeSynchronously(DOCUMENT_INFO_ASPECT_NAME, document("edited", "title"), 2_000L);

    assertTrue(
        deferred.stream()
            .flatMap(batch -> batch.getItems().stream())
            .noneMatch(item -> SEMANTIC_CONTENT_ASPECT_NAME.equals(item.getAspectName())),
        "a synchronous source write must not defer its semanticContent marker");
    assertEquals(textMarkerToken().getVersion(), Long.valueOf(2L));
    assertFalse(storedProfile().isCurrent());
  }

  @Test
  public void testAFailedMarkerProduceIsWrittenSynchronously() {
    MetricUtils metrics = mock(MetricUtils.class);
    opContext = withMetrics(metrics);
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    write(
        SEMANTIC_CONTENT_ASPECT_NAME,
        profiles(SemanticDocumentProvenance.sha256Hex("body")),
        1_500L);
    doThrow(new RuntimeException("produce failed"))
        .when(entityService)
        .ingestProposalAsync(any(OperationContext.class), any(AspectsBatch.class));

    write(DOCUMENT_INFO_ASPECT_NAME, document("edited", "title"), 2_000L);

    SemanticSourceToken token = textMarkerToken();
    assertEquals(token.getCreatedTime(), Long.valueOf(1_000L));
    assertEquals(token.getVersion(), Long.valueOf(2L));
    assertFalse(storedProfile().isCurrent());
    verify(metrics).increment(EntityServiceImpl.class, "semanticRefreshMarker.syncFallback", 1);
    verify(metrics, never())
        .increment(EntityServiceImpl.class, "semanticRefreshMarker.syncFallbackFailed", 1);
  }

  @Test
  public void testAFailedSyncFallbackFailsTheSourceWriteAndLeavesTheMarkerAsItWas() {
    MetricUtils metrics = mock(MetricUtils.class);
    opContext = withMetrics(metrics);
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    write(
        SEMANTIC_CONTENT_ASPECT_NAME,
        profiles(SemanticDocumentProvenance.sha256Hex("body")),
        1_500L);
    write(DOCUMENT_INFO_ASPECT_NAME, document("edited", "title"), 2_000L);
    entityService.ingestProposal(opContext, deferred.get(0), false);
    assertEquals(textMarkerToken().getVersion(), Long.valueOf(2L));

    doThrow(new RuntimeException("produce failed"))
        .when(entityService)
        .ingestProposalAsync(any(OperationContext.class), any(AspectsBatch.class));
    doAnswer(
            invocation -> {
              AspectsBatch batch = invocation.getArgument(1);
              if (batch.getItems().stream()
                  .anyMatch(item -> SEMANTIC_CONTENT_ASPECT_NAME.equals(item.getAspectName()))) {
                throw new RuntimeException("sync write failed");
              }
              return invocation.callRealMethod();
            })
        .when(entityService)
        .ingestProposalSync(any(OperationContext.class), any(AspectsBatch.class));

    assertThrows(
        RuntimeException.class,
        () -> write(DOCUMENT_INFO_ASPECT_NAME, document("edited again", "title"), 3_000L));

    assertEquals(textMarkerToken().getVersion(), Long.valueOf(2L));
    verify(metrics).increment(EntityServiceImpl.class, "semanticRefreshMarker.syncFallback", 1);
    verify(metrics)
        .increment(EntityServiceImpl.class, "semanticRefreshMarker.syncFallbackFailed", 1);
  }

  @Test
  public void testARollbackOfAWatchedEditRestoresTheOldTextAndUnmarksProfilesOfTheEdit() {
    // Rollback restores the previous row from version history, which is kept only when a
    // retention policy allows more than one version.
    RetentionService<ChangeItemImpl> retention = mock(RetentionService.class);
    when(retention.getMaxVersionsToKeepForWrite(any(), any(), any())).thenReturn(20);
    entityService.setRetentionService(retention);
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    write(
        SEMANTIC_CONTENT_ASPECT_NAME,
        profiles(SemanticDocumentProvenance.sha256Hex("body")),
        1_500L);
    writeInRun(DOCUMENT_INFO_ASPECT_NAME, document("edited", "title"), 2_000L, "run-1");
    entityService.ingestProposal(opContext, deferred.get(0), false);
    deferred.clear();
    write(SEMANTIC_CONTENT_ASPECT_NAME, profilesRead("edited", textToken(1_000L, 2L)), 2_500L);
    assertTrue(storedProfile().isCurrent());

    RollbackResult result =
        entityService.deleteAspectWithoutMCL(
            opContext,
            URN.toString(),
            DOCUMENT_INFO_ASPECT_NAME,
            Map.of("runId", "run-1"),
            false,
            EntityServiceImpl.DeletePurpose.ROLLBACK);

    assertFalse(result.isNoOp());
    assertEquals(textOf(result.getOldValue()), "edited");
    assertEquals(textOf(result.getNewValue()), "body");
    long discardedVersion = Long.parseLong(result.getOldSystemMetadata().getVersion());
    assertEquals(Long.parseLong(result.getNewSystemMetadata().getVersion()), discardedVersion + 1L);

    assertEquals(deferred.size(), 1);
    entityService.ingestProposal(opContext, deferred.get(0), false);
    SemanticSourceToken token = textMarkerToken();
    assertEquals(token.getCreatedTime(), Long.valueOf(1_000L));
    assertEquals(token.getVersion(), Long.valueOf(discardedVersion + 1L));
    assertFalse(storedProfile().isCurrent());
  }

  @Test
  public void testARollbackOfAnEditMadeAfterLegacyRowsKeepsTheRestoredTokenAfterTheEdit() {
    // Rows written before aspectCreated existed have no creation stamp; a read takes the row's
    // createdOn. The edit's token is therefore its row's createdOn (2000), and the history row
    // it displaced keeps the earlier one (1000). A rollback that restored the history row's
    // creation time would order the restored token before the edit's.
    RetentionService<ChangeItemImpl> retention = mock(RetentionService.class);
    when(retention.getMaxVersionsToKeepForWrite(any(), any(), any())).thenReturn(20);
    entityService.setRetentionService(retention);
    write(DOCUMENT_INFO_ASPECT_NAME, document("body", "title"), 1_000L);
    writeInRun(DOCUMENT_INFO_ASPECT_NAME, document("edited", "title"), 2_000L, "run-1");
    deferred.clear();
    stripAspectCreated(DOCUMENT_INFO_ASPECT_NAME);
    write(SEMANTIC_CONTENT_ASPECT_NAME, profilesMarkedAt("edited", textToken(2_000L, 2L)), 2_500L);
    assertTrue(storedProfile().isCurrent());

    RollbackResult result =
        entityService.deleteAspectWithoutMCL(
            opContext,
            URN.toString(),
            DOCUMENT_INFO_ASPECT_NAME,
            Map.of("runId", "run-1"),
            false,
            EntityServiceImpl.DeletePurpose.ROLLBACK);

    assertEquals(textOf(result.getNewValue()), "body");
    long discardedVersion = Long.parseLong(result.getOldSystemMetadata().getVersion());
    assertEquals(deferred.size(), 1);
    entityService.ingestProposal(opContext, deferred.get(0), false);
    deferred.clear();
    assertEquals(textMarkerToken().getCreatedTime(), Long.valueOf(2_000L));
    assertEquals(textMarkerToken().getVersion(), Long.valueOf(discardedVersion + 1L));
    assertFalse(storedProfile().isCurrent());

    write(DOCUMENT_INFO_ASPECT_NAME, document("edited again", "title"), 3_000L);
    entityService.ingestProposal(opContext, deferred.get(0), false);
    assertEquals(textMarkerToken().getCreatedTime(), Long.valueOf(2_000L));
    assertEquals(textMarkerToken().getVersion(), Long.valueOf(discardedVersion + 2L));
    write(
        SEMANTIC_CONTENT_ASPECT_NAME,
        profilesRead("edited again", textToken(2_000L, discardedVersion + 2L)),
        3_500L);
    assertTrue(storedProfile().isCurrent());
  }

  /** Drops aspectCreated from every stored row of the aspect, as a pre-upgrade row has it. */
  private void stripAspectCreated(String aspectName) {
    List<EbeanAspectV2> rows =
        server
            .find(EbeanAspectV2.class)
            .where()
            .eq("key.urn", URN.toString())
            .eq("key.aspect", aspectName)
            .findList();
    for (EbeanAspectV2 row : rows) {
      SystemMetadata systemMetadata =
          RecordUtils.toRecordTemplate(SystemMetadata.class, row.getSystemMetadata());
      SystemMetadataUtils.stripAspectCreated(systemMetadata);
      row.setSystemMetadata(RecordUtils.toJsonString(systemMetadata));
      server.update(row);
    }
  }

  private static SemanticContent profilesMarkedAt(String text, SemanticSourceToken textToken) {
    SemanticContent content = profilesRead(text, textToken);
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(
        SemanticProfileRefresh.watchKey(DOCUMENT_INFO_ASPECT_NAME, "contents.text"), textToken);
    content.setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
    return content;
  }

  private static String textOf(RecordTemplate document) {
    return ((DocumentInfo) document).getContents().getText();
  }

  private OperationContext withMetrics(MetricUtils metrics) {
    return opContext.toBuilder()
        .systemTelemetryContext(
            SystemTelemetryContext.builder()
                .metricUtils(metrics)
                .tracer(OpenTelemetry.noop().getTracer("test"))
                .build())
        .build(opContext.getSystemActorContext().getAuthentication(), false);
  }

  private SemanticSourceToken textMarkerToken() {
    return storedContent()
        .getRefreshMarker()
        .getSources()
        .get(SemanticProfileRefresh.watchKey(DOCUMENT_INFO_ASPECT_NAME, "contents.text"));
  }

  private void write(String aspectName, RecordTemplate record, long time) {
    entityService.ingestProposal(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                List.of(
                    ChangeItemImpl.builder()
                        .urn(URN)
                        .aspectName(aspectName)
                        .recordTemplate(record)
                        .auditStamp(new AuditStamp().setTime(time).setActor(ACTOR))
                        .build(opContext.getAspectRetriever())))
            .build(opContext),
        false);
  }

  private void writeSynchronously(String aspectName, RecordTemplate record, long time) {
    entityService.ingestProposal(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                List.of(
                    ChangeItemImpl.builder()
                        .urn(URN)
                        .aspectName(aspectName)
                        .recordTemplate(record)
                        .auditStamp(new AuditStamp().setTime(time).setActor(ACTOR))
                        .headers(Map.of(SYNC_INDEX_UPDATE_HEADER_NAME, "true"))
                        .build(opContext.getAspectRetriever())))
            .build(opContext),
        false);
  }

  private void writeInRun(String aspectName, RecordTemplate record, long time, String runId) {
    entityService.ingestProposal(
        opContext,
        AspectsBatchImpl.builder()
            .retrieverContext(opContext.getRetrieverContext())
            .items(
                List.of(
                    ChangeItemImpl.builder()
                        .urn(URN)
                        .aspectName(aspectName)
                        .recordTemplate(record)
                        .auditStamp(new AuditStamp().setTime(time).setActor(ACTOR))
                        .systemMetadata(new SystemMetadata().setRunId(runId))
                        .build(opContext.getAspectRetriever())))
            .build(opContext),
        false);
  }

  private EmbeddingModelData storedProfile() {
    return storedContent().getEmbeddings().get("documentText");
  }

  private SemanticContent storedContent() {
    return (SemanticContent)
        entityService.getLatestAspect(opContext, URN, SEMANTIC_CONTENT_ASPECT_NAME);
  }

  private static EntityRegistry withSemanticRefresh(EntityRegistry base) {
    SemanticProfileRegistry profiles = new ConfiguredSemanticProfileRegistry(null);
    MutationHook hook =
        new SemanticRefreshMarkerHook()
            .setRegistry(profiles)
            .setConfig(config(SemanticRefreshMarkerHook.class, SEMANTIC_CONTENT_ASPECT_NAME));
    MCPSideEffect sideEffect =
        new SemanticRefreshSideEffect()
            .setRegistry(profiles)
            .setConfig(config(SemanticRefreshSideEffect.class, DOCUMENT_INFO_ASPECT_NAME));
    List<MutationHook> hooks = new ArrayList<>(base.getAllMutationHooks());
    hooks.add(hook);
    List<MCPSideEffect> sideEffects = new ArrayList<>(base.getAllMCPSideEffects());
    sideEffects.add(sideEffect);
    EntityRegistry registry = spy(base);
    doReturn(hooks).when(registry).getAllMutationHooks();
    doReturn(sideEffects).when(registry).getAllMCPSideEffects();
    return registry;
  }

  private static AspectPluginConfig config(Class<?> plugin, String aspectName) {
    return AspectPluginConfig.builder()
        .className(plugin.getName())
        .enabled(true)
        .supportedOperations(OPERATIONS)
        .supportedEntityAspectNames(
            List.of(
                AspectPluginConfig.EntityAspectName.builder()
                    .entityName("*")
                    .aspectName(aspectName)
                    .build()))
        .build();
  }

  private static DocumentInfo document(String text, String title) {
    AuditStamp stamp = new AuditStamp().setTime(0L).setActor(ACTOR);
    return new DocumentInfo()
        .setTitle(title)
        .setStatus(new DocumentStatus().setState(DocumentState.PUBLISHED))
        .setContents(new DocumentContents().setText(text))
        .setCreated(stamp)
        .setLastModified(stamp);
  }

  private static SemanticSourceToken textToken(long createdTime, long version) {
    return new SemanticSourceToken().setCreatedTime(createdTime).setVersion(version);
  }

  private static SemanticContent profilesRead(String text, SemanticSourceToken textToken) {
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(
        SemanticProfileRefresh.watchKey(DOCUMENT_INFO_ASPECT_NAME, "contents.text"), textToken);
    SemanticContent content = profiles(SemanticDocumentProvenance.sha256Hex(text));
    content
        .getEmbeddings()
        .get("documentText")
        .setRefreshMarker(new SemanticRefreshMarker().setSources(sources));
    return content;
  }

  private static SemanticContent profiles(String sourceTextSha256) {
    EmbeddingModelDataMap embeddings = new EmbeddingModelDataMap();
    embeddings.put(
        "documentText",
        new EmbeddingModelData()
            .setModelVersion("model")
            .setGeneratedAt(1L)
            .setTotalChunks(0)
            .setChunks(new EmbeddingChunkArray())
            .setSourceTextSha256(sourceTextSha256));
    return new SemanticContent().setEmbeddings(embeddings);
  }
}
