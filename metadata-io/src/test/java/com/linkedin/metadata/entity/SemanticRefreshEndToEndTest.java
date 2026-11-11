package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
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
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticDocumentProvenance;
import com.linkedin.metadata.search.semantic.profile.ConfiguredSemanticProfileRegistry;
import com.linkedin.metadata.search.semantic.profile.SemanticProfileRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.List;
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

  private EntityServiceImpl entityService;
  private OperationContext opContext;
  private final List<AspectsBatch> deferred = new ArrayList<>();

  @BeforeMethod
  public void setup() {
    deferred.clear();
    Database server = EbeanTestUtils.createTestServer(getClass().getSimpleName());
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
    entityService.setUpdateIndicesService(mock(UpdateIndicesService.class));
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
    assertEquals(
        stored.getRefreshMarker().getSources().get(DOCUMENT_INFO_ASPECT_NAME),
        Long.valueOf(2_000L));
    assertFalse(stored.getEmbeddings().get("documentText").isCurrent());
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
