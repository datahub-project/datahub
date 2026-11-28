package com.linkedin.metadata.aspect.consistency.check;

import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.EmbeddingChunk;
import com.linkedin.common.EmbeddingChunkArray;
import com.linkedin.common.EmbeddingModelData;
import com.linkedin.common.EmbeddingModelDataMap;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.SemanticRefreshMarker;
import com.linkedin.common.SemanticSourceToken;
import com.linkedin.common.SemanticSourceTokenMap;
import com.linkedin.common.SemanticVectorState;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.FloatArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.SemanticContentIndexMismatchCheck.IndexReadFailedException;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields.DocumentValues;
import com.linkedin.metadata.search.elasticsearch.index.SemanticContentIndexFields.ProfileValues;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SemanticContentIndexMismatchCheckTest {

  private static final Urn DOC_URN = UrnUtils.getUrn("urn:li:document:doc-1");
  private static final Urn DOC_URN_2 = UrnUtils.getUrn("urn:li:document:doc-2");
  private static final Urn BRIDGE_DOC_URN = UrnUtils.getUrn("urn:li:document:bridge-dataset-abc");
  private static final String MODEL_A = "cohere_v3";
  private static final String MODEL_B = "titan_v2";

  private SemanticContentIndexFields indexFields;
  private EntityService<?> entityService;
  private OperationContext opContext;
  private CheckContext checkContext;
  private Sha256UrnEntityDocumentIdHasher hasher;
  private String v3DocumentIndex;

  @BeforeMethod
  public void setup() {
    indexFields = mock(SemanticContentIndexFields.class);
    entityService = mock(EntityService.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    checkContext =
        CheckContext.builder().operationContext(opContext).entityService(entityService).build();
    hasher = new Sha256UrnEntityDocumentIdHasher();
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    EntitySpec docSpec = opContext.getEntityRegistry().getEntitySpec("document");
    v3DocumentIndex = convention.getEntityIndexNameV3(opContext, V3IndexKeys.resolve(docSpec));
  }

  @Test
  public void testOnDemandCheckExcludedFromDefaultAndIncludedWhenRequested() {
    SemanticContentIndexMismatchCheck check =
        new SemanticContentIndexMismatchCheck(indexFields, v3ReadWriteConfig(), hasher, List.of());
    ConsistencyCheckRegistry registry = new ConsistencyCheckRegistry(List.of(check));

    assertTrue(registry.getDefaultByEntityTypeAndIds("document", null).isEmpty());
    assertEquals(
        registry.getDefaultByEntityTypeAndIds(
            "document", List.of(SemanticContentIndexMismatchCheck.CHECK_ID)),
        List.of(check));
  }

  @Test
  public void testInactiveWhenV3ReadHoldOrV2Active() {
    EntityIndexConfiguration readHoldConfig = new EntityIndexConfiguration();
    EntityIndexVersionConfiguration v2 = new EntityIndexVersionConfiguration();
    v2.setEnabled(true);
    EntityIndexVersionConfiguration v3 = new EntityIndexVersionConfiguration();
    v3.setEnabled(true);
    v3.setKeywordReadEnabled(false);
    readHoldConfig.setV2(v2);
    readHoldConfig.setV3(v3);
    readHoldConfig.setSemanticSearch(semanticConfig());
    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setEntityIndex(readHoldConfig);

    SemanticContentIndexMismatchCheck check =
        new SemanticContentIndexMismatchCheck(indexFields, esConfig, hasher, List.of());

    List<ConsistencyIssue> issues =
        check.check(checkContext, Map.of(DOC_URN, responseWith(readySqlContent())));
    assertTrue(issues.isEmpty());
    verifyNoInteractions(indexFields);
  }

  @Test
  public void testMatchingDocumentIgnoresRetiredInvalidDimensionAndNewerMarkerTokens()
      throws Exception {
    SemanticContent sql = readySqlContent();
    // Add a retired model and a wrong-dimension model_b in SQL — both are dropped by V3 projector.
    sql.getEmbeddings()
        .put("retired_model", modelData(1, 500L, SemanticVectorState.READY, true, "h0", "s0", 1));
    sql.getEmbeddings()
        .put(MODEL_B, modelData(1, 600L, SemanticVectorState.READY, true, "hb", "sb", 99));

    String docId = hasher.documentId(opContext, DOC_URN);
    DocumentValues v3Doc =
        new DocumentValues(
            docId,
            DOC_URN.toString(),
            false,
            null,
            marker("documentInfo", 1000L, 3L), // newer version than SQL's version 2 -> covered
            Map.of(
                MODEL_A,
                new ProfileValues(
                    "READY",
                    true,
                    "hash-a",
                    "retained-from-older-partial-merge", // unset on SQL below when cleared
                    1,
                    1700000000000L,
                    marker("documentInfo", 1000L, 2L))));
    sql.getEmbeddings().get(MODEL_A).removeSourceTextSha256();

    when(indexFields.byDocumentId(eq(opContext), eq(v3DocumentIndex), any(), any()))
        .thenReturn(Map.of(docId, v3Doc));

    SemanticContentIndexMismatchCheck check =
        new SemanticContentIndexMismatchCheck(indexFields, v3ReadWriteConfig(), hasher, List.of());

    assertTrue(check.check(checkContext, Map.of(DOC_URN, responseWith(sql))).isEmpty());
  }

  @Test
  public void testMissingAndTombstonedDocumentsEmitFullEntityRestoreIndices() throws Exception {
    String docId2 = hasher.documentId(opContext, DOC_URN_2);
    DocumentValues tombstoned =
        new DocumentValues(docId2, DOC_URN_2.toString(), true, null, null, Map.of());
    when(indexFields.byDocumentId(eq(opContext), eq(v3DocumentIndex), any(), any()))
        .thenReturn(Map.of(docId2, tombstoned));

    SemanticContentIndexMismatchCheck check =
        new SemanticContentIndexMismatchCheck(indexFields, v3ReadWriteConfig(), hasher, List.of());
    Map<Urn, EntityResponse> responses = new LinkedHashMap<>();
    responses.put(DOC_URN, responseWith(readySqlContent()));
    responses.put(DOC_URN_2, responseWith(readySqlContent()));

    List<ConsistencyIssue> issues = check.check(checkContext, responses);
    assertEquals(issues.size(), 2);

    ConsistencyIssue missing = issues.get(0);
    assertEquals(missing.getEntityUrn(), DOC_URN);
    assertEquals(missing.getFixType(), ConsistencyFixType.RESTORE_INDICES);
    assertNull(missing.getMetadata());
    assertTrue(missing.getDetails().contains("missing_document"));

    ConsistencyIssue tombstoneIssue = issues.get(1);
    assertEquals(tombstoneIssue.getEntityUrn(), DOC_URN_2);
    assertEquals(tombstoneIssue.getFixType(), ConsistencyFixType.RESTORE_INDICES);
    assertNull(tombstoneIssue.getMetadata());
    assertTrue(tombstoneIssue.getDetails().contains("tombstone_document"));
  }

  @Test
  public void testProfileAndRootMismatchesEmitSemanticContentScopedRestoreIndices()
      throws Exception {
    SemanticContent sql = readySqlContent();
    String docId = hasher.documentId(opContext, DOC_URN);
    DocumentValues staleV3Doc =
        new DocumentValues(
            docId,
            DOC_URN.toString(),
            false,
            "EMPTY_TEXT", // stale skipReason left in V3
            marker("documentInfo", 1000L, 1L), // older than SQL version 2
            Map.of(
                MODEL_A,
                new ProfileValues(
                    "READY",
                    true, // SQL flipped current -> false
                    "old-hash",
                    "sha-a",
                    1,
                    1700000000000L,
                    marker("documentInfo", 1000L, 2L)),
                MODEL_B,
                new ProfileValues( // unexpected stale profile absent from SQL
                    "READY", true, "hb", "sb", 1, 1600000000000L, null)));
    sql.getEmbeddings().get(MODEL_A).setCurrent(false);

    when(indexFields.byDocumentId(eq(opContext), eq(v3DocumentIndex), any(), any()))
        .thenReturn(Map.of(docId, staleV3Doc));

    SemanticContentIndexMismatchCheck check =
        new SemanticContentIndexMismatchCheck(indexFields, v3ReadWriteConfig(), hasher, List.of());

    List<ConsistencyIssue> issues = check.check(checkContext, Map.of(DOC_URN, responseWith(sql)));
    assertEquals(issues.size(), 1);
    ConsistencyIssue issue = issues.get(0);
    assertEquals(issue.getFixType(), ConsistencyFixType.RESTORE_INDICES);
    assertEquals(issue.getMetadata(), Map.of("aspects", SEMANTIC_CONTENT_ASPECT_NAME));
    assertTrue(issue.getDetails().contains("skipReason"));
    assertTrue(issue.getDetails().contains("refreshMarker"));
    assertTrue(issue.getDetails().contains("embeddings.cohere_v3.current"));
    assertTrue(issue.getDetails().contains("embeddings.cohere_v3.contentHash"));
    assertTrue(issue.getDetails().contains("embeddings.titan_v2:unexpected_profile"));
  }

  @Test
  public void testSkipsMissingSqlAspectOrphansAndSkipExtensionWithoutQueryingV3() {
    checkContext.addOrphanUrns("document", Set.of(DOC_URN));
    SemanticContentCheckSkip bridgeSkip =
        urn -> urn.toString().startsWith("urn:li:document:bridge-");
    SemanticContentIndexMismatchCheck check =
        new SemanticContentIndexMismatchCheck(
            indexFields, v3ReadWriteConfig(), hasher, List.of(bridgeSkip));

    EntityResponse withoutSemanticContent =
        new EntityResponse().setUrn(DOC_URN_2).setAspects(new EnvelopedAspectMap());
    Map<Urn, EntityResponse> responses = new LinkedHashMap<>();
    responses.put(DOC_URN, responseWith(readySqlContent())); // orphan
    responses.put(BRIDGE_DOC_URN, responseWith(readySqlContent())); // skipped by extension
    responses.put(
        DOC_URN_2, withoutSemanticContent); // keyAspectOnly entity with no semanticContent

    assertTrue(check.check(checkContext, responses).isEmpty());
    verifyNoInteractions(indexFields);
  }

  @Test
  public void testIndexReadFailureThrowsFatalConsistencyException() throws Exception {
    when(indexFields.byDocumentId(any(), any(), any(), any()))
        .thenThrow(new IOException("OpenSearch timeout"));

    SemanticContentIndexMismatchCheck check =
        new SemanticContentIndexMismatchCheck(indexFields, v3ReadWriteConfig(), hasher, List.of());

    assertThrows(
        IndexReadFailedException.class,
        () -> check.check(checkContext, Map.of(DOC_URN, responseWith(readySqlContent()))));
  }

  private static ElasticSearchConfiguration v3ReadWriteConfig() {
    EntityIndexVersionConfiguration v2 = new EntityIndexVersionConfiguration();
    v2.setEnabled(false);
    EntityIndexVersionConfiguration v3 = new EntityIndexVersionConfiguration();
    v3.setEnabled(true);
    v3.setKeywordReadEnabled(true);
    EntityIndexConfiguration entityIndex = new EntityIndexConfiguration();
    entityIndex.setV2(v2);
    entityIndex.setV3(v3);
    entityIndex.setSemanticSearch(semanticConfig());
    ElasticSearchConfiguration es = new ElasticSearchConfiguration();
    es.setEntityIndex(entityIndex);
    return es;
  }

  private static SemanticSearchConfiguration semanticConfig() {
    ModelEmbeddingConfig modelA = new ModelEmbeddingConfig();
    modelA.setVectorDimension(2);
    ModelEmbeddingConfig modelB = new ModelEmbeddingConfig();
    modelB.setVectorDimension(3);
    SemanticSearchConfiguration semantic = new SemanticSearchConfiguration();
    semantic.setEnabled(true);
    semantic.setEnabledEntities(Set.of("document"));
    semantic.setModels(Map.of(MODEL_A, modelA, MODEL_B, modelB));
    return semantic;
  }

  private static SemanticContent readySqlContent() {
    EmbeddingModelDataMap map = new EmbeddingModelDataMap();
    EmbeddingModelData profile =
        modelData(1, 1700000000000L, SemanticVectorState.READY, true, "hash-a", "sha-a", 2);
    profile.setRefreshMarker(marker("documentInfo", 1000L, 2L));
    map.put(MODEL_A, profile);
    return new SemanticContent()
        .setEmbeddings(map)
        .setRefreshMarker(marker("documentInfo", 1000L, 2L));
  }

  private static EmbeddingModelData modelData(
      int totalChunks,
      long generatedAt,
      SemanticVectorState state,
      boolean current,
      String contentHash,
      String sourceSha,
      int vectorDim) {
    FloatArray vec = new FloatArray();
    for (int i = 0; i < vectorDim; i++) {
      vec.add(0.1f * (i + 1));
    }
    EmbeddingChunkArray chunks = new EmbeddingChunkArray();
    chunks.add(new EmbeddingChunk().setPosition(0).setVector(vec));
    return new EmbeddingModelData()
        .setModelVersion("provider/model")
        .setTotalChunks(totalChunks)
        .setGeneratedAt(generatedAt)
        .setChunks(chunks)
        .setVectorState(state)
        .setCurrent(current)
        .setContentHash(contentHash)
        .setSourceTextSha256(sourceSha);
  }

  private static SemanticRefreshMarker marker(String watchKey, long createdTime, long version) {
    SemanticSourceTokenMap sources = new SemanticSourceTokenMap();
    sources.put(
        watchKey, new SemanticSourceToken().setCreatedTime(createdTime).setVersion(version));
    return new SemanticRefreshMarker().setSources(sources);
  }

  private static EntityResponse responseWith(SemanticContent semanticContent) {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        SEMANTIC_CONTENT_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(semanticContent.data())));
    return new EntityResponse().setUrn(DOC_URN).setAspects(aspects);
  }
}
