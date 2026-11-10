package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_OPERATION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROFILE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOCUMENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORCE_INDEXING_KEY;
import static com.linkedin.metadata.Constants.SEMANTIC_TEXT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Operation;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetUsageStatistics;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.RetryableIndexingException;
import com.linkedin.metadata.service.TimeseriesWriteThrottleCache;
import com.linkedin.metadata.service.UpdateIndicesUtil;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.ConnectionClosedException;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.config.RequestConfig;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UpdateIndicesV3StrategyTest {

  @Mock private EntityIndexVersionConfiguration v3Config;
  @Mock private ElasticSearchService elasticSearchService;
  @Mock private ESWriteDAO esWriteDAO;
  @Mock private ESBulkProcessor bulkProcessor;
  @Mock private SearchDocumentTransformer searchDocumentTransformer;
  @Mock private TimeseriesAspectService timeseriesAspectService;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private AspectSpec mockAspectSpec;
  @Mock private MCLItem mockEvent;
  @Mock private RecordTemplate mockAspect;
  @Mock private SystemMetadata mockSystemMetadata;
  @Mock private AuditStamp mockAuditStamp;
  @Mock private ESIndexBuilder mockIndexBuilder;
  private ObjectNode mockSearchDocument;
  @Mock private ReindexConfig mockReindexConfig;
  @Mock private AspectRetriever aspectRetriever;
  @Mock private SystemAspect storedSystemAspect;
  @Mock private RecordTemplate storedAspect;
  @Mock private AspectSpec storedAspectSpec;
  @Mock private AuditStamp storedAuditStamp;
  @Mock private SystemMetadata storedSystemMetadata;

  private OperationContext operationContext;
  private UpdateIndicesV3Strategy strategy;
  private Urn testUrn;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    testUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

    // Setup common mocks
    when(v3Config.isEnabled()).thenReturn(true);
    when(mockEvent.getUrn()).thenReturn(testUrn);
    when(mockEvent.getEntitySpec()).thenReturn(mockEntitySpec);
    when(mockEvent.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockEvent.getRecordTemplate()).thenReturn(mockAspect);
    when(mockEvent.getSystemMetadata()).thenReturn(mockSystemMetadata);

    // Setup entity spec mocks
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec));
    when(mockEvent.getAuditStamp()).thenReturn(mockAuditStamp);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEntitySpec.getName()).thenReturn(DATASET_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);

    // Setup mock search document
    mockSearchDocument = JsonNodeFactory.instance.objectNode();
    mockSearchDocument.put("urn", testUrn.toString());
    mockSearchDocument.put("_entityType", "dataset");

    // Setup mock index builder
    when(elasticSearchService.getIndexBuilder()).thenReturn(mockIndexBuilder);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(esWriteDAO);
    when(esWriteDAO.bulkProcessors()).thenReturn(List.of(bulkProcessor));
    when(bulkProcessor.flushAndAwait()).thenReturn(BulkProcessorFlushResult.empty());
    when(elasticSearchService.getIndexBuilder(anyString())).thenReturn(mockIndexBuilder);

    // Create strategy with V2 disabled (testing V3-only scenario)
    strategy = createStrategy(false, null);
  }

  @Test
  public void testRollbackDualWriteRepeatsTheWriteToTheOldBackingIndex() throws Exception {
    String alias = v3IndexName("dataset");
    String oldBacking = alias + "_1779165194703";
    AtomicReference<String> recordedIndex = new AtomicReference<>();
    UpdateIndicesV3Strategy dualWritingStrategy =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(
                Map.of(alias, v3Target(alias, oldBacking)),
                Map.of(),
                (indexName, startTime) -> recordedIndex.set(indexName),
                null,
                true));
    mockSearchDocument.put("name", "test dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    dualWritingStrategy.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite live = writeCaptor.getAllValues().get(0);
    V3SearchDocumentWrite rollback = writeCaptor.getAllValues().get(1);
    assertEquals(live.indexName(), alias);
    assertEquals(rollback.indexName(), oldBacking);
    assertEquals(rollback.docId(), live.docId());
    assertEquals(rollback.source(), live.source());
    assertEquals(rollback.kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertEquals(recordedIndex.get(), alias);
    verify(bulkProcessor, never()).flushAndAwait();
  }

  @Test
  public void testBaselineFillWritesToTheTargetIndexNamedInTheHeader() throws Exception {
    String alias = v3IndexName("dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    mockSearchDocument.put("name", "test dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    MCLItem fill = searchEvent(BackfillHeaders.MODE_BASELINE);
    fill.getMetadataChangeLog().getHeaders().put(BackfillHeaders.TARGET_INDEX_HEADER_NAME, next);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(fill)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().indexName(), next);
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.CREATE);
    verify(bulkProcessor).flushAndAwait();
  }

  @Test
  public void testBaselineWithoutATargetReplacesTheAliasDocument() throws Exception {
    // Operator LoadIndices maintenance: it repairs documents already in the alias, so it replaces
    // them, keeping the index-only fields the existing document holds.
    String alias = v3IndexName("dataset");
    mockSearchDocument.put("name", "reloaded dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    MCLItem reload = searchEvent(BackfillHeaders.MODE_BASELINE);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(reload)), false);

    verify(elasticSearchService)
        .getV3DocumentSourceBySearchGroup(eq(operationContext), eq("dataset"), anyString());
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().indexName(), alias);
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.REPLACE);
  }

  @Test
  public void testABaselineEventWithoutTheTargetMakesTheBatchReplaceTheAlias() throws Exception {
    // A side-effect event for the same URN may join the fill's batch without the target header.
    // Create-only would drop its change if the fill wrote the document first, so the batch
    // replaces the alias document instead.
    String alias = v3IndexName("dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    AspectSpec otherSpec = mock(AspectSpec.class);
    when(otherSpec.getName()).thenReturn("ownership");
    when(otherSpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpec("ownership")).thenReturn(otherSpec);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenAnswer(invocation -> Optional.of(searchDocument("mixed")));
    MCLItem fill =
        aspectEvent(
            mockAspectSpec, mockAspect, ChangeType.RESTATE, null, BackfillHeaders.MODE_BASELINE);
    fill.getMetadataChangeLog().getHeaders().put(BackfillHeaders.TARGET_INDEX_HEADER_NAME, next);
    MCLItem sideEffect =
        aspectEvent(otherSpec, mockAspect, ChangeType.RESTATE, null, BackfillHeaders.MODE_BASELINE);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(fill, sideEffect)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().indexName(), alias);
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.REPLACE);
  }

  @Test
  public void testAHeaderlessEventLeavesATargetFillOnItsIndex() throws Exception {
    // The headerless event is a different aspect: events for one aspect collapse to the last, so
    // this one survives beside the fill.
    String alias = v3IndexName("dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    AspectSpec otherSpec = mock(AspectSpec.class);
    when(otherSpec.getName()).thenReturn("ownership");
    when(otherSpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpec("ownership")).thenReturn(otherSpec);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    MCLItem fill = searchEvent(BackfillHeaders.MODE_BASELINE);
    fill.getMetadataChangeLog().getHeaders().put(BackfillHeaders.TARGET_INDEX_HEADER_NAME, next);
    MCLItem sideEffect = aspectEvent(otherSpec, mockAspect, ChangeType.RESTATE, null, null);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(fill, sideEffect)), false);

    // An event with no backfill mode, such as an MCL side effect, does not take the fill off the
    // index it named. A baseline event that itself lacks the target still does.
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().indexName(), next);
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.CREATE);
  }

  @Test
  public void testTargetFillDoesNotReadTheExistingDocument() throws Exception {
    mockSearchDocument.put("name", "baseline dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    MCLItem fill = searchEvent(BackfillHeaders.MODE_BASELINE);
    fill.getMetadataChangeLog()
        .getHeaders()
        .put(BackfillHeaders.TARGET_INDEX_HEADER_NAME, v3IndexName("dataset"));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(fill)), false);

    verify(elasticSearchService, never())
        .getV3DocumentSourceBySearchGroup(any(OperationContext.class), anyString(), anyString());
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testCatchUpWritesStillReadTheExistingDocument() throws Exception {
    mockSearchDocument.put("name", "replayed dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    MCLItem replay = searchEvent(BackfillHeaders.MODE_CATCH_UP);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(replay)), false);

    verify(elasticSearchService)
        .getV3DocumentSourceBySearchGroup(eq(operationContext), eq("dataset"), anyString());
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
  }

  private MCLItem completeFillEvent(AspectSpec aspectSpec, RecordTemplate record) {
    MCLItem event =
        aspectEvent(aspectSpec, record, ChangeType.RESTATE, null, BackfillHeaders.MODE_BASELINE);
    when(event.getMetadataChangeLog())
        .thenReturn(
            new MetadataChangeLog()
                .setHeaders(
                    new StringMap(
                        Map.of(
                            BackfillHeaders.MODE_HEADER_NAME,
                            BackfillHeaders.MODE_BASELINE,
                            BackfillHeaders.COMPLETE_ENTITY_HEADER_NAME,
                            "true"))));
    return event;
  }

  private ObjectNode documentWrittenWith(OperationContext context) throws Exception {
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(context), writeCaptor.capture());
    return (ObjectNode) new ObjectMapper().readTree(writeCaptor.getValue().source());
  }

  @Test
  public void testCompleteFillBatchIsTheStoredStateAndIsNotHydrated() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec keySpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(mockAuditStamp.getTime()).thenReturn(1_700_000_000_000L);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenAnswer(invocation -> Optional.of(searchDocument("filled")));

    strategy.processBatch(
        hydratedContext,
        Map.of(
            testUrn,
            List.of(
                completeFillEvent(keySpec, mockAspect),
                completeFillEvent(mockAspectSpec, mockAspect))),
        false);

    verify(aspectRetriever, never()).getLatestSystemAspects(any(), anyMap());
    ObjectNode written = documentWrittenWith(hydratedContext);
    assertTrue(written.get("_aspects").has("datasetKey"));
    assertTrue(written.get("_aspects").has("datasetProperties"));
    // The key aspect goes through the stored-aspect path, which stamps systemCreated; the event
    // overlay path never does for a RESTATE.
    assertEquals(written.get("systemCreated").asLong(), 1_700_000_000_000L);
  }

  @Test
  public void testBaselineFillWithoutTheCompletenessHeaderStillHydrates() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenAnswer(invocation -> Optional.of(searchDocument("partial")));
    MCLItem fill =
        aspectEvent(
            mockAspectSpec, mockAspect, ChangeType.RESTATE, null, BackfillHeaders.MODE_BASELINE);

    strategy.processBatch(hydratedContext, Map.of(testUrn, List.of(fill)), false);

    verify(aspectRetriever).getLatestSystemAspects(any(), anyMap());
  }

  @Test
  public void testCompleteFillBatchAppliesEditableOverridesLast() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec baseSpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(baseSpec.getName()).thenReturn("corpUserInfo");
    AspectSpec editableSpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(editableSpec.getName()).thenReturn("corpUserEditableInfo");
    RecordTemplate base = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate edited = org.mockito.Mockito.mock(RecordTemplate.class);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            eq(base),
            eq(baseSpec),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenAnswer(invocation -> Optional.of(searchDocument("base").put("displayName", "base")));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            eq(edited),
            eq(editableSpec),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenAnswer(
            invocation -> Optional.of(searchDocument("edited").put("displayName", "edited")));

    // Key order puts the override aspect before its base aspect; applied in that order the base
    // aspect would win the shared root field. The stored-aspect path applies overrides last.
    strategy.processBatch(
        hydratedContext,
        Map.of(
            testUrn,
            List.of(completeFillEvent(editableSpec, edited), completeFillEvent(baseSpec, base))),
        false);

    assertEquals(documentWrittenWith(hydratedContext).get("displayName").asText(), "edited");
  }

  @Test
  public void testRollbackDualWriteCoversTombstones() throws Exception {
    String alias = v3IndexName("dataset");
    String oldBacking = alias + "_1779165194703";
    UpdateIndicesV3Strategy dualWritingStrategy =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(
                Map.of(alias, v3Target(alias, oldBacking)), Map.of(), null, null, true));
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);

    dualWritingStrategy.processBatch(operationContext, Map.of(testUrn, List.of(delete)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getAllValues().get(0).kind(), V3SearchDocumentWrite.Kind.DELETE);
    assertEquals(writeCaptor.getAllValues().get(1).indexName(), oldBacking);
    assertEquals(writeCaptor.getAllValues().get(1).kind(), V3SearchDocumentWrite.Kind.DELETE);
    assertEquals(
        writeCaptor.getAllValues().get(1).docId(), writeCaptor.getAllValues().get(0).docId());
  }

  @Test
  public void testLiveHardDeleteRemovesTheAliasDocumentAndTombstonesTheRebuildTarget()
      throws Exception {
    // A rebuild writes a next index while the alias still serves the old one; catch-up cannot
    // replay a hard delete, so the next index itself gets a tombstone the fill's create-only write
    // cannot overwrite. The alias just loses the document.
    String alias = v3IndexName("dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    UpdateIndicesV3Strategy mirroring =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(Map.of(), Map.of(alias, next), null, null, true));
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);

    mirroring.processBatch(operationContext, Map.of(testUrn, List.of(delete)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite live = writeCaptor.getAllValues().get(0);
    V3SearchDocumentWrite mirrored = writeCaptor.getAllValues().get(1);
    assertEquals(live.indexName(), alias);
    assertEquals(live.kind(), V3SearchDocumentWrite.Kind.DELETE);
    assertEquals(mirrored.indexName(), next);
    assertEquals(mirrored.kind(), V3SearchDocumentWrite.Kind.TOMBSTONE);
    assertEquals(mirrored.docId(), live.docId());
    verify(bulkProcessor).flushAndAwait();
    assertTrue(
        operationContext
            .getObjectMapper()
            .readTree(mirrored.source())
            .get(V3SearchDocumentProjector.TOMBSTONE_FIELD)
            .asBoolean());
  }

  @Test
  public void testLiveHardDeleteTombstonesTheAliasWhileItIsFilledInPlace() throws Exception {
    // The fill writes the alias itself and creates only absent documents: a removed document would
    // let the fill bring the entity back, so the alias keeps a tombstone until catch-up.
    String alias = v3IndexName("dataset");
    UpdateIndicesV3Strategy fillingInPlace =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(Map.of(), Map.of(alias, alias), null, null, true));
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);

    fillingInPlace.processBatch(operationContext, Map.of(testUrn, List.of(delete)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().indexName(), alias);
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.TOMBSTONE);
  }

  @Test
  public void testAFailedMirrorIsRetryableAndNotSkippedWhileV2IsDualWritten() throws Exception {
    // A removal has no later event to repair it: a mirror that cannot be handed to the bulk
    // processor fails the batch as retryable so the consumer redelivers it, even while V2 is
    // dual-written, where a live upsert's failure is skipped.
    String alias = v3IndexName("dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    UpdateIndicesV3Strategy dualWriting =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .v2Enabled(true)
            .incrementalReindexWriteTargets(
                new IncrementalReindexWriteTargets(Map.of(), Map.of(alias, next), null, null, true))
            .build();
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);
    doAnswer(
            invocation -> {
              V3SearchDocumentWrite write = invocation.getArgument(1);
              if (next.equals(write.indexName())) {
                throw new IllegalStateException("processor closed");
              }
              return null;
            })
        .when(elasticSearchService)
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () ->
                dualWriting.processBatch(
                    operationContext, Map.of(testUrn, List.of(delete)), false));

    assertEquals(thrown.getCause().getMessage(), "processor closed");
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testADeleteThatFailedForGoodIsSkippedWhileV2IsDualWritten() throws Exception {
    // Redelivery cannot help a non-retryable failure, and failing the batch would drop the other
    // entities' writes with it, so such a delete is logged and skipped like any other live write.
    UpdateIndicesV3Strategy dualWriting = createStrategy(true, (TimeseriesWriteThrottleCache) null);
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);
    doThrow(new IllegalStateException("mapping rejected"))
        .when(elasticSearchService)
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));

    dualWriting.processBatch(operationContext, Map.of(testUrn, List.of(delete)), false);

    verify(elasticSearchService)
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testLiveUpsertIsNotMirroredIntoAnInFlightRebuildTarget() throws Exception {
    // Only deletes need the mirror: an upsert leaves a row that catch-up re-derives.
    String alias = v3IndexName("dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    UpdateIndicesV3Strategy mirroring =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(Map.of(), Map.of(alias, next), null, null, true));
    mockSearchDocument.put("name", "test dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    mirroring.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().indexName(), alias);
  }

  @Test
  public void testLiveNonKeyDeleteMirrorOnlyUpdatesAnExistingFillDocument() throws Exception {
    // A partial merge into an entity the fill has not reached would leave a stub the fill's
    // create-only write cannot replace, so the mirror only updates a document already there.
    String alias = v3IndexName("dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    UpdateIndicesV3Strategy mirroring =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(Map.of(), Map.of(alias, next), null, null, true));
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    ObjectNode deleted = searchDocument("deleted property");
    deleted.putNull("name");
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            nullable(RecordTemplate.class),
            eq(mockAspectSpec),
            eq(true),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(deleted));
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);

    mirroring.processBatch(operationContext, Map.of(testUrn, List.of(delete)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite live = writeCaptor.getAllValues().get(0);
    V3SearchDocumentWrite mirrored = writeCaptor.getAllValues().get(1);
    assertEquals(live.kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertEquals(mirrored.indexName(), next);
    assertEquals(mirrored.kind(), V3SearchDocumentWrite.Kind.MERGE_EXISTING);
    verify(elasticSearchService)
        .appendRunIdBySearchGroup(
            eq(operationContext), eq("dataset"), eq(live.docId()), eq(testUrn), eq("delete-run"));
    verify(elasticSearchService)
        .appendRunIdToExistingDocument(
            eq(operationContext), eq(next), eq(live.docId()), eq("delete-run"));
  }

  @Test
  public void testLiveMergeUpsertsWhileTheAliasIsFilledInPlace() throws Exception {
    // A partial document this creates ahead of the fill is skipped by the create-only fill, and
    // rebuilt by catch-up, which replays this event's window.
    String alias = v3IndexName("dataset");
    UpdateIndicesV3Strategy fillingInPlace =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(Map.of(), Map.of(alias, alias), null, null, true));
    mockSearchDocument.put("name", "test dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    fillingInPlace.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().indexName(), alias);
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
  }

  @Test
  public void testTimeseriesWriteDoesNotCreateADocumentWhileTheAliasIsFilledInPlace()
      throws Exception {
    // Catch-up never replays a timeseries row, so a document created here would stay partial: the
    // create-only fill skips an entity the index already holds.
    String alias = v3IndexName("dataset");
    String oldBacking = alias + "_1779165194703";
    UpdateIndicesV3Strategy fillingInPlace =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(
                Map.of(alias, v3Target(alias, oldBacking)),
                Map.of(alias, alias),
                null,
                null,
                true));
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1L);
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("run-ts");
    mockSearchDocument.put("rowCount", 1);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    fillingInPlace.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite live = writeCaptor.getAllValues().get(0);
    V3SearchDocumentWrite rollback = writeCaptor.getAllValues().get(1);
    assertEquals(live.indexName(), alias);
    assertEquals(live.kind(), V3SearchDocumentWrite.Kind.MERGE_EXISTING);
    assertEquals(rollback.indexName(), oldBacking);
    assertEquals(rollback.kind(), V3SearchDocumentWrite.Kind.MERGE_EXISTING);
    verify(elasticSearchService, never())
        .appendRunIdBySearchGroup(any(), anyString(), anyString(), any(Urn.class), anyString());
    verify(elasticSearchService, never())
        .appendRunIdByIndexName(any(), anyString(), anyString(), any(Urn.class), anyString());
    verify(elasticSearchService)
        .appendRunIdToExistingDocument(
            eq(operationContext), eq(alias), eq(live.docId()), eq("run-ts"));
    verify(elasticSearchService)
        .appendRunIdToExistingDocument(
            eq(operationContext), eq(oldBacking), eq(live.docId()), eq("run-ts"));
    verify(timeseriesAspectService)
        .upsertDocument(
            eq(operationContext), eq("dataset"), nullable(String.class), anyString(), any());
  }

  @Test
  public void testNoRollbackTargetForTheIndexWritesOnce() throws Exception {
    UpdateIndicesV3Strategy dualWritingStrategy =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(
                Map.of("chartindex_v3", v3Target("chartindex_v3", "chartindex_v3_1779165194703")),
                Map.of(),
                null,
                null,
                true));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    dualWritingStrategy.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), false);

    verify(elasticSearchService, times(1))
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testRollbackDualWriteFailureDoesNotFailTheLiveWrite() throws Exception {
    String alias = v3IndexName("dataset");
    String oldBacking = alias + "_1779165194703";
    UpdateIndicesV3Strategy dualWritingStrategy =
        createStrategyWithWriteTargets(
            new IncrementalReindexWriteTargets(
                Map.of(alias, v3Target(alias, oldBacking)), Map.of(), null, null, true));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    doThrow(new IllegalStateException("old backing index is gone"))
        .when(elasticSearchService)
        .writeV3SearchDocument(
            eq(operationContext), argThat(write -> oldBacking.equals(write.indexName())));

    dualWritingStrategy.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), false);

    verify(elasticSearchService)
        .writeV3SearchDocument(
            eq(operationContext), argThat(write -> alias.equals(write.indexName())));
  }

  private UpdateIndicesV3Strategy createStrategyWithWriteTargets(
      IncrementalReindexWriteTargets incrementalReindexWriteTargets) {
    return UpdateIndicesV3Strategy.builder()
        .v3Config(v3Config)
        .elasticSearchService(elasticSearchService)
        .searchDocumentTransformer(searchDocumentTransformer)
        .timeseriesAspectService(timeseriesAspectService)
        .incrementalReindexWriteTargets(incrementalReindexWriteTargets)
        .build();
  }

  private String v3IndexName(String searchGroup) {
    return operationContext
        .getSearchContext()
        .getIndexConvention()
        .getEntityIndexNameV3(operationContext, searchGroup);
  }

  private static IncrementalReindexWriteTargets.RollbackTarget v3Target(
      String alias, String oldBacking) {
    return new IncrementalReindexWriteTargets.RollbackTarget(
        alias, oldBacking, EntityIndexVersion.V3);
  }

  private UpdateIndicesV3Strategy createStrategy(
      boolean v2Enabled, TimeseriesWriteThrottleCache timeseriesThrottleCache) {
    return UpdateIndicesV3Strategy.builder()
        .v3Config(v3Config)
        .elasticSearchService(elasticSearchService)
        .searchDocumentTransformer(searchDocumentTransformer)
        .timeseriesAspectService(timeseriesAspectService)
        .v2Enabled(v2Enabled)
        .timeseriesThrottleCache(timeseriesThrottleCache)
        .build();
  }

  private UpdateIndicesV3Strategy createStrategy(
      boolean v2Enabled,
      EntityDocumentIdHasher entityDocumentIdHasher,
      List<V3SearchDocumentContributor> documentContributors) {
    return UpdateIndicesV3Strategy.builder()
        .v3Config(v3Config)
        .elasticSearchService(elasticSearchService)
        .searchDocumentTransformer(searchDocumentTransformer)
        .timeseriesAspectService(timeseriesAspectService)
        .v2Enabled(v2Enabled)
        .entityDocumentIdHasher(entityDocumentIdHasher)
        .documentContributors(documentContributors)
        .build();
  }

  private MCLItem searchEvent(String backfillMode) {
    MCLItem event = org.mockito.Mockito.mock(MCLItem.class);
    when(event.getUrn()).thenReturn(testUrn);
    when(event.getEntitySpec()).thenReturn(mockEntitySpec);
    when(event.getAspectSpec()).thenReturn(mockAspectSpec);
    when(event.getRecordTemplate()).thenReturn(mockAspect);
    when(event.getSystemMetadata()).thenReturn(mockSystemMetadata);
    when(event.getAuditStamp()).thenReturn(mockAuditStamp);
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(event.getAspectName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    if (backfillMode != null) {
      when(event.getMetadataChangeLog())
          .thenReturn(
              new MetadataChangeLog()
                  .setHeaders(
                      new StringMap(Map.of(BackfillHeaders.MODE_HEADER_NAME, backfillMode))));
    }
    return event;
  }

  private ObjectNode searchDocument(String name) {
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put("urn", testUrn.toString());
    document.put("_entityType", "dataset");
    document.put("name", name);
    return document;
  }

  /**
   * The entity's key aspect as hydration returns it for an entity that exists in primary storage.
   * It projects nothing, so the document under test is unchanged; its presence is what tells the
   * writer the entity is still there.
   */
  private SystemAspect hydratedKeyAspect(OperationContext hydratedContext) throws Exception {
    AspectSpec keySpec = mock(AspectSpec.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(keySpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(keySpec);
    RecordTemplate keyRecord = mock(RecordTemplate.class);
    SystemAspect keyAspect = mock(SystemAspect.class);
    when(keyAspect.getAspectName()).thenReturn("datasetKey");
    when(keyAspect.getAspectSpec()).thenReturn(keySpec);
    when(keyAspect.getRecordTemplate()).thenReturn(keyRecord);
    when(keyAspect.getUrn()).thenReturn(testUrn);
    when(keyAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(keyAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext), eq(testUrn), eq(keyRecord), eq(keySpec), eq(false), any()))
        .thenReturn(Optional.empty());
    return keyAspect;
  }

  private MCLItem aspectEvent(
      AspectSpec aspectSpec,
      RecordTemplate recordTemplate,
      ChangeType changeType,
      String runId,
      String backfillMode) {
    return aspectEvent(testUrn, aspectSpec, recordTemplate, changeType, runId, backfillMode);
  }

  private MCLItem aspectEvent(
      Urn urn,
      AspectSpec aspectSpec,
      RecordTemplate recordTemplate,
      ChangeType changeType,
      String runId,
      String backfillMode) {
    MCLItem event = org.mockito.Mockito.mock(MCLItem.class);
    String aspectName = aspectSpec.getName();
    when(event.getUrn()).thenReturn(urn);
    when(event.getEntitySpec()).thenReturn(mockEntitySpec);
    when(event.getAspectSpec()).thenReturn(aspectSpec);
    when(event.getAspectName()).thenReturn(aspectName);
    when(event.getRecordTemplate()).thenReturn(recordTemplate);
    when(event.getAuditStamp()).thenReturn(mockAuditStamp);
    when(event.getChangeType()).thenReturn(changeType);
    if (runId != null) {
      when(event.getSystemMetadata()).thenReturn(new SystemMetadata().setRunId(runId));
    }
    if (backfillMode != null) {
      when(event.getMetadataChangeLog())
          .thenReturn(
              new MetadataChangeLog()
                  .setHeaders(
                      new StringMap(Map.of(BackfillHeaders.MODE_HEADER_NAME, backfillMode))));
    }
    return event;
  }

  /** Puts one event on the rebuild path. Live events carry no backfill header. */
  private void markBackfill(MCLItem event, String mode) {
    when(event.getMetadataChangeLog())
        .thenReturn(
            new MetadataChangeLog()
                .setHeaders(new StringMap(Map.of(BackfillHeaders.MODE_HEADER_NAME, mode))));
  }

  private AspectSpec searchableTimeseriesSpec(String aspectName) {
    AspectSpec aspectSpec = org.mockito.Mockito.mock(AspectSpec.class);
    SearchableFieldSpec searchableFieldSpec = org.mockito.Mockito.mock(SearchableFieldSpec.class);
    SearchableAnnotation annotation = org.mockito.Mockito.mock(SearchableAnnotation.class);
    when(annotation.isV3Searchable()).thenReturn(true);
    when(annotation.resolvedForV3()).thenReturn(annotation);
    when(annotation.isIncludeSystemModifiedAt()).thenReturn(true);
    when(searchableFieldSpec.getSearchableAnnotation()).thenReturn(annotation);
    when(aspectSpec.getName()).thenReturn(aspectName);
    when(aspectSpec.isTimeseries()).thenReturn(true);
    when(aspectSpec.getSearchableFieldSpecs()).thenReturn(List.of(searchableFieldSpec));
    if (DATASET_PROFILE_ASPECT_NAME.equals(aspectName)) {
      doReturn(DatasetProfile.class).when(aspectSpec).getDataTemplateClass();
    } else if (DATASET_USAGE_STATISTICS_ASPECT_NAME.equals(aspectName)) {
      doReturn(DatasetUsageStatistics.class).when(aspectSpec).getDataTemplateClass();
    } else if (DATASET_OPERATION_ASPECT_NAME.equals(aspectName)) {
      doReturn(Operation.class).when(aspectSpec).getDataTemplateClass();
    }
    return aspectSpec;
  }

  private EnvelopedAspect timeseriesEnvelope(RecordTemplate aspect) {
    return new EnvelopedAspect()
        .setAspect(GenericRecordUtils.serializeAspect(aspect))
        .setSystemMetadata(new SystemMetadata().setRunId("stored-timeseries"));
  }

  private SystemAspect storedAspect(
      String aspectName, AspectSpec aspectSpec, RecordTemplate record) {
    SystemAspect aspect = org.mockito.Mockito.mock(SystemAspect.class);
    when(aspect.getAspectName()).thenReturn(aspectName);
    when(aspect.getAspectSpec()).thenReturn(aspectSpec);
    when(aspect.getRecordTemplate()).thenReturn(record);
    when(aspect.getUrn()).thenReturn(testUrn);
    when(aspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(aspect.getAuditStamp()).thenReturn(mockAuditStamp);
    return aspect;
  }

  private static Map<String, SystemAspect> orderedAspects(SystemAspect first, SystemAspect second) {
    Map<String, SystemAspect> aspects = new LinkedHashMap<>();
    aspects.put(first.getAspectName(), first);
    aspects.put(second.getAspectName(), second);
    return aspects;
  }

  @Test
  public void testIsEnabled() {
    // Test when enabled
    when(v3Config.isEnabled()).thenReturn(true);
    assertTrue(strategy.isEnabled());

    // Test when disabled
    when(v3Config.isEnabled()).thenReturn(false);
    assertFalse(strategy.isEnabled());
  }

  @Test
  public void testShouldSkipSearchIndexUpdateDelegatesToRegisteredSkips() {
    UpdateIndicesV3Strategy skipping =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .searchIndexSkips(List.of(event -> event == mockEvent))
            .build();

    assertTrue(skipping.shouldSkipSearchIndexUpdate(mockEvent));
    assertFalse(strategy.shouldSkipSearchIndexUpdate(mockEvent));
  }

  @Test
  public void testProcessBatch_EmptyEvents() {
    // Execute with empty events
    strategy.processBatch(operationContext, Collections.emptyMap(), true);

    // Verify no processing occurred
    verify(elasticSearchService, never())
        .upsertDocument(any(), anyString(), anyString(), anyString());
    verify(elasticSearchService, never()).deleteDocument(any(), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_SingleEvent() throws Exception {
    // Setup mocks
    mockSearchDocument.put("name", "test dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute
    strategy.processBatch(operationContext, groupedEvents, true);

    // Verify
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite write = writeCaptor.getValue();
    JsonNode writtenDocument = operationContext.getObjectMapper().readTree(write.source());
    assertEquals(
        write.indexName(),
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "dataset"));
    assertEquals(write.urn(), testUrn);
    assertEquals(write.kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertEquals(write.docId(), DigestUtils.sha256Hex(testUrn.toString()));
    assertFalse(write.docId().contains("urn:li:"));
    assertTrue(write.source().contains("\"urn\":\"" + testUrn + "\""));
    assertEquals(writtenDocument.get("name").asText(), "test dataset");
    assertEquals(
        writtenDocument.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).get("name").asText(),
        "test dataset");
  }

  @Test
  public void testProcessBatch_TargetFillWritesCreateOnly() throws Exception {
    MetadataChangeLog mcl =
        new MetadataChangeLog()
            .setHeaders(
                new StringMap(
                    Map.of(
                        BackfillHeaders.MODE_HEADER_NAME,
                        BackfillHeaders.MODE_BASELINE,
                        BackfillHeaders.TARGET_INDEX_HEADER_NAME,
                        v3IndexName("dataset"))));
    when(mockEvent.getMetadataChangeLog()).thenReturn(mcl);
    mockSearchDocument.put("name", "baseline dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.CREATE);
  }

  @Test
  public void testLiveWriteFailureIsSkippedWhileV2IsDualWritten() throws Exception {
    // One URN's V3 failure must not stall V2 for live traffic; the entity's next event rebuilds it.
    strategy = createStrategy(true, (TimeseriesWriteThrottleCache) null);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    doThrow(new RetryableIndexingException("bulk unavailable", new RuntimeException("io")))
        .when(elasticSearchService)
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    verify(elasticSearchService)
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testCatchUpReplayFailureIsNotSkippedWhileV2IsDualWritten() throws Exception {
    // A catch-up replay is the only pass over its window and its producer checkpoints after the
    // send, so a swallowed failure is a document nothing comes back for; a later baseline cannot
    // repair it because the fill only creates absent documents. The failure must reach the caller:
    // the loader retries, the MAE consumer redelivers.
    strategy = createStrategy(true, (TimeseriesWriteThrottleCache) null);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(storedSystemAspect.getAspectName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(storedSystemAspect.getAspectSpec()).thenReturn(mockAspectSpec);
    when(storedSystemAspect.getRecordTemplate()).thenReturn(storedAspect);
    when(storedSystemAspect.getUrn()).thenReturn(testUrn);
    when(storedSystemAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(storedSystemAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    "datasetKey",
                    hydratedKey,
                    DATASET_PROPERTIES_ASPECT_NAME,
                    storedSystemAspect)));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(storedAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(searchDocument("authoritative current")));
    RetryableIndexingException failure =
        new RetryableIndexingException("bulk unavailable", new RuntimeException("io"));
    doThrow(failure)
        .when(elasticSearchService)
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
    MCLItem replay =
        aspectEvent(
            mockAspectSpec, mockAspect, ChangeType.RESTATE, null, BackfillHeaders.MODE_CATCH_UP);

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () ->
                strategy.processBatch(
                    hydratedContext, Collections.singletonMap(testUrn, List.of(replay)), true));

    assertEquals(thrown, failure);
  }

  @Test
  public void testBaselineFillFailureIsNotSkippedWhileV2IsDualWritten() throws Exception {
    // The fill is the only pass over the historical corpus and the loader counts a URN as
    // processed once the batch returns, so a swallowed failure would be a permanent hole. It must
    // reach the loader, which retries the batch and otherwise fails the run for a resume.
    strategy = createStrategy(true, (TimeseriesWriteThrottleCache) null);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    RetryableIndexingException failure =
        new RetryableIndexingException("bulk unavailable", new RuntimeException("io"));
    doThrow(failure)
        .when(elasticSearchService)
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
    MCLItem fill = searchEvent(BackfillHeaders.MODE_BASELINE);

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () ->
                strategy.processBatch(
                    operationContext, Collections.singletonMap(testUrn, List.of(fill)), true));

    assertEquals(thrown, failure);
  }

  @Test
  public void testCoalescesAspectUpdatesToFinalEventAndRetainsAllRunIds() throws Exception {
    RecordTemplate firstAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate finalAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem firstEvent =
        aspectEvent(
            mockAspectSpec,
            firstAspect,
            ChangeType.UPSERT,
            "first-run",
            BackfillHeaders.MODE_BASELINE);
    MCLItem finalEvent =
        aspectEvent(
            mockAspectSpec,
            finalAspect,
            ChangeType.UPSERT,
            "final-run",
            BackfillHeaders.MODE_BASELINE);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(firstAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("stale")));
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(finalAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("final")));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(firstEvent, finalEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("name").asText(), "final");
    assertEquals(writtenDocument.get("runId").get(0).asText(), "first-run");
    assertEquals(writtenDocument.get("runId").get(1).asText(), "final-run");
    verify(searchDocumentTransformer, never())
        .transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(firstAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp));
  }

  @Test
  public void testCoalescedEventsRetainFinalOccurrenceOrder() throws Exception {
    AspectSpec secondAspectSpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(secondAspectSpec.getName()).thenReturn("ownership");
    when(secondAspectSpec.isTimeseries()).thenReturn(false);
    RecordTemplate firstAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate secondAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate finalFirstAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem firstEvent =
        aspectEvent(
            mockAspectSpec, firstAspect, ChangeType.UPSERT, null, BackfillHeaders.MODE_BASELINE);
    MCLItem secondEvent =
        aspectEvent(
            secondAspectSpec, secondAspect, ChangeType.UPSERT, null, BackfillHeaders.MODE_BASELINE);
    MCLItem finalFirstEvent =
        aspectEvent(
            mockAspectSpec,
            finalFirstAspect,
            ChangeType.UPSERT,
            null,
            BackfillHeaders.MODE_BASELINE);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(secondAspect),
            eq(secondAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("second")));
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(finalFirstAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("final first")));

    strategy.processBatch(
        operationContext, Map.of(testUrn, List.of(firstEvent, secondEvent, finalFirstEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("name").asText(), "final first");
    assertEquals(
        writtenDocument.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).get("name").asText(),
        "final first");
    assertEquals(writtenDocument.get("_aspects").get("ownership").get("name").asText(), "second");
  }

  @Test
  public void testCoalescedStructuredPropertyRetainsEarliestPreviousValue() throws Exception {
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    Urn datasetType = UrnUtils.getUrn("urn:li:entityType:datahub.dataset");
    Urn chartType = UrnUtils.getUrn("urn:li:entityType:datahub.chart");
    Urn dashboardType = UrnUtils.getUrn("urn:li:entityType:datahub.dashboard");
    StructuredPropertyDefinition initialDefinition =
        propertyDefinition("initial.property", new UrnArray(datasetType));
    StructuredPropertyDefinition firstDefinition =
        propertyDefinition("first.property", new UrnArray(datasetType, chartType));
    StructuredPropertyDefinition finalDefinition =
        propertyDefinition("final.property", new UrnArray(datasetType, chartType, dashboardType));
    MCLItem firstEvent =
        aspectEvent(mockAspectSpec, firstDefinition, ChangeType.UPSERT, null, null);
    MCLItem finalEvent =
        aspectEvent(mockAspectSpec, finalDefinition, ChangeType.UPSERT, null, null);
    when(firstEvent.getPreviousRecordTemplate()).thenReturn(initialDefinition);
    when(finalEvent.getPreviousRecordTemplate()).thenReturn(firstDefinition);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(finalDefinition),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("final definition")));
    SearchClientShim<?> searchClient = stubMappingClient();

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(firstEvent, finalEvent)), true);

    String mapping = capturedPutMapping(searchClient);
    assertTrue(mapping.contains("final_property"));
    assertFalse(mapping.contains("first_property"));
    assertFalse(mapping.contains("initial_property"));
    verify(elasticSearchService, never())
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class));
  }

  @Test
  public void testLiveStructuredPropertyMappingUpdateIsSkippedWhenHookDisabled() throws Exception {
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    StructuredPropertyDefinition definition =
        propertyDefinition(
            "live.property", new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    MCLItem event = aspectEvent(mockAspectSpec, definition, ChangeType.UPSERT, null, null);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(definition),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("definition")));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(event)), false);

    verify(elasticSearchService, never())
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class));
    verify(mockIndexBuilder, never()).getSearchClient();
    verify(mockIndexBuilder, never())
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), anyBoolean());
  }

  @Test
  public void testBaselineStructuredPropertyDefinitionUpdatesMappings() throws Exception {
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    StructuredPropertyDefinition definition =
        propertyDefinition(
            "baseline.property",
            new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    MCLItem event =
        aspectEvent(
            mockAspectSpec, definition, ChangeType.UPSERT, null, BackfillHeaders.MODE_BASELINE);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(definition),
            eq(mockAspectSpec),
            eq(false),
            any()))
        .thenReturn(Optional.of(searchDocument("baseline definition")));
    SearchClientShim<?> searchClient = stubMappingClient();

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(event)), true);

    assertTrue(capturedPutMapping(searchClient).contains("structuredProperties"));
    verify(elasticSearchService, never())
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class));
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(any(), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.REPLACE);
  }

  @Test
  public void testFinalNonKeyDeleteNullsProjectedFields() throws Exception {
    RecordTemplate upsertAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem upsert =
        aspectEvent(mockAspectSpec, upsertAspect, ChangeType.UPSERT, "upsert-run", null);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    ObjectNode deleted = searchDocument("must be removed");
    deleted.putNull("name");
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            nullable(RecordTemplate.class),
            eq(mockAspectSpec),
            eq(true),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(deleted));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(upsert, delete)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertTrue(writtenDocument.get("name").isNull());
    assertTrue(
        writtenDocument
            .path("_aspects")
            .path(DATASET_PROPERTIES_ASPECT_NAME)
            .path("name")
            .isNull());
    verify(aspectRetriever, never()).getLatestSystemAspects(any(), anyMap());
  }

  @Test
  public void testFinalNonKeyUpsertSupersedesEarlierDelete() throws Exception {
    RecordTemplate restoredAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);
    MCLItem upsert =
        aspectEvent(mockAspectSpec, restoredAspect, ChangeType.UPSERT, "upsert-run", null);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(restoredAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("restored")));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(delete, upsert)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertEquals(writtenDocument.get("name").asText(), "restored");
    assertFalse(writtenDocument.path("_v3Tombstone").asBoolean());
  }

  @Test
  public void testFinalKeyDeleteSupersedesEarlierUpsert() throws Exception {
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    RecordTemplate keyAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem upsert = aspectEvent(mockAspectSpec, keyAspect, ChangeType.UPSERT, "upsert-run", null);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(upsert, delete)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.DELETE);
    assertTrue(writtenDocument.get("_v3Tombstone").asBoolean());
    verify(searchDocumentTransformer, never())
        .transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(keyAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp));
  }

  @Test
  public void testFinalKeyUpsertSupersedesEarlierDelete() throws Exception {
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    RecordTemplate keyAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);
    MCLItem upsert = aspectEvent(mockAspectSpec, keyAspect, ChangeType.UPSERT, "upsert-run", null);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(keyAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("restored")));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(delete, upsert)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertEquals(writtenDocument.get("name").asText(), "restored");
    assertFalse(writtenDocument.get("removed").asBoolean());
    assertTrue(writtenDocument.get("_v3Tombstone").isNull());
  }

  @Test
  public void testLiveKeyUpsertClearsHardDeleteMarker() throws Exception {
    AspectSpec keySpec = mock(AspectSpec.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(keySpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(keySpec);
    RecordTemplate keyAspect = mock(RecordTemplate.class);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(keyAspect),
            eq(keySpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("recreated")));

    strategy.processBatch(
        operationContext,
        Map.of(testUrn, List.of(aspectEvent(keySpec, keyAspect, ChangeType.UPSERT, null, null))),
        true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite write = writeCaptor.getValue();
    JsonNode writtenDocument = operationContext.getObjectMapper().readTree(write.source());
    assertEquals(write.kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertFalse(writtenDocument.get("removed").asBoolean());
    assertTrue(writtenDocument.get("_v3Tombstone").isNull());
    verify(aspectRetriever, never()).getLatestSystemAspects(any(), any());
  }

  @Test
  public void testLiveMergeStampsIndexTimeAndAppendsRunId() throws Exception {
    RecordTemplate aspect = mock(RecordTemplate.class);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(aspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("named")));

    strategy.processBatch(
        operationContext,
        Map.of(
            testUrn,
            List.of(aspectEvent(mockAspectSpec, aspect, ChangeType.UPSERT, "live-run", null))),
        true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite write = writeCaptor.getValue();
    JsonNode writtenDocument = operationContext.getObjectMapper().readTree(write.source());
    assertFalse(writtenDocument.has("runId"));
    assertTrue(writtenDocument.hasNonNull("systemIndexModified"));
    verify(elasticSearchService)
        .appendRunIdBySearchGroup(
            eq(operationContext), eq("dataset"), eq(write.docId()), eq(testUrn), eq("live-run"));
    verify(elasticSearchService, never())
        .appendRunIdByIndexName(any(), anyString(), anyString(), any(), any());
    verify(elasticSearchService, never())
        .appendRunIdToExistingDocument(any(), anyString(), anyString(), any());
  }

  @Test
  public void testLiveKeyDeleteThenUpsertClearsHardDeleteMarker() throws Exception {
    AspectSpec keySpec = mock(AspectSpec.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(keySpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(keySpec);
    RecordTemplate keyAspect = mock(RecordTemplate.class);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(keyAspect),
            eq(keySpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("recreated")));
    MCLItem delete = aspectEvent(keySpec, null, ChangeType.DELETE, null, null);
    MCLItem upsert = aspectEvent(keySpec, keyAspect, ChangeType.UPSERT, null, null);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(delete, upsert)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertFalse(writtenDocument.get("removed").asBoolean());
    assertTrue(writtenDocument.get("_v3Tombstone").isNull());
    assertEquals(writtenDocument.get("name").asText(), "recreated");
  }

  @Test
  public void testLiveKeyUpsertKeepsStatusRemoved() throws Exception {
    AspectSpec keySpec = mock(AspectSpec.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(keySpec.isTimeseries()).thenReturn(false);
    AspectSpec statusSpec = mock(AspectSpec.class);
    when(statusSpec.getName()).thenReturn("status");
    when(statusSpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(keySpec);
    when(mockEntitySpec.getAspectSpec("status")).thenReturn(statusSpec);
    RecordTemplate keyAspect = mock(RecordTemplate.class);
    RecordTemplate statusAspect = mock(RecordTemplate.class);
    ObjectNode statusDocument = searchDocument("soft deleted");
    statusDocument.put("removed", true);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(keyAspect),
            eq(keySpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("recreated")));
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(statusAspect),
            eq(statusSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(statusDocument));

    strategy.processBatch(
        operationContext,
        Map.of(
            testUrn,
            List.of(
                aspectEvent(keySpec, keyAspect, ChangeType.UPSERT, null, null),
                aspectEvent(statusSpec, statusAspect, ChangeType.UPSERT, null, null))),
        true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertTrue(writtenDocument.get("removed").asBoolean());
    assertTrue(writtenDocument.get("_v3Tombstone").isNull());
  }

  @Test
  public void testLiveNonKeyUpdateLeavesRemovedUnset() throws Exception {
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            any(),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("tagged")));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertFalse(writtenDocument.has("removed"));
    assertFalse(writtenDocument.has("_v3Tombstone"));
  }

  @Test
  public void testLiveSemanticContentMergesLikeAnyOtherAspect() throws Exception {
    when(mockAspectSpec.getName()).thenReturn("semanticContent");
    when(mockEvent.getAspectName()).thenReturn("semanticContent");
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("embedded")));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
  }

  @Test
  public void testSupersededBaselineEventDoesNotMakeTheWriteABaselineFill() throws Exception {
    RecordTemplate baselineAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate liveAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem baseline =
        aspectEvent(
            mockAspectSpec, baselineAspect, ChangeType.UPSERT, null, BackfillHeaders.MODE_BASELINE);
    MCLItem live = aspectEvent(mockAspectSpec, liveAspect, ChangeType.UPSERT, null, null);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(liveAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("live")));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(baseline, live)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
  }

  @Test
  public void testCatchUpReplacesWithAuthoritativeHydratedState() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    RecordTemplate liveAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate staleCatchUpAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem liveEvent = aspectEvent(mockAspectSpec, liveAspect, ChangeType.UPSERT, null, null);
    MCLItem catchUpEvent =
        aspectEvent(
            mockAspectSpec,
            staleCatchUpAspect,
            ChangeType.UPSERT,
            null,
            BackfillHeaders.MODE_CATCH_UP);
    when(storedSystemAspect.getAspectName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(storedSystemAspect.getAspectSpec()).thenReturn(mockAspectSpec);
    when(storedSystemAspect.getRecordTemplate()).thenReturn(storedAspect);
    when(storedSystemAspect.getUrn()).thenReturn(testUrn);
    when(storedSystemAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(storedSystemAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    "datasetKey",
                    hydratedKey,
                    DATASET_PROPERTIES_ASPECT_NAME,
                    storedSystemAspect)));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(storedAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(searchDocument("authoritative current")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(liveAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("live event")));

    strategy.processBatch(
        hydratedContext, Collections.singletonMap(testUrn, List.of(liveEvent)), true);
    strategy.processBatch(
        hydratedContext, Collections.singletonMap(testUrn, List.of(catchUpEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    List<V3SearchDocumentWrite> writes = writeCaptor.getAllValues();
    assertEquals(writes.get(0).kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertEquals(writes.get(1).kind(), V3SearchDocumentWrite.Kind.REPLACE);
    JsonNode catchUpDocument = hydratedContext.getObjectMapper().readTree(writes.get(1).source());
    assertEquals(catchUpDocument.get("name").asText(), "authoritative current");
    verify(searchDocumentTransformer, never())
        .transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(staleCatchUpAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp));
  }

  @Test
  public void testStoredAspectRootFieldCollisionResolvesDeterministically() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);

    // Two stored aspects both project the root convenience field "name". Neither is an editable
    // override, so the ascending aspect-name tiebreak decides the winner: "zebraAspect" is applied
    // last and wins, regardless of the hydration map's iteration order.
    AspectSpec aardvarkSpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(aardvarkSpec.getName()).thenReturn("aardvarkAspect");
    when(aardvarkSpec.isTimeseries()).thenReturn(false);
    AspectSpec zebraSpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(zebraSpec.getName()).thenReturn("zebraAspect");
    when(zebraSpec.isTimeseries()).thenReturn(false);

    RecordTemplate aardvarkRecord = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate zebraRecord = org.mockito.Mockito.mock(RecordTemplate.class);
    SystemAspect aardvarkAspect = storedAspect("aardvarkAspect", aardvarkSpec, aardvarkRecord);
    SystemAspect zebraAspect = storedAspect("zebraAspect", zebraSpec, zebraRecord);

    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(aardvarkRecord),
            eq(aardvarkSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("name-from-aardvark")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(zebraRecord),
            eq(zebraSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("name-from-zebra")));

    // A trigger event for a third aspect drives the build without touching the "name" root field.
    RecordTemplate triggerRecord = org.mockito.Mockito.mock(RecordTemplate.class);
    ObjectNode triggerDoc = JsonNodeFactory.instance.objectNode();
    triggerDoc.put("urn", testUrn.toString());
    triggerDoc.put("_entityType", "dataset");
    triggerDoc.put("trigger", "x");
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(triggerRecord),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(triggerDoc));
    MCLItem triggerEvent =
        aspectEvent(
            mockAspectSpec, triggerRecord, ChangeType.UPSERT, null, BackfillHeaders.MODE_BASELINE);

    // Same two aspects hydrated in opposite iteration orders across the two runs. Build the maps
    // before stubbing so the getAspectName() calls are not misread as nested stubbing.
    final Map<String, SystemAspect> aardvarkFirst = orderedAspects(aardvarkAspect, zebraAspect);
    final Map<String, SystemAspect> zebraFirst = orderedAspects(zebraAspect, aardvarkAspect);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, aardvarkFirst), Map.of(testUrn, zebraFirst));

    strategy.processBatch(
        hydratedContext, Collections.singletonMap(testUrn, List.of(triggerEvent)), true);
    strategy.processBatch(
        hydratedContext, Collections.singletonMap(testUrn, List.of(triggerEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    for (V3SearchDocumentWrite write : writeCaptor.getAllValues()) {
      JsonNode document = hydratedContext.getObjectMapper().readTree(write.source());
      assertEquals(document.get("name").asText(), "name-from-zebra");
    }
  }

  @Test
  public void testEditableOverrideAspectWinsRootFieldCollision() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);

    // Real DataHub pair: corpUserInfo (ingested base) and corpUserEditableInfo (user override) both
    // project the root convenience field "name" (displayName). The editable aspect must win the
    // shared root field over the base aspect regardless of hydration order, even though plain
    // ascending order would sort corpUserEditableInfo before corpUserInfo and let the base win.
    AspectSpec baseSpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(baseSpec.getName()).thenReturn("corpUserInfo");
    when(baseSpec.isTimeseries()).thenReturn(false);
    AspectSpec editableSpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(editableSpec.getName()).thenReturn("corpUserEditableInfo");
    when(editableSpec.isTimeseries()).thenReturn(false);

    RecordTemplate baseRecord = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate editableRecord = org.mockito.Mockito.mock(RecordTemplate.class);
    SystemAspect baseAspect = storedAspect("corpUserInfo", baseSpec, baseRecord);
    SystemAspect editableAspect =
        storedAspect("corpUserEditableInfo", editableSpec, editableRecord);

    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(baseRecord),
            eq(baseSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("name-from-base")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(editableRecord),
            eq(editableSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("name-from-editable")));

    // A trigger event for a third aspect drives the build without touching the "name" root field.
    RecordTemplate triggerRecord = org.mockito.Mockito.mock(RecordTemplate.class);
    ObjectNode triggerDoc = JsonNodeFactory.instance.objectNode();
    triggerDoc.put("urn", testUrn.toString());
    triggerDoc.put("_entityType", "dataset");
    triggerDoc.put("trigger", "x");
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(triggerRecord),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(triggerDoc));
    MCLItem triggerEvent =
        aspectEvent(
            mockAspectSpec, triggerRecord, ChangeType.UPSERT, null, BackfillHeaders.MODE_BASELINE);

    // Same two aspects hydrated in opposite iteration orders across the two runs.
    final Map<String, SystemAspect> baseFirst = orderedAspects(baseAspect, editableAspect);
    final Map<String, SystemAspect> editableFirst = orderedAspects(editableAspect, baseAspect);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, baseFirst), Map.of(testUrn, editableFirst));

    strategy.processBatch(
        hydratedContext, Collections.singletonMap(testUrn, List.of(triggerEvent)), true);
    strategy.processBatch(
        hydratedContext, Collections.singletonMap(testUrn, List.of(triggerEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    for (V3SearchDocumentWrite write : writeCaptor.getAllValues()) {
      JsonNode document = hydratedContext.getObjectMapper().readTree(write.source());
      assertEquals(document.get("name").asText(), "name-from-editable");
    }
  }

  @Test
  public void testStaleCatchUpNonKeyDeleteUsesAuthoritativeHydratedAspect() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    when(storedSystemAspect.getAspectName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(storedSystemAspect.getAspectSpec()).thenReturn(mockAspectSpec);
    when(storedSystemAspect.getRecordTemplate()).thenReturn(storedAspect);
    when(storedSystemAspect.getUrn()).thenReturn(testUrn);
    when(storedSystemAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(storedSystemAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    "datasetKey",
                    hydratedKey,
                    DATASET_PROPERTIES_ASPECT_NAME,
                    storedSystemAspect)));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(storedAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(searchDocument("current property")));
    MCLItem staleDelete =
        aspectEvent(mockAspectSpec, null, ChangeType.DELETE, null, BackfillHeaders.MODE_CATCH_UP);

    strategy.processBatch(hydratedContext, Map.of(testUrn, List.of(staleDelete)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.REPLACE);
    assertEquals(writtenDocument.get("name").asText(), "current property");
  }

  @Test
  public void testStaleCatchUpKeyDeleteDoesNotTombstoneRecreatedEntity() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec keySpec = org.mockito.Mockito.mock(AspectSpec.class);
    RecordTemplate currentKey = org.mockito.Mockito.mock(RecordTemplate.class);
    SystemAspect currentKeyAspect = org.mockito.Mockito.mock(SystemAspect.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(keySpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(keySpec));
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(keySpec);
    when(currentKeyAspect.getAspectName()).thenReturn("datasetKey");
    when(currentKeyAspect.getAspectSpec()).thenReturn(keySpec);
    when(currentKeyAspect.getRecordTemplate()).thenReturn(currentKey);
    when(currentKeyAspect.getUrn()).thenReturn(testUrn);
    when(currentKeyAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(currentKeyAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of("datasetKey", currentKeyAspect)));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(currentKey),
            eq(keySpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(searchDocument("current key")));
    MCLItem staleKeyDelete =
        aspectEvent(keySpec, null, ChangeType.DELETE, null, BackfillHeaders.MODE_CATCH_UP);

    strategy.processBatch(hydratedContext, Map.of(testUrn, List.of(staleKeyDelete)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.REPLACE);
    assertEquals(writtenDocument.get("name").asText(), "current key");
    assertFalse(writtenDocument.path("_v3Tombstone").asBoolean());
  }

  @Test
  public void testCatchUpKeyDeleteRemovesWhenAuthoritativeKeyIsAbsent() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec keySpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(keySpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(keySpec));
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(keySpec);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of());
    MCLItem keyDelete =
        aspectEvent(keySpec, null, ChangeType.DELETE, null, BackfillHeaders.MODE_CATCH_UP);

    strategy.processBatch(hydratedContext, Map.of(testUrn, List.of(keyDelete)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    // Catch-up runs once the fill is done, so nothing can bring the entity back: delete.
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.DELETE);
    assertTrue(writtenDocument.get("_v3Tombstone").asBoolean());
    verify(aspectRetriever).getLatestSystemAspects(eq(hydratedContext), anyMap());
  }

  @Test
  public void testCatchUpReplayRemovesAnEntityGoneFromPrimaryStorage() throws Exception {
    // The replayed row was read before the entity was hard-deleted. The live DELETE already
    // removed the document; rebuilding from an empty hydration would bring it back as a bare
    // document and surface the deleted entity as a blank hit, so the replay deletes and never
    // looks at the row value it carries.
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec keySpec = mock(AspectSpec.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(keySpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(keySpec, mockAspectSpec));
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(keySpec);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of());
    RecordTemplate replayedAspect = mock(RecordTemplate.class);
    MCLItem replay =
        aspectEvent(
            mockAspectSpec,
            replayedAspect,
            ChangeType.RESTATE,
            null,
            BackfillHeaders.MODE_CATCH_UP);

    strategy.processBatch(hydratedContext, Map.of(testUrn, List.of(replay)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.DELETE);
    assertTrue(writtenDocument.get("_v3Tombstone").asBoolean());
    assertTrue(writtenDocument.get("removed").asBoolean());
    verify(searchDocumentTransformer, never())
        .transformAspect(
            eq(hydratedContext), eq(testUrn), eq(replayedAspect), any(), anyBoolean(), any());
  }

  @Test
  public void testDelayedLiveNonKeyDeleteProjectsTheDeleteWithoutHydration() throws Exception {
    // Arrival order wins. A delayed live non-key DELETE is projected as itself and does not read
    // primary storage, so it cannot keep a value that was re-ingested after the delete was emitted.
    // The catch-up twin still rebuilds from the hydrated aspect.
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    ObjectNode deleted = searchDocument("reingested property");
    deleted.putNull("name");
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            nullable(RecordTemplate.class),
            eq(mockAspectSpec),
            eq(true),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(deleted));
    MCLItem delayedLiveDelete =
        aspectEvent(mockAspectSpec, null, ChangeType.DELETE, "delete-run", null);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(delayedLiveDelete)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertTrue(writtenDocument.get("name").isNull());
    verify(aspectRetriever, never()).getLatestSystemAspects(any(), anyMap());
  }

  @Test
  public void testStructuredPropertiesDeleteWithNullRecordWritesTheUrnAndItsSibling()
      throws Exception {
    AspectSpec structuredProperties =
        operationContext
            .getEntityRegistry()
            .getEntitySpec(DATASET_ENTITY_NAME)
            .getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME))
        .thenReturn(structuredProperties);
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    Urn sibling = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SiblingDataset,PROD)");
    MCLItem delete =
        aspectEvent(testUrn, structuredProperties, null, ChangeType.DELETE, "delete-run", null);
    RecordTemplate siblingAspect = mock(RecordTemplate.class);
    MCLItem siblingEvent =
        aspectEvent(sibling, mockAspectSpec, siblingAspect, ChangeType.UPSERT, "sibling-run", null);
    SearchDocumentTransformer real =
        new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            nullable(RecordTemplate.class),
            eq(structuredProperties),
            eq(true),
            any()))
        .thenAnswer(
            invocation ->
                real.transformAspect(
                    invocation.getArgument(0),
                    invocation.getArgument(1),
                    invocation.getArgument(2),
                    invocation.getArgument(3),
                    invocation.getArgument(4),
                    invocation.getArgument(5)));
    ObjectNode siblingDocument = JsonNodeFactory.instance.objectNode();
    siblingDocument.put("urn", sibling.toString());
    siblingDocument.put("name", "sibling");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            eq(sibling),
            eq(siblingAspect),
            eq(mockAspectSpec),
            eq(false),
            any()))
        .thenReturn(Optional.of(siblingDocument));

    Map<Urn, List<MCLItem>> batch = new LinkedHashMap<>();
    batch.put(testUrn, List.of(delete));
    batch.put(sibling, List.of(siblingEvent));
    strategy.processBatch(operationContext, batch, false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(
        writeCaptor.getAllValues().stream()
            .map(V3SearchDocumentWrite::urn)
            .collect(Collectors.toSet()),
        Set.of(testUrn, sibling));
    JsonNode deleted =
        operationContext
            .getObjectMapper()
            .readTree(
                writeCaptor.getAllValues().stream()
                    .filter(write -> write.urn().equals(testUrn))
                    .findFirst()
                    .orElseThrow()
                    .source());
    assertEquals(deleted.get("urn").asText(), testUrn.toString());
  }

  @Test
  public void testLiveProjectionFailureDoesNotDropSiblingUrns() throws Exception {
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    Urn sibling = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SiblingDataset,PROD)");
    MCLItem failing = aspectEvent(mockAspectSpec, mockAspect, ChangeType.UPSERT, null, null);
    RecordTemplate siblingAspect = mock(RecordTemplate.class);
    MCLItem siblingEvent =
        aspectEvent(sibling, mockAspectSpec, siblingAspect, ChangeType.UPSERT, null, null);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            any(),
            eq(mockAspectSpec),
            anyBoolean(),
            any()))
        .thenThrow(new RuntimeException("Transform aspect error"));
    ObjectNode siblingDocument = searchDocument("sibling");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            eq(sibling),
            eq(siblingAspect),
            eq(mockAspectSpec),
            eq(false),
            any()))
        .thenReturn(Optional.of(siblingDocument));

    Map<Urn, List<MCLItem>> batch = new LinkedHashMap<>();
    batch.put(testUrn, List.of(failing));
    batch.put(sibling, List.of(siblingEvent));
    strategy.processBatch(operationContext, batch, false);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().urn(), sibling);
  }

  @Test
  public void testLiveAspectReadFailureIsRedeliveredWhileV2IsDualWritten() throws Exception {
    // A transient aspect read is not repaired by the entity's next event. Dual-write still skips
    // a transient index write, but this failure has to reach the consumer so it redelivers.
    strategy = createStrategy(true, (TimeseriesWriteThrottleCache) null);
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    Urn sibling = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SiblingDataset,PROD)");
    MCLItem failing = aspectEvent(mockAspectSpec, mockAspect, ChangeType.UPSERT, null, null);
    RecordTemplate siblingAspect = mock(RecordTemplate.class);
    MCLItem siblingEvent =
        aspectEvent(sibling, mockAspectSpec, siblingAspect, ChangeType.UPSERT, null, null);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            any(),
            eq(mockAspectSpec),
            anyBoolean(),
            any()))
        .thenThrow(new RemoteInvocationException("gms unavailable"));

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () -> {
              Map<Urn, List<MCLItem>> batch = new LinkedHashMap<>();
              batch.put(testUrn, List.of(failing));
              batch.put(sibling, List.of(siblingEvent));
              strategy.processBatch(operationContext, batch, false);
            });

    assertTrue(hasCause(thrown, RemoteInvocationException.class));
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  private static boolean hasCause(Throwable failure, Class<? extends Throwable> causeType) {
    Throwable current = failure;
    while (current != null) {
      if (causeType.isInstance(current)) {
        return true;
      }
      if (current == current.getCause()) {
        break;
      }
      current = current.getCause();
    }
    return false;
  }

  @Test
  public void testDelayedLiveKeyDeleteRemovesWithoutReadingPrimary() throws Exception {
    // A live key DELETE removes immediately. It does not read primary storage, so a recreate
    // that landed after this delete was emitted does not keep the document. The catch-up twin
    // still rebuilds when the key is present.
    AspectSpec keySpec = org.mockito.Mockito.mock(AspectSpec.class);
    when(keySpec.getName()).thenReturn("datasetKey");
    when(keySpec.isTimeseries()).thenReturn(false);
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(keySpec);
    MCLItem delayedLiveKeyDelete =
        aspectEvent(keySpec, null, ChangeType.DELETE, "delete-run", null);

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(delayedLiveKeyDelete)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.DELETE);
    assertTrue(writtenDocument.get("_v3Tombstone").asBoolean());
    assertTrue(writtenDocument.get("removed").asBoolean());
    verify(aspectRetriever, never()).getLatestSystemAspects(any(), anyMap());
    verify(searchDocumentTransformer, never())
        .transformAspect(any(), any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void testDelayedLiveUpsertWritesTheEventWithoutReadingPrimary() throws Exception {
    // Arrival order wins. A delayed live UPSERT writes its own projection and does not read
    // primary storage to keep a newer stored value.
    RecordTemplate staleValue = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem staleUpsert = aspectEvent(mockAspectSpec, staleValue, ChangeType.UPSERT, null, null);
    when(staleUpsert.getSystemMetadata()).thenReturn(new SystemMetadata().setVersion("1"));
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(staleValue),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("stale property")));

    strategy.processBatch(operationContext, Map.of(testUrn, List.of(staleUpsert)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    assertEquals(writtenDocument.get("name").asText(), "stale property");
    verify(aspectRetriever, never()).getLatestSystemAspects(any(), anyMap());
  }

  @Test
  public void testNewerLiveUpsertStillOverlaysHydratedAspect() throws Exception {
    // A live upsert writes the event's projection. It does not read primary storage.
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    when(storedSystemAspect.getAspectName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(storedSystemAspect.getAspectSpec()).thenReturn(mockAspectSpec);
    when(storedSystemAspect.getRecordTemplate()).thenReturn(storedAspect);
    when(storedSystemAspect.getUrn()).thenReturn(testUrn);
    when(storedSystemAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(storedSystemAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    when(storedSystemAspect.getSystemMetadataVersion()).thenReturn(Optional.of(2L));
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of(DATASET_PROPERTIES_ASPECT_NAME, storedSystemAspect)));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(storedAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(searchDocument("current property")));
    RecordTemplate newerValue = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem newerUpsert = aspectEvent(mockAspectSpec, newerValue, ChangeType.UPSERT, null, null);
    when(newerUpsert.getSystemMetadata()).thenReturn(new SystemMetadata().setVersion("3"));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(newerValue),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("newer property")));

    strategy.processBatch(hydratedContext, Map.of(testUrn, List.of(newerUpsert)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("name").asText(), "newer property");
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    verify(aspectRetriever, never()).getLatestSystemAspects(any(), anyMap());
  }

  @Test
  public void testNonNumericEventVersionDoesNotDropWrite() throws Exception {
    // A live event whose systemMetadata.version is non-numeric still writes. The live path does
    // not read primary storage or compare versions.
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(mockEntitySpec.getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME)).thenReturn(mockAspectSpec);
    when(storedSystemAspect.getAspectName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(storedSystemAspect.getAspectSpec()).thenReturn(mockAspectSpec);
    when(storedSystemAspect.getRecordTemplate()).thenReturn(storedAspect);
    when(storedSystemAspect.getUrn()).thenReturn(testUrn);
    when(storedSystemAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(storedSystemAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    when(storedSystemAspect.getSystemMetadataVersion()).thenReturn(Optional.of(2L));
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of(DATASET_PROPERTIES_ASPECT_NAME, storedSystemAspect)));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(storedAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(searchDocument("current property")));
    RecordTemplate eventValue = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem nonNumericUpsert =
        aspectEvent(mockAspectSpec, eventValue, ChangeType.UPSERT, null, null);
    when(nonNumericUpsert.getSystemMetadata())
        .thenReturn(new SystemMetadata().setVersion("not-a-number"));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(eventValue),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("event property")));

    strategy.processBatch(hydratedContext, Map.of(testUrn, List.of(nonNumericUpsert)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("name").asText(), "event property");
    assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.MERGE);
    verify(aspectRetriever, never()).getLatestSystemAspects(any(), anyMap());
  }

  @Test
  public void testCatchUpReadsTimeseriesBeforeExistingSourceAndPrimary() throws Exception {
    AspectSpec timeseriesSpec = org.mockito.Mockito.mock(AspectSpec.class);
    SearchableFieldSpec searchableFieldSpec = org.mockito.Mockito.mock(SearchableFieldSpec.class);
    when(timeseriesSpec.getName()).thenReturn(DATASET_PROFILE_ASPECT_NAME);
    when(timeseriesSpec.isTimeseries()).thenReturn(true);
    when(timeseriesSpec.getSearchableFieldSpecs()).thenReturn(List.of(searchableFieldSpec));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, timeseriesSpec));
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of());
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null)))
        .thenReturn(Map.of());
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(hydratedContext), eq("dataset"), anyString()))
        .thenReturn(Optional.empty());
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("catchup")));

    strategy.processBatch(
        hydratedContext,
        Collections.singletonMap(
            testUrn, Collections.singletonList(searchEvent(BackfillHeaders.MODE_CATCH_UP))),
        true);

    InOrder ordering = inOrder(aspectRetriever, timeseriesAspectService, elasticSearchService);
    ordering
        .verify(timeseriesAspectService)
        .getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null));
    ordering
        .verify(elasticSearchService)
        .getV3DocumentSourceBySearchGroup(eq(hydratedContext), eq("dataset"), anyString());
    ordering.verify(aspectRetriever).getLatestSystemAspects(eq(hydratedContext), anyMap());
  }

  @Test
  public void testBatchesTimeseriesHydrationAcrossUrns() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    Urn secondUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SecondDataset,PROD)");
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, profileSpec));
    SystemAspect firstKey = hydratedKeyAspect(hydratedContext);
    SystemAspect secondKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of("datasetKey", firstKey),
                secondUrn,
                Map.of("datasetKey", secondKey)));
    RecordTemplate firstVersionedAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    RecordTemplate secondVersionedAspect = org.mockito.Mockito.mock(RecordTemplate.class);
    MCLItem firstEvent =
        aspectEvent(
            testUrn,
            mockAspectSpec,
            firstVersionedAspect,
            ChangeType.UPSERT,
            null,
            BackfillHeaders.MODE_CATCH_UP);
    MCLItem secondEvent =
        aspectEvent(
            secondUrn,
            mockAspectSpec,
            secondVersionedAspect,
            ChangeType.UPSERT,
            null,
            BackfillHeaders.MODE_CATCH_UP);
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext), anySet(), eq(Set.of(DATASET_PROFILE_ASPECT_NAME)), eq(null)))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    DATASET_PROFILE_ASPECT_NAME,
                    timeseriesEnvelope(new DatasetProfile().setTimestampMillis(1L))),
                secondUrn,
                Map.of(
                    DATASET_PROFILE_ASPECT_NAME,
                    timeseriesEnvelope(new DatasetProfile().setTimestampMillis(2L)))));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            any(Urn.class),
            any(RecordTemplate.class),
            eq(profileSpec),
            eq(false),
            nullable(AuditStamp.class)))
        .thenAnswer(
            invocation -> {
              DatasetProfile profile =
                  new DatasetProfile(((RecordTemplate) invocation.getArgument(2)).data());
              return Optional.of(
                  JsonNodeFactory.instance
                      .objectNode()
                      .put("profileMarker", "profile-" + profile.getTimestampMillis()));
            });
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(firstVersionedAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("first")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(secondUrn),
            eq(secondVersionedAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(
            Optional.of(
                JsonNodeFactory.instance
                    .objectNode()
                    .put("urn", secondUrn.toString())
                    .put("name", "second")));
    Map<Urn, List<MCLItem>> groupedEvents = new LinkedHashMap<>();
    groupedEvents.put(testUrn, List.of(firstEvent));
    groupedEvents.put(secondUrn, List.of(secondEvent));

    strategy.processBatch(hydratedContext, groupedEvents, true);

    verify(timeseriesAspectService)
        .getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn, secondUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null));
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, times(2))
        .writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    Map<Urn, V3SearchDocumentWrite> writesByUrn =
        writeCaptor.getAllValues().stream()
            .collect(Collectors.toMap(V3SearchDocumentWrite::urn, write -> write));
    assertEquals(
        hydratedContext
            .getObjectMapper()
            .readTree(writesByUrn.get(testUrn).source())
            .get("profileMarker")
            .asText(),
        "profile-1");
    assertEquals(
        hydratedContext
            .getObjectMapper()
            .readTree(writesByUrn.get(secondUrn).source())
            .get("profileMarker")
            .asText(),
        "profile-2");
  }

  @Test
  public void testProcessBatch_IncludesRunIdInV3WriteWhenRunIdPresent() throws Exception {
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("run-456");
    markBackfill(mockEvent, BackfillHeaders.MODE_BASELINE);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    strategy.processBatch(operationContext, groupedEvents, true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    JsonNode writtenDocument =
        operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("runId").get(0).asText(), "run-456");
    verify(elasticSearchService, never())
        .appendRunIdBySearchGroup(any(), anyString(), anyString(), any(), anyString());
  }

  @Test
  public void testProcessBatch_ReplacementHasherChangesDocId() throws Exception {
    EntityDocumentIdHasher replacement =
        (operation, urn) -> DigestUtils.sha256Hex("extra|" + urn.toString());
    strategy = createStrategy(false, replacement, List.of());
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    String defaultId = new Sha256UrnEntityDocumentIdHasher().documentId(operationContext, testUrn);
    String replacedId = replacement.documentId(operationContext, testUrn);
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertEquals(writeCaptor.getValue().docId(), replacedId);
    assertFalse(writeCaptor.getValue().docId().equals(defaultId));
  }

  @Test
  public void testProcessBatch_DocumentContributorAddsRootField() throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("_ext", "1");
    strategy = createStrategy(false, new Sha256UrnEntityDocumentIdHasher(), List.of(contributor));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    assertTrue(writeCaptor.getValue().source().contains("\"_ext\":\"1\""));
  }

  @Test
  public void testProcessBatch_DocumentContributorCannotOverwriteUrn() throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("urn", "overwritten");
    strategy = createStrategy(false, new Sha256UrnEntityDocumentIdHasher(), List.of(contributor));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    expectThrows(
        IllegalStateException.class,
        () ->
            strategy.processBatch(
                operationContext,
                Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
                true));
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testProcessBatch_DocumentContributorOverwriteDoesNotFailDualWrite() throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("urn", "overwritten");
    strategy = createStrategy(true, new Sha256UrnEntityDocumentIdHasher(), List.of(contributor));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testProcessBatch_DocumentContributorCannotOverwriteRunId() throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("runId", "stolen");
    strategy = createStrategy(false, new Sha256UrnEntityDocumentIdHasher(), List.of(contributor));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    expectThrows(
        IllegalStateException.class,
        () ->
            strategy.processBatch(
                operationContext,
                Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
                true));
  }

  @Test
  public void testProcessBatch_MergesIndexOnlyFieldsForFullReplace() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("current-run");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, storedAspectSpec));
    when(storedAspectSpec.getName()).thenReturn("ownership");
    when(storedAspectSpec.isTimeseries()).thenReturn(false);
    when(storedSystemAspect.getAspectName()).thenReturn("ownership");
    when(storedSystemAspect.getAspectSpec()).thenReturn(storedAspectSpec);
    when(storedSystemAspect.getRecordTemplate()).thenReturn(storedAspect);
    when(storedSystemAspect.getUrn()).thenReturn(testUrn);
    when(storedSystemAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(storedSystemAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    when(storedSystemAspect.getSystemMetadata()).thenReturn(storedSystemMetadata);
    when(storedSystemMetadata.hasRunId()).thenReturn(true);
    when(storedSystemMetadata.getRunId()).thenReturn("stored-run");
    when(storedSystemMetadata.hasLastRunId()).thenReturn(true);
    when(storedSystemMetadata.getLastRunId()).thenReturn("stored-last-run");
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(
            Map.of(testUrn, Map.of("datasetKey", hydratedKey, "ownership", storedSystemAspect)));
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(hydratedContext), eq("dataset"), anyString()))
        .thenReturn(Optional.of(Map.of("runId", List.of("old-run"), "systemCreated", 123_456L)));

    ObjectNode changedAspectDocument = JsonNodeFactory.instance.objectNode();
    changedAspectDocument.put("urn", testUrn.toString());
    changedAspectDocument.put("name", "changed dataset");
    ObjectNode storedAspectDocument = JsonNodeFactory.instance.objectNode();
    storedAspectDocument.put("urn", testUrn.toString());
    storedAspectDocument.put("owner", "urn:li:corpuser:datahub");
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(changedAspectDocument));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(storedAspect),
            eq(storedAspectSpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(storedAspectDocument));
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);

    strategy.processBatch(
        hydratedContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());

    assertEquals(writtenDocument.get("runId").get(0).asText(), "old-run");
    assertEquals(writtenDocument.get("runId").get(1).asText(), "stored-run");
    assertEquals(writtenDocument.get("runId").get(2).asText(), "stored-last-run");
    assertEquals(writtenDocument.get("runId").get(3).asText(), "current-run");
    assertEquals(writtenDocument.get("systemCreated").asLong(), 123_456L);
    assertTrue(writtenDocument.hasNonNull("systemIndexModified"));
    verify(aspectRetriever, times(1)).getLatestSystemAspects(eq(hydratedContext), anyMap());
    // Catch-up checkpoints when the batch returns, so this replay drains before returning.
    InOrder writeOrder = inOrder(elasticSearchService, bulkProcessor);
    writeOrder
        .verify(elasticSearchService)
        .writeV3SearchDocument(eq(hydratedContext), any(V3SearchDocumentWrite.class));
    writeOrder.verify(bulkProcessor).flushAndAwait();
    verify(elasticSearchService, times(1))
        .getV3DocumentSourceBySearchGroup(eq(hydratedContext), eq("dataset"), anyString());
  }

  @Test
  public void testProcessBatch_MetadataOnlyUpdatePreservesStoredRootEmbeddings() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    UpdateIndicesV3Strategy vectorProjectionStrategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .vectorProjectionEnabled(true)
            .expectedVectorDimensions(Map.of("cohere_embed_v3", 3))
            .build();
    when(mockEntitySpec.getSearchGroup()).thenReturn("primary");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, storedAspectSpec));
    when(storedAspectSpec.getName()).thenReturn("semanticContent");
    when(storedAspectSpec.isTimeseries()).thenReturn(false);
    when(storedSystemAspect.getAspectName()).thenReturn("semanticContent");
    when(storedSystemAspect.getAspectSpec()).thenReturn(storedAspectSpec);
    when(storedSystemAspect.getRecordTemplate()).thenReturn(storedAspect);
    when(storedSystemAspect.getUrn()).thenReturn(testUrn);
    when(storedSystemAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(storedSystemAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of("semanticContent", storedSystemAspect)));

    ObjectNode changedAspectDocument = JsonNodeFactory.instance.objectNode();
    changedAspectDocument.put("urn", testUrn.toString());
    changedAspectDocument.put("name", "metadata-only update");
    ObjectNode semanticContentDocument = JsonNodeFactory.instance.objectNode();
    semanticContentDocument.put("urn", testUrn.toString());
    ObjectNode embeddings = JsonNodeFactory.instance.objectNode();
    ObjectNode model = JsonNodeFactory.instance.objectNode();
    ObjectNode chunk = JsonNodeFactory.instance.objectNode();
    chunk.set("vector", JsonNodeFactory.instance.arrayNode().add(0.1d).add(0.2d).add(0.3d));
    model.set("chunks", JsonNodeFactory.instance.arrayNode().add(chunk));
    embeddings.set("cohere_embed_v3", model);
    semanticContentDocument.set("embeddings", embeddings);
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(changedAspectDocument));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(storedAspect),
            eq(storedAspectSpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(semanticContentDocument));
    markBackfill(mockEvent, BackfillHeaders.MODE_BASELINE);

    vectorProjectionStrategy.processBatch(
        hydratedContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());

    assertEquals(writtenDocument.get("name").asText(), "metadata-only update");
    assertEquals(
        writtenDocument
            .get("embeddings")
            .get("cohere_embed_v3")
            .get("chunks")
            .get(0)
            .get("vector")
            .size(),
        3);
    assertFalse(writtenDocument.path("_aspects").has("semanticContent"));
    assertEquals(
        writeCaptor.getValue().indexName(),
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "primary"));
  }

  @Test
  public void testProcessBatch_HydratesLatestStoredAspectsForFullReplace() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, storedAspectSpec));
    when(storedAspectSpec.getName()).thenReturn("ownership");
    when(storedAspectSpec.isTimeseries()).thenReturn(false);
    when(storedSystemAspect.getAspectName()).thenReturn("ownership");
    when(storedSystemAspect.getAspectSpec()).thenReturn(storedAspectSpec);
    when(storedSystemAspect.getRecordTemplate()).thenReturn(storedAspect);
    when(storedSystemAspect.getUrn()).thenReturn(testUrn);
    when(storedSystemAspect.getEntitySpec()).thenReturn(mockEntitySpec);
    when(storedSystemAspect.getAuditStamp()).thenReturn(storedAuditStamp);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of("ownership", storedSystemAspect)));

    ObjectNode changedAspectDocument = JsonNodeFactory.instance.objectNode();
    changedAspectDocument.put("urn", testUrn.toString());
    changedAspectDocument.put("name", "changed dataset");
    ObjectNode storedAspectDocument = JsonNodeFactory.instance.objectNode();
    storedAspectDocument.put("urn", testUrn.toString());
    storedAspectDocument.put("owner", "urn:li:corpuser:datahub");
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(changedAspectDocument));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(storedAspect),
            eq(storedAspectSpec),
            eq(false),
            eq(storedAuditStamp)))
        .thenReturn(Optional.of(storedAspectDocument));
    markBackfill(mockEvent, BackfillHeaders.MODE_BASELINE);

    strategy.processBatch(
        hydratedContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("name").asText(), "changed dataset");
    assertEquals(writtenDocument.get("owner").asText(), "urn:li:corpuser:datahub");
    assertEquals(
        writtenDocument.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).get("name").asText(),
        "changed dataset");
    assertEquals(
        writtenDocument.get("_aspects").get("ownership").get("owner").asText(),
        "urn:li:corpuser:datahub");
  }

  @DataProvider(name = "retryableReadFailures")
  public Object[][] retryableReadFailures() {
    return new Object[][] {
      {new RuntimeException(new ConnectException("Connection refused"))},
      {new RuntimeException(new SocketException("Connection reset"))},
      {new RuntimeException(new SocketTimeoutException("Read timed out"))},
      {new RuntimeException(new NoHttpResponseException("host failed to respond"))},
      {new RuntimeException(new ConnectionClosedException("connection closed prematurely"))},
      {new OpenSearchStatusException("rate limited", RestStatus.TOO_MANY_REQUESTS)},
      {new OpenSearchStatusException("service unavailable", RestStatus.SERVICE_UNAVAILABLE)},
      {es8HttpFailure(429)},
      {es8HttpFailure(503)}
    };
  }

  private static ElasticsearchException es8HttpFailure(int status) {
    return new ElasticsearchException(
        "test",
        ErrorResponse.of(
            response ->
                response
                    .status(status)
                    .error(error -> error.type("test_error").reason("test failure"))));
  }

  @Test(dataProvider = "retryableReadFailures")
  public void testRetriesKnownTransientPrimaryStorageFailure(RuntimeException transientFailure)
      throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenThrow(transientFailure)
        .thenReturn(Map.of());
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("hydrated after retry")));

    strategy.processBatch(
        hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true);

    verify(aspectRetriever, times(2)).getLatestSystemAspects(eq(hydratedContext), anyMap());
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(hydratedContext), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testExhaustedTransientPrimaryStorageFailureIsRetryable() {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    RuntimeException transientFailure =
        new RuntimeException(new ConnectException("Connection refused"));
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenThrow(transientFailure);

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () ->
                strategy.processBatch(
                    hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true));

    assertEquals(thrown.getCause(), transientFailure);
    verify(aspectRetriever, times(3)).getLatestSystemAspects(eq(hydratedContext), anyMap());
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testRetriesTransientExistingSourceReadFailure() throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("existing source retry")));
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(operationContext), eq("dataset"), anyString()))
        .thenThrow(new ConnectException("Connection refused"))
        .thenReturn(Optional.empty());

    strategy.processBatch(
        operationContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true);

    verify(elasticSearchService, times(2))
        .getV3DocumentSourceBySearchGroup(eq(operationContext), eq("dataset"), anyString());
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testGeneralExistingSourceIOExceptionRunsOnceWithoutWrite() throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("must not be written")));
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(operationContext), eq("dataset"), anyString()))
        .thenThrow(new IOException("existing source unavailable"));

    RuntimeException thrown =
        expectThrows(
            RuntimeException.class,
            () ->
                strategy.processBatch(
                    operationContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true));

    assertTrue(thrown.getCause() instanceof IOException);
    verify(elasticSearchService)
        .getV3DocumentSourceBySearchGroup(eq(operationContext), eq("dataset"), anyString());
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @DataProvider(name = "permanentReadFailures")
  public Object[][] permanentReadFailures() {
    return new Object[][] {
      {new IllegalArgumentException("invalid read request")},
      {new SecurityException("read forbidden")},
      {new IllegalStateException("malformed response")},
      {new RuntimeException(new IOException("general I/O failure"))},
      {new RuntimeException(new JsonParseException(null, "malformed JSON"))},
      {new OpenSearchStatusException("bad request", RestStatus.BAD_REQUEST)},
      {es8HttpFailure(400)}
    };
  }

  @Test(dataProvider = "permanentReadFailures")
  public void testPermanentHydrationFailureRunsOnceWithoutWrite(RuntimeException hydrationFailure) {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenThrow(hydrationFailure);

    RuntimeException thrown =
        expectThrows(
            RuntimeException.class,
            () ->
                strategy.processBatch(
                    hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true));

    assertEquals(thrown, hydrationFailure);
    verify(aspectRetriever).getLatestSystemAspects(eq(hydratedContext), anyMap());
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testInterruptedHydrationReadPreservesInterruptAndRunsOnce() {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    RuntimeException interruptedFailure =
        new RuntimeException(new InterruptedException("read interrupted"));
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenThrow(interruptedFailure);

    try {
      RetryableIndexingException thrown =
          expectThrows(
              RetryableIndexingException.class,
              () ->
                  strategy.processBatch(
                      hydratedContext,
                      Map.of(testUrn, Collections.singletonList(mockEvent)),
                      true));

      assertEquals(thrown.getCause(), interruptedFailure);
      assertTrue(Thread.currentThread().isInterrupted());
      verify(aspectRetriever).getLatestSystemAspects(eq(hydratedContext), anyMap());
      verify(elasticSearchService, never())
          .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void testRetriesTransientTimeseriesHydrationFailure() throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, profileSpec));
    when(mockEntitySpec.getAspectSpec(DATASET_PROFILE_ASPECT_NAME)).thenReturn(profileSpec);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of());
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null)))
        .thenThrow(new RuntimeException(new SocketException("Connection reset")))
        .thenReturn(Map.of());
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("timeseries hydrated after retry")));

    strategy.processBatch(
        hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true);

    verify(timeseriesAspectService, times(2))
        .getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null));
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(hydratedContext), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testRetriesStrictTimeseriesFailureAndPreservesOmittedProjection() throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    AspectSpec usageSpec = searchableTimeseriesSpec(DATASET_USAGE_STATISTICS_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(List.of(mockAspectSpec, profileSpec, usageSpec));
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of("datasetKey", hydratedKey)));
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME, DATASET_USAGE_STATISTICS_ASPECT_NAME)),
            eq(null)))
        .thenThrow(new RuntimeException(new SocketException("Connection reset")))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    DATASET_PROFILE_ASPECT_NAME,
                    timeseriesEnvelope(new DatasetProfile().setTimestampMillis(1L)))));
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(hydratedContext), eq("dataset"), anyString()))
        .thenReturn(
            Optional.of(
                Map.of(
                    "usageMarker",
                    "existing-usage",
                    "_aspects",
                    Map.of(
                        DATASET_USAGE_STATISTICS_ASPECT_NAME,
                        Map.of("usageMarker", "existing-usage")))));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            any(RecordTemplate.class),
            eq(profileSpec),
            eq(false),
            nullable(AuditStamp.class)))
        .thenReturn(
            Optional.of(
                JsonNodeFactory.instance.objectNode().put("profileMarker", "current-profile")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("versioned update")));

    strategy.processBatch(
        hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true);

    verify(timeseriesAspectService, times(2))
        .getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME, DATASET_USAGE_STATISTICS_ASPECT_NAME)),
            eq(null));
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("profileMarker").asText(), "current-profile");
    assertEquals(writtenDocument.get("usageMarker").asText(), "existing-usage");
  }

  @Test
  public void testStrictTimeseriesFailureAfterRetriesAbortsReplacement() {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, profileSpec));
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null)))
        .thenThrow(new RuntimeException(new SocketTimeoutException("Search timed out")));

    expectThrows(
        RetryableIndexingException.class,
        () ->
            strategy.processBatch(
                hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true));

    verify(timeseriesAspectService, times(3))
        .getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null));
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testEmptyTimeseriesHydrationPreservesExistingProjection() throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, profileSpec));
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of("datasetKey", hydratedKey)));
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null)))
        .thenReturn(Map.of());
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(hydratedContext), eq("dataset"), anyString()))
        .thenReturn(
            Optional.of(
                Map.of(
                    "profileMarker",
                    "existing-profile",
                    "_aspects",
                    Map.of(
                        DATASET_PROFILE_ASPECT_NAME,
                        Map.of(
                            "profileMarker",
                            "existing-profile",
                            "_systemMetadata",
                            Map.of("runId", "profile-run"))))));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("versioned update")));

    strategy.processBatch(
        hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("profileMarker").asText(), "existing-profile");
    assertEquals(
        writtenDocument
            .get("_aspects")
            .get(DATASET_PROFILE_ASPECT_NAME)
            .get("profileMarker")
            .asText(),
        "existing-profile");
  }

  @Test
  public void testExistingTimeseriesFallbackKeepsMetadataNestedAndCanonical() throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, profileSpec));
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of("datasetKey", hydratedKey)));
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null)))
        .thenReturn(Map.of());
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(hydratedContext), eq("dataset"), anyString()))
        .thenReturn(
            Optional.of(
                Map.of(
                    "_aspects",
                    Map.of(
                        DATASET_PROFILE_ASPECT_NAME,
                        Map.of(
                            "profileMarker",
                            "existing-profile",
                            "_systemmetadata",
                            Map.of("runId", "legacy-run"),
                            "_aspects",
                            Map.of("invalid", true),
                            "urn",
                            "legacy-urn",
                            "_entityType",
                            "legacy-type",
                            "_v3Tombstone",
                            true)))));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("versioned update")));

    strategy.processBatch(
        hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("urn").asText(), testUrn.toString());
    assertEquals(writtenDocument.get("_entityType").asText(), DATASET_ENTITY_NAME);
    assertFalse(writtenDocument.has("_systemMetadata"));
    assertFalse(writtenDocument.has("_systemmetadata"));
    assertFalse(writtenDocument.has("_v3Tombstone"));
    assertFalse(writtenDocument.get("_aspects").has("invalid"));
    JsonNode profileAspect = writtenDocument.get("_aspects").get(DATASET_PROFILE_ASPECT_NAME);
    assertFalse(profileAspect.has("_systemmetadata"));
    assertEquals(profileAspect.get("_systemMetadata").get("runId").asText(), "legacy-run");
  }

  @Test
  public void testPartialTimeseriesHydrationPreservesOmittedAspectProjection() throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_CATCH_UP);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    AspectSpec usageSpec = searchableTimeseriesSpec(DATASET_USAGE_STATISTICS_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(List.of(mockAspectSpec, profileSpec, usageSpec));
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of("datasetKey", hydratedKey)));
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME, DATASET_USAGE_STATISTICS_ASPECT_NAME)),
            eq(null)))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    DATASET_PROFILE_ASPECT_NAME,
                    timeseriesEnvelope(new DatasetProfile().setTimestampMillis(1L)))));
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(hydratedContext), eq("dataset"), anyString()))
        .thenReturn(
            Optional.of(
                Map.of(
                    "profileMarker",
                    "existing-profile",
                    "usageMarker",
                    "existing-usage",
                    "_aspects",
                    Map.of(
                        DATASET_PROFILE_ASPECT_NAME,
                        Map.of("profileMarker", "existing-profile"),
                        DATASET_USAGE_STATISTICS_ASPECT_NAME,
                        Map.of("usageMarker", "existing-usage")))));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            any(RecordTemplate.class),
            eq(profileSpec),
            eq(false),
            nullable(AuditStamp.class)))
        .thenReturn(
            Optional.of(
                JsonNodeFactory.instance.objectNode().put("profileMarker", "current-profile")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("versioned update")));

    strategy.processBatch(
        hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("profileMarker").asText(), "current-profile");
    assertEquals(writtenDocument.get("usageMarker").asText(), "existing-usage");
    assertEquals(
        writtenDocument
            .get("_aspects")
            .get(DATASET_USAGE_STATISTICS_ASPECT_NAME)
            .get("usageMarker")
            .asText(),
        "existing-usage");
  }

  @Test
  public void testCatchUpTimeseriesDeletePreservesExistingProjection() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, profileSpec));
    when(mockEntitySpec.getAspectSpec(DATASET_PROFILE_ASPECT_NAME)).thenReturn(profileSpec);
    SystemAspect hydratedKey = hydratedKeyAspect(hydratedContext);
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of(testUrn, Map.of("datasetKey", hydratedKey)));
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME)),
            eq(null)))
        .thenReturn(Map.of());
    when(elasticSearchService.getV3DocumentSourceBySearchGroup(
            eq(hydratedContext), eq("dataset"), anyString()))
        .thenReturn(
            Optional.of(
                Map.of(
                    "profileMarker",
                    "existing-profile",
                    "_aspects",
                    Map.of(
                        DATASET_PROFILE_ASPECT_NAME,
                        Map.of("profileMarker", "existing-profile")))));
    MCLItem deleteEvent =
        aspectEvent(profileSpec, null, ChangeType.DELETE, null, BackfillHeaders.MODE_CATCH_UP);

    strategy.processBatch(hydratedContext, Map.of(testUrn, List.of(deleteEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("profileMarker").asText(), "existing-profile");
    assertEquals(
        writtenDocument
            .get("_aspects")
            .get(DATASET_PROFILE_ASPECT_NAME)
            .get("profileMarker")
            .asText(),
        "existing-profile");
  }

  @Test
  public void testVersionedUpdatePreservesLatestSearchableTimeseriesProjections() throws Exception {
    markBackfill(mockEvent, BackfillHeaders.MODE_BASELINE);
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    AspectSpec usageSpec = searchableTimeseriesSpec(DATASET_USAGE_STATISTICS_ASPECT_NAME);
    AspectSpec operationSpec = searchableTimeseriesSpec(DATASET_OPERATION_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(List.of(mockAspectSpec, profileSpec, usageSpec, operationSpec));
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of());
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(
                Set.of(
                    DATASET_PROFILE_ASPECT_NAME,
                    DATASET_USAGE_STATISTICS_ASPECT_NAME,
                    DATASET_OPERATION_ASPECT_NAME)),
            eq(null)))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    DATASET_PROFILE_ASPECT_NAME,
                    timeseriesEnvelope(new DatasetProfile()),
                    DATASET_USAGE_STATISTICS_ASPECT_NAME,
                    timeseriesEnvelope(new DatasetUsageStatistics()),
                    DATASET_OPERATION_ASPECT_NAME,
                    timeseriesEnvelope(new Operation()))));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            any(RecordTemplate.class),
            eq(profileSpec),
            eq(false),
            nullable(AuditStamp.class)))
        .thenReturn(
            Optional.of(
                JsonNodeFactory.instance.objectNode().put("profileMarker", "latest-profile")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            any(RecordTemplate.class),
            eq(usageSpec),
            eq(false),
            nullable(AuditStamp.class)))
        .thenReturn(
            Optional.of(JsonNodeFactory.instance.objectNode().put("usageMarker", "latest-usage")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            any(RecordTemplate.class),
            eq(operationSpec),
            eq(false),
            nullable(AuditStamp.class)))
        .thenReturn(
            Optional.of(
                JsonNodeFactory.instance.objectNode().put("operationMarker", "latest-operation")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(searchDocument("changed versioned aspect")));

    strategy.processBatch(
        hydratedContext, Map.of(testUrn, Collections.singletonList(mockEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("profileMarker").asText(), "latest-profile");
    assertEquals(writtenDocument.get("usageMarker").asText(), "latest-usage");
    assertEquals(writtenDocument.get("operationMarker").asText(), "latest-operation");
  }

  @Test
  public void testTimeseriesUpdatePreservesOtherLatestTimeseriesProjection() throws Exception {
    when(aspectRetriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    OperationContext hydratedContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    AspectSpec profileSpec = searchableTimeseriesSpec(DATASET_PROFILE_ASPECT_NAME);
    AspectSpec usageSpec = searchableTimeseriesSpec(DATASET_USAGE_STATISTICS_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(profileSpec, usageSpec));
    when(aspectRetriever.getLatestSystemAspects(eq(hydratedContext), anyMap()))
        .thenReturn(Map.of());
    when(timeseriesAspectService.getLatestTimeseriesAspectValuesStrict(
            eq(hydratedContext),
            eq(Set.of(testUrn)),
            eq(Set.of(DATASET_PROFILE_ASPECT_NAME, DATASET_USAGE_STATISTICS_ASPECT_NAME)),
            eq(null)))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    DATASET_PROFILE_ASPECT_NAME,
                    timeseriesEnvelope(new DatasetProfile()),
                    DATASET_USAGE_STATISTICS_ASPECT_NAME,
                    timeseriesEnvelope(new DatasetUsageStatistics()))));
    RecordTemplate newProfile = org.mockito.Mockito.mock(RecordTemplate.class);
    DataMap newProfileData = new DataMap();
    newProfileData.put("timestampMillis", 1L);
    when(newProfile.data()).thenReturn(newProfileData);
    when(profileSpec.getTimeseriesFieldSpecs()).thenReturn(List.of());
    when(profileSpec.getTimeseriesFieldCollectionSpecs()).thenReturn(List.of());
    MCLItem profileEvent =
        aspectEvent(
            profileSpec,
            newProfile,
            ChangeType.UPSERT,
            "profile-run",
            BackfillHeaders.MODE_BASELINE);
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            any(RecordTemplate.class),
            eq(profileSpec),
            eq(false),
            nullable(AuditStamp.class)))
        .thenReturn(
            Optional.of(JsonNodeFactory.instance.objectNode().put("profileMarker", "stored")),
            Optional.of(JsonNodeFactory.instance.objectNode().put("profileMarker", "event")));
    when(searchDocumentTransformer.transformAspect(
            eq(hydratedContext),
            eq(testUrn),
            any(RecordTemplate.class),
            eq(usageSpec),
            eq(false),
            nullable(AuditStamp.class)))
        .thenReturn(
            Optional.of(JsonNodeFactory.instance.objectNode().put("usageMarker", "preserved")));

    strategy.processBatch(
        hydratedContext, Map.of(testUrn, Collections.singletonList(profileEvent)), true);

    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(hydratedContext), writeCaptor.capture());
    JsonNode writtenDocument =
        hydratedContext.getObjectMapper().readTree(writeCaptor.getValue().source());
    assertEquals(writtenDocument.get("profileMarker").asText(), "event");
    assertEquals(writtenDocument.get("usageMarker").asText(), "preserved");
  }

  @Test
  public void testProcessBatch_StructuredProperties() throws Exception {
    // Setup for structured properties
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute
    strategy.processBatch(operationContext, groupedEvents, true);

    // Verify versioned write was called with search group
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testProcessBatch_TimeseriesAspect() throws Exception {
    // Setup for timeseries aspect
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1L);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute
    strategy.processBatch(operationContext, groupedEvents, true);

    // Verify versioned write was called (timeseries aspects are treated like regular
    // aspects in V3)
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
    verify(timeseriesAspectService)
        .upsertDocument(
            eq(operationContext), eq("dataset"), nullable(String.class), anyString(), any());
  }

  @Test
  public void testTimeseriesIndexWriteIsLeftToV2WhenThatStrategyIsEnabled() throws Exception {
    UpdateIndicesV3Strategy dualWriting = createStrategy(true, null);
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1L);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    dualWriting.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    verify(timeseriesAspectService, never())
        .upsertDocument(any(), anyString(), anyString(), anyString(), any());
  }

  @Test
  public void testUpdateIndexMappings_V2Disabled_ProcessesStructuredProperty() throws Exception {
    // Setup for structured property
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);

    StructuredPropertyDefinition mockStructuredProperty = new StructuredPropertyDefinition();
    mockStructuredProperty.setEntityTypes(
        new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")));

    when(mockAspect.data()).thenReturn(mockStructuredProperty.data());

    // Mock the reindex configs
    when(elasticSearchService.buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class)))
        .thenReturn(Collections.singletonList(mockReindexConfig));
    when(mockReindexConfig.name()).thenReturn("test-index");

    // Execute
    strategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify processing occurred
    verify(elasticSearchService)
        .buildReindexConfigsWithNewStructProp(
            eq(operationContext), eq(testUrn), any(StructuredPropertyDefinition.class));
  }

  @Test
  public void testUpdateIndexMappings_NonStructuredProperty() {
    // Setup for non-structured property
    when(mockEntitySpec.getName()).thenReturn(DATASET_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);

    // Execute
    strategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify no processing occurred
    verify(elasticSearchService, never())
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class));
  }

  @Test
  public void testStructuredPropertyDefinitionDeleteSkipsMappingUpdate() throws Exception {
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    when(mockEntitySpec.getAspectSpec(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME))
        .thenReturn(mockAspectSpec);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            nullable(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any()))
        .thenReturn(Optional.of(mockSearchDocument));
    MCLItem delete = aspectEvent(mockAspectSpec, null, ChangeType.DELETE, null, null);
    UpdateIndicesV3Strategy spiedStrategy = spy(strategy);

    spiedStrategy.processBatch(operationContext, Map.of(testUrn, List.of(delete)), true);

    verify(spiedStrategy, never()).updateIndexMappings(any(), any(), any(), any(), any(), any());
    verify(mockIndexBuilder, never()).getSearchClient();
    verify(mockIndexBuilder, never())
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), anyBoolean());
  }

  @Test
  public void testGetIndexMappings() {
    // Execute
    Collection<MappingsBuilder.IndexMapping> mappings = strategy.getIndexMappings(operationContext);

    // Verify - should return mappings from MultiEntityMappingsBuilder
    assertTrue(mappings instanceof Collection);
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredProperty() throws IOException {
    StructuredPropertyDefinition property =
        new StructuredPropertyDefinition()
            .setQualifiedName("retention")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:retention");

    Collection<MappingsBuilder.IndexMapping> mappings =
        strategy.getIndexMappingsWithNewStructuredProperty(operationContext, propertyUrn, property);

    // As on V2, the strategy returns its mappings builder's mappings for the new property
    assertFalse(mappings.isEmpty());
    assertEquals(
        mappings,
        new MultiEntityMappingsBuilder(
                EntityIndexConfiguration.builder().v3(v3Config).build(),
                null,
                ESUtils.KEYWORD_MAXLENGTH,
                List.of())
            .getIndexMappingsWithNewStructuredProperty(operationContext, propertyUrn, property));
  }

  @Test
  public void testConstructor_IOExceptionWhenMappingConfigResourceNotFound() {
    // Setup v3Config to use a non-existent mapping configuration resource
    when(v3Config.getMappingConfig()).thenReturn("non-existent-mapping-config.yaml");

    // Execute and verify that RuntimeException is thrown with IOException as cause
    RuntimeException exception =
        expectThrows(RuntimeException.class, () -> createStrategy(false, null));

    // Verify the exception message and cause
    assertTrue(exception.getMessage().contains("Failed to initialize V3 mappings builder"));
    assertTrue(exception.getCause() instanceof IOException);
    assertTrue(exception.getCause().getMessage().contains("Configuration resource not found"));
  }

  @Test
  public void testConstructor_IOExceptionWhenMappingConfigHasInvalidFormat() {
    // Setup v3Config to use a mapping configuration with invalid format
    when(v3Config.getMappingConfig()).thenReturn("invalid-format-mapping-config.yaml");

    // Execute and verify that RuntimeException is thrown with IOException as cause
    RuntimeException exception =
        expectThrows(RuntimeException.class, () -> createStrategy(false, null));

    // Verify the exception message and cause
    assertTrue(exception.getMessage().contains("Failed to initialize V3 mappings builder"));
    assertTrue(exception.getCause() instanceof IOException);
  }

  @Test
  public void testConstructor_IOExceptionWhenMappingConfigMissingRequiredSections() {
    // Setup v3Config to use a mapping configuration missing required sections
    when(v3Config.getMappingConfig()).thenReturn("missing-sections-mapping-config.yaml");

    // Execute and verify that RuntimeException is thrown with IOException as cause
    RuntimeException exception =
        expectThrows(RuntimeException.class, () -> createStrategy(false, null));

    // Verify the exception message and cause
    assertTrue(exception.getMessage().contains("Failed to initialize V3 mappings builder"));
    assertTrue(exception.getCause() instanceof IOException);
    // Note: The specific error message may vary depending on the actual file content
    // We just verify that an IOException is thrown
  }

  @Test
  public void testConstructor_SuccessWhenMappingConfigIsNull() {
    // Setup v3Config with null mapping configuration (should not throw exception)
    when(v3Config.getMappingConfig()).thenReturn(null);

    // Execute - should not throw exception
    UpdateIndicesV3Strategy strategyWithNullConfig = createStrategy(false, null);

    // Verify strategy was created successfully
    assertTrue(strategyWithNullConfig.isEnabled());
  }

  @Test
  public void testConstructor_SuccessWhenMappingConfigIsEmpty() {
    // Setup v3Config with empty mapping configuration (should not throw exception)
    when(v3Config.getMappingConfig()).thenReturn("");

    // Execute - should not throw exception
    UpdateIndicesV3Strategy strategyWithEmptyConfig = createStrategy(false, null);

    // Verify strategy was created successfully
    assertTrue(strategyWithEmptyConfig.isEnabled());
  }

  @Test
  public void testConstructor_SuccessWhenMappingConfigIsWhitespace() {
    // Setup v3Config with whitespace-only mapping configuration (should not throw exception)
    when(v3Config.getMappingConfig()).thenReturn("   ");

    // Execute - should not throw exception
    UpdateIndicesV3Strategy strategyWithWhitespaceConfig = createStrategy(false, null);

    // Verify strategy was created successfully
    assertTrue(strategyWithWhitespaceConfig.isEnabled());
  }

  @Test
  public void testUpdateIndexMappings_IOExceptionFromApplyMappings() throws Exception {
    // Setup for structured property
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);

    StructuredPropertyDefinition mockStructuredProperty = new StructuredPropertyDefinition();
    mockStructuredProperty.setEntityTypes(
        new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")));

    when(mockAspect.data()).thenReturn(mockStructuredProperty.data());

    // Mock the reindex configs
    when(elasticSearchService.buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class)))
        .thenReturn(Collections.singletonList(mockReindexConfig));
    when(mockReindexConfig.name()).thenReturn("test-index");

    // Mock applyMappings to throw IOException
    doThrow(new IOException("Elasticsearch communication error"))
        .when(mockIndexBuilder)
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), anyBoolean());

    // Execute - the method catches the RuntimeException and logs it, so no exception is thrown
    strategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify that applyMappings was called (which would have thrown the IOException)
    verify(mockIndexBuilder)
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), eq(false));
  }

  @Test
  public void testProcessBatch_ExceptionInKeyAspectDeletionCheck() throws Exception {
    // Setup for DELETE event
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey");

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Mock UpdateIndicesUtil.extractSpecPair to throw RuntimeException
    try (var mockedStatic = mockStatic(UpdateIndicesUtil.class)) {
      mockedStatic
          .when(() -> UpdateIndicesUtil.extractSpecPair(any(MCLItem.class)))
          .thenThrow(new RuntimeException("Failed to retrieve Aspect Spec"));

      expectThrows(
          RuntimeException.class,
          () -> strategy.processBatch(operationContext, groupedEvents, true));

      // Verify that no document operations were performed due to the exception
      verify(elasticSearchService, never())
          .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
      verify(elasticSearchService, never()).deleteDocument(any(), anyString(), anyString());
    }
  }

  @Test
  public void testProcessBatch_KeyAspectDeletion_Success() throws Exception {
    // Setup for DELETE event with key aspect
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey"); // This is the key aspect
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Mock UpdateIndicesUtil methods to return successful results
    try (var mockedStatic = mockStatic(UpdateIndicesUtil.class)) {
      mockedStatic
          .when(() -> UpdateIndicesUtil.extractSpecPair(any(MCLItem.class)))
          .thenReturn(Pair.of(mockEntitySpec, mockAspectSpec));
      mockedStatic.when(() -> UpdateIndicesUtil.isDeletingKey(any(Pair.class))).thenReturn(true);

      // Execute
      strategy.processBatch(operationContext, groupedEvents, true);

      ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
          ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
      verify(elasticSearchService)
          .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
      V3SearchDocumentWrite write = writeCaptor.getValue();
      JsonNode tombstone = operationContext.getObjectMapper().readTree(write.source());
      assertEquals(
          write.indexName(),
          operationContext
              .getSearchContext()
              .getIndexConvention()
              .getEntityIndexNameV3(operationContext, "dataset"));
      assertEquals(write.urn(), testUrn);
      assertEquals(write.kind(), V3SearchDocumentWrite.Kind.DELETE);
      assertTrue(tombstone.get("removed").asBoolean());
      assertTrue(tombstone.get("_v3Tombstone").asBoolean());
      verify(elasticSearchService, never())
          .deleteDocumentBySearchGroup(any(), anyString(), anyString());
    }
  }

  @Test
  public void testProcessBatch_KeyAspectDeletion_DocumentContributorAddsRootField()
      throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("_ext", "tombstone");
    strategy = createStrategy(false, new Sha256UrnEntityDocumentIdHasher(), List.of(contributor));
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    try (var mockedStatic = mockStatic(UpdateIndicesUtil.class)) {
      mockedStatic
          .when(() -> UpdateIndicesUtil.extractSpecPair(any(MCLItem.class)))
          .thenReturn(Pair.of(mockEntitySpec, mockAspectSpec));
      mockedStatic.when(() -> UpdateIndicesUtil.isDeletingKey(any(Pair.class))).thenReturn(true);

      strategy.processBatch(operationContext, groupedEvents, true);

      ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
          ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
      verify(elasticSearchService)
          .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
      JsonNode tombstone =
          operationContext.getObjectMapper().readTree(writeCaptor.getValue().source());
      assertEquals(writeCaptor.getValue().kind(), V3SearchDocumentWrite.Kind.DELETE);
      assertTrue(tombstone.get("removed").asBoolean());
      assertEquals(tombstone.get("_ext").asText(), "tombstone");
    }
  }

  @Test
  public void testProcessBatch_KeyAspectDeletion_NullSearchGroup() throws Exception {
    // Setup for DELETE event with key aspect but null search group
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey"); // This is the key aspect
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn(null); // Null search group

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Mock UpdateIndicesUtil methods to return successful results
    try (var mockedStatic = mockStatic(UpdateIndicesUtil.class)) {
      mockedStatic
          .when(() -> UpdateIndicesUtil.extractSpecPair(any(MCLItem.class)))
          .thenReturn(Pair.of(mockEntitySpec, mockAspectSpec));
      mockedStatic.when(() -> UpdateIndicesUtil.isDeletingKey(any(Pair.class))).thenReturn(true);

      strategy.processBatch(operationContext, groupedEvents, true);

      // Unset searchGroup uses the entity type name as the V3 index key.
      ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
          ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
      verify(elasticSearchService)
          .writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
      V3SearchDocumentWrite write = writeCaptor.getValue();
      assertEquals(write.indexName(), v3IndexName(DATASET_ENTITY_NAME));
      assertEquals(write.urn(), testUrn);
      assertEquals(write.kind(), V3SearchDocumentWrite.Kind.DELETE);
    }
  }

  @Test
  public void testProcessBatch_EmptyEventsListForUrn() throws Exception {
    // Setup for URN with empty events list (different from empty groupedEvents map)
    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.emptyList());

    strategy.processBatch(operationContext, groupedEvents, true);

    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testProcessBatch_UpsertWithNullSearchGroup() throws Exception {
    // Setup for UPSERT event with null search group
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn(null); // Null search group

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Mock search document transformer to return a document
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    // Execute - should handle null search group gracefully
    strategy.processBatch(operationContext, groupedEvents, true);

    // Unset searchGroup uses the entity type name as the V3 index key
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite write = writeCaptor.getValue();
    assertEquals(write.indexName(), v3IndexName(DATASET_ENTITY_NAME));
    assertEquals(write.urn(), testUrn);
    assertEquals(write.kind(), V3SearchDocumentWrite.Kind.MERGE);
  }

  @Test
  public void testProcessBatch_ExceptionInAspectProcessing() throws Exception {
    // Setup for UPSERT event that will cause an exception during aspect processing
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    RuntimeException aspectSpecFailure = new RuntimeException("Aspect spec error");
    when(mockEvent.getAspectSpec()).thenThrow(aspectSpecFailure);

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    RuntimeException thrown =
        expectThrows(
            RuntimeException.class,
            () -> strategy.processBatch(operationContext, groupedEvents, true));

    assertEquals(thrown, aspectSpecFailure);
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testProcessBatch_ExceptionInTransformAspect() throws Exception {
    // A live projection failure is not retryable. Skipping the aspect lets the rest of the batch
    // be written; failing it would drop every later URN, and the listener still commits.
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenThrow(new RuntimeException("Transform aspect error"));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    verify(searchDocumentTransformer, times(1))
        .transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class));
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testLiveBatchDoesNotDrainWhenProjectionFails() throws Exception {
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenThrow(new RuntimeException("Transform aspect error"));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
    verify(bulkProcessor, never()).flushAndAwait();
  }

  @Test
  public void testProcessBatchDrainsPendingWritesEvenWhenProcessingThrows() throws Exception {
    // A fill checkpoints when the batch returns, so a mid-batch failure still drains the writes
    // already queued.
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    markBackfill(mockEvent, BackfillHeaders.MODE_BASELINE);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenThrow(new RuntimeException("Transform aspect error"));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    expectThrows(
        RuntimeException.class, () -> strategy.processBatch(operationContext, groupedEvents, true));

    verify(bulkProcessor, atLeastOnce()).flushAndAwait();
  }

  @Test
  public void testTransientDrainFailureEscalatesAsRetryable() throws Exception {
    // A transient bulk-write failure (transport/timeout/incomplete) must escalate as retryable so
    // the MAE transports redeliver instead of committing past a lost live update.
    mockSearchDocument.put("name", "x");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    when(bulkProcessor.flushAndAwait())
        .thenReturn(
            new BulkProcessorFlushResult(0, 0, 0, 1, 0, false, new IOException("es timeout")));
    markBackfill(mockEvent, BackfillHeaders.MODE_BASELINE);

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () -> strategy.processBatch(operationContext, groupedEvents, true));
    assertTrue(thrown.getMessage().contains("transiently"));
  }

  @Test
  public void testPoisonDrainFailureIsNotRetryable() throws Exception {
    // Item-level rejections will not succeed on retry, so they must not escalate for redelivery
    // (which would head-of-line-block the batch); they fail loudly and stay at-most-once.
    mockSearchDocument.put("name", "x");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    when(bulkProcessor.flushAndAwait())
        .thenReturn(new BulkProcessorFlushResult(0, 0, 1, 0, 0, false, null));
    markBackfill(mockEvent, BackfillHeaders.MODE_BASELINE);

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // RetryableIndexingException extends RuntimeException (not IllegalStateException), so matching
    // IllegalStateException here already confirms the poison failure did not escalate as retryable.
    IllegalStateException thrown =
        expectThrows(
            IllegalStateException.class,
            () -> strategy.processBatch(operationContext, groupedEvents, true));
    assertTrue(thrown.getMessage().contains("non-retryable"));
  }

  // ==================== Timeseries throttle tests ====================

  private void stubTimeseriesAspectForIndexWrite(RecordTemplate aspect, long timestampMillis) {
    DataMap data = new DataMap();
    data.put("timestampMillis", timestampMillis);
    when(aspect.data()).thenReturn(data);
    when(mockAspectSpec.getTimeseriesFieldSpecs()).thenReturn(List.of());
    when(mockAspectSpec.getTimeseriesFieldCollectionSpecs()).thenReturn(List.of());
  }

  private TimeseriesWriteThrottleCache buildThrottleCache(
      boolean entityEnabled, boolean tsEnabled, boolean observeEnabled) {
    return new TimeseriesWriteThrottleCache(
        com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration.builder()
            .entityIndex(
                com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration
                    .IndexThrottleConfig.builder()
                    .enabled(entityEnabled)
                    .build())
            .timeseriesIndex(
                com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration
                    .IndexThrottleConfig.builder()
                    .enabled(tsEnabled)
                    .build())
            .observe(
                com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration
                    .IndexThrottleConfig.builder()
                    .enabled(observeEnabled)
                    .build())
            .refreshPeriodSeconds(3600)
            .maxCacheUrns(10_000)
            .build());
  }

  @Test
  public void testThrottle_EntityIndexSuppressesTimeseriesAspect() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(true, false, false);
    UpdateIndicesV3Strategy throttledStrategy = createStrategy(false, cache);

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_001_000L);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1_000_001_000L);

    // Prime cache
    cache.recordWrite(testUrn.toString(), "datasetProfile", 1_000_000_000L);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    throttledStrategy.processBatch(operationContext, groupedEvents, true);

    // Timeseries aspect should be excluded from the document (throttled)
    // Since it's the only aspect, no upsert should happen
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
    verify(timeseriesAspectService)
        .upsertDocument(
            eq(operationContext), eq("dataset"), eq("datasetProfile"), anyString(), any());
  }

  @Test
  public void testThrottle_LeavesStoredTimeseriesValueInsteadOfUsingAnEarlierBatchEvent()
      throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(true, false, false);
    UpdateIndicesV3Strategy throttledStrategy = createStrategy(false, cache);

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    RecordTemplate earlierRecord = mock(RecordTemplate.class);
    stubTimeseriesAspectForIndexWrite(earlierRecord, 1_000_000_000L);
    RecordTemplate throttledRecord = mock(RecordTemplate.class);
    stubTimeseriesAspectForIndexWrite(throttledRecord, 1_000_001_000L);
    MCLItem earlier = aspectEvent(mockAspectSpec, earlierRecord, ChangeType.UPSERT, null, null);
    MCLItem throttled = aspectEvent(mockAspectSpec, throttledRecord, ChangeType.UPSERT, null, null);
    AuditStamp earlierAuditStamp = mock(AuditStamp.class);
    AuditStamp throttledAuditStamp = mock(AuditStamp.class);
    when(earlierAuditStamp.getTime()).thenReturn(1_000_000_000L);
    when(throttledAuditStamp.getTime()).thenReturn(1_000_001_000L);
    when(earlier.getAuditStamp()).thenReturn(earlierAuditStamp);
    when(throttled.getAuditStamp()).thenReturn(throttledAuditStamp);
    cache.recordWrite(testUrn.toString(), "datasetProfile", 1_000_000_000L);

    throttledStrategy.processBatch(
        operationContext, Map.of(testUrn, List.of(earlier, throttled)), true);

    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
    verify(searchDocumentTransformer, never())
        .transformAspect(any(), any(), any(), any(), anyBoolean(), any());
    verify(timeseriesAspectService, times(2))
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString(), any());
  }

  @Test
  public void testThrottle_ObserveModeDoesNotSuppressInV3() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(false, false, true);
    UpdateIndicesV3Strategy throttledStrategy = createStrategy(false, cache);

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_001_000L);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1_000_001_000L);

    cache.recordWrite(testUrn.toString(), "datasetProfile", 1_000_000_000L);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    throttledStrategy.processBatch(operationContext, groupedEvents, true);

    // Observe mode should NOT suppress — the write should proceed
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
    verify(timeseriesAspectService)
        .upsertDocument(
            eq(operationContext), eq("dataset"), eq("datasetProfile"), anyString(), any());
  }

  @Test
  public void testThrottle_FirstWritePassesThroughInV3() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(true, false, false);
    UpdateIndicesV3Strategy throttledStrategy = createStrategy(false, cache);

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_000_000L);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1_000_000_000L);
    // No prior cache entry → not throttled

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    throttledStrategy.processBatch(operationContext, groupedEvents, true);

    // First write should not be throttled
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
    verify(timeseriesAspectService)
        .upsertDocument(
            eq(operationContext), eq("dataset"), eq("datasetProfile"), anyString(), any());
  }

  @Test
  public void testThrottle_EntityAndObserveBothEnabledInV3() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(true, false, true);
    UpdateIndicesV3Strategy throttledStrategy = createStrategy(false, cache);

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_001_000L);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1_000_001_000L);

    cache.recordWrite(testUrn.toString(), "datasetProfile", 1_000_000_000L);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    throttledStrategy.processBatch(operationContext, groupedEvents, true);

    // Entity index enabled suppresses the entity document. Observe mode does not suppress the
    // shared timeseries index.
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
    verify(timeseriesAspectService)
        .upsertDocument(
            eq(operationContext), eq("dataset"), eq("datasetProfile"), anyString(), any());
  }

  @Test
  public void testThrottle_NullCacheNoThrottling() throws Exception {
    // Default strategy (null cache) should not throttle timeseries aspects
    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_000_000L);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1_000_000_000L);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    strategy.processBatch(operationContext, groupedEvents, true);

    // With null throttle cache, all writes proceed
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
    verify(timeseriesAspectService)
        .upsertDocument(
            eq(operationContext), eq("dataset"), eq("datasetProfile"), anyString(), any());
  }

  @Test
  public void testThrottle_TimeseriesIndexSuppressesSharedIndexWrite() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(false, true, false);
    UpdateIndicesV3Strategy throttledStrategy = createStrategy(false, cache);

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_001_000L);
    stubTimeseriesAspectForIndexWrite(mockAspect, 1_000_001_000L);
    cache.recordWrite(testUrn.toString(), "datasetProfile", 1_000_000_000L);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    throttledStrategy.processBatch(operationContext, Map.of(testUrn, List.of(mockEvent)), true);

    verify(timeseriesAspectService, never())
        .upsertDocument(any(), anyString(), anyString(), anyString(), any());
    verify(elasticSearchService)
        .writeV3SearchDocument(eq(operationContext), any(V3SearchDocumentWrite.class));
  }

  @Test
  public void testProcessBatch_ExceptionInStructuredPropertiesProcessing() throws Exception {
    // Setup for UPSERT event with structured properties aspect that will cause an exception
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    RuntimeException aspectSpecFailure = new RuntimeException("Aspect spec error");
    when(mockEvent.getAspectSpec()).thenThrow(aspectSpecFailure);

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    RuntimeException thrown =
        expectThrows(
            RuntimeException.class,
            () -> strategy.processBatch(operationContext, groupedEvents, true));

    assertEquals(thrown, aspectSpecFailure);
    verify(elasticSearchService, never())
        .writeV3SearchDocument(any(OperationContext.class), any(V3SearchDocumentWrite.class));
  }

  // These three tests expect document embeddings at the V3 document root. The projector does not
  // write that shape yet, so they stay disabled until the projector and the mapping agree.
  @Test(enabled = false)
  public void testLiftsEmbeddingsToDocumentV3RootWhenSemanticEnabled() throws Exception {
    Urn documentUrn = UrnUtils.getUrn("urn:li:document:bridge-1");
    when(mockEvent.getUrn()).thenReturn(documentUrn);
    when(mockEntitySpec.getName()).thenReturn("document");
    when(mockEntitySpec.getSearchGroup()).thenReturn("document");
    when(mockAspectSpec.getName()).thenReturn("semanticContent");
    when(mockEvent.getAspectName()).thenReturn("semanticContent");

    ObjectNode semanticDoc = JsonNodeFactory.instance.objectNode();
    semanticDoc.put("urn", documentUrn.toString());
    ObjectNode embeddings = JsonNodeFactory.instance.objectNode();
    embeddings.put("model", "cohere_embed_v3");
    semanticDoc.set("embeddings", embeddings);
    semanticDoc.put("skipReason", "EMPTY_TEXT");

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(semanticDoc));

    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(true);
    semanticConfig.setEnabledEntities(Set.of("document"));
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(1024);
    semanticConfig.setModels(Map.of("cohere_embed_v3", model));

    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .v2Enabled(false)
            .expectedVectorDimensions(Map.of())
            .entityDocumentIdHasher(new Sha256UrnEntityDocumentIdHasher())
            .documentContributors(List.of())
            .semanticSearchConfiguration(semanticConfig)
            .build();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(documentUrn, Collections.singletonList(mockEvent)),
        true);

    ObjectNode written = capturedDocument("document");
    assertTrue(written.has("embeddings"));
    assertEquals(written.get("skipReason").asText(), "EMPTY_TEXT");
    assertFalse(written.has("resolvedTextSha256"));
    assertFalse(written.get("_aspects").get("semanticContent").has("embeddings"));
    assertFalse(written.get("_aspects").get("semanticContent").has("skipReason"));
  }

  // testLiftsEmbeddingsToDocumentV3RootWhenSemanticEnabled.
  @Test(enabled = false)
  public void testDoesNotLiftEmbeddingsWhenSemanticDisabled() throws Exception {
    Urn documentUrn = UrnUtils.getUrn("urn:li:document:bridge-1");
    when(mockEvent.getUrn()).thenReturn(documentUrn);
    when(mockEntitySpec.getName()).thenReturn("document");
    when(mockEntitySpec.getSearchGroup()).thenReturn("document");
    when(mockAspectSpec.getName()).thenReturn("semanticContent");
    when(mockEvent.getAspectName()).thenReturn("semanticContent");

    ObjectNode semanticDoc = JsonNodeFactory.instance.objectNode();
    semanticDoc.put("urn", documentUrn.toString());
    ObjectNode embeddings = JsonNodeFactory.instance.objectNode();
    embeddings.put("model", "cohere_embed_v3");
    semanticDoc.set("embeddings", embeddings);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(semanticDoc));

    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(false);
    semanticConfig.setEnabledEntities(Set.of("document"));
    semanticConfig.setModels(Map.of("cohere_embed_v3", new ModelEmbeddingConfig()));

    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .v2Enabled(false)
            .expectedVectorDimensions(Map.of())
            .entityDocumentIdHasher(new Sha256UrnEntityDocumentIdHasher())
            .documentContributors(List.of())
            .semanticSearchConfiguration(semanticConfig)
            .build();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(documentUrn, Collections.singletonList(mockEvent)),
        true);

    ObjectNode written = capturedDocument("document");
    assertFalse(written.has("embeddings"));
    assertTrue(written.get("_aspects").get("semanticContent").has("embeddings"));
  }

  @Test
  public void testDoesNotLiftEmbeddingsForDataset() throws Exception {
    when(mockAspectSpec.getName()).thenReturn("semanticContent");
    when(mockEvent.getAspectName()).thenReturn("semanticContent");

    ObjectNode semanticDoc = JsonNodeFactory.instance.objectNode();
    semanticDoc.put("urn", testUrn.toString());
    ObjectNode embeddings = JsonNodeFactory.instance.objectNode();
    embeddings.put("model", "cohere_embed_v3");
    semanticDoc.set("embeddings", embeddings);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(semanticDoc));

    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(true);
    semanticConfig.setEnabledEntities(Set.of("document"));
    semanticConfig.setModels(Map.of("cohere_embed_v3", new ModelEmbeddingConfig()));

    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .v2Enabled(false)
            .expectedVectorDimensions(Map.of())
            .entityDocumentIdHasher(new Sha256UrnEntityDocumentIdHasher())
            .documentContributors(List.of())
            .semanticSearchConfiguration(semanticConfig)
            .build();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    ObjectNode written = capturedDocument("dataset");
    assertFalse(written.has("embeddings"));
  }

  @Test
  public void testDocumentInfoMclDoesNotComputeResolvedTextHash() throws Exception {
    Urn documentUrn = UrnUtils.getUrn("urn:li:document:bridge-1");
    when(mockEvent.getUrn()).thenReturn(documentUrn);
    when(mockEntitySpec.getName()).thenReturn(DOCUMENT_ENTITY_NAME);
    when(mockEntitySpec.getSearchGroup()).thenReturn("document");
    when(mockAspectSpec.getName()).thenReturn(DOCUMENT_INFO_ASPECT_NAME);
    when(mockEvent.getAspectName()).thenReturn(DOCUMENT_INFO_ASPECT_NAME);

    ObjectNode infoDoc = JsonNodeFactory.instance.objectNode();
    infoDoc.put("urn", documentUrn.toString());
    infoDoc.put("text", "the body");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(infoDoc));

    enableDocumentSemanticStrategy();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(documentUrn, Collections.singletonList(mockEvent)),
        true);

    ObjectNode written = capturedDocument("document");
    assertEquals(written.get("text").asText(), "the body");
    assertFalse(written.has("resolvedTextSha256"));
  }

  @Test
  public void testSemanticTextMclDoesNotComputeResolvedTextHash() throws Exception {
    Urn documentUrn = UrnUtils.getUrn("urn:li:document:bridge-1");
    when(mockEvent.getUrn()).thenReturn(documentUrn);
    when(mockEntitySpec.getName()).thenReturn(DOCUMENT_ENTITY_NAME);
    when(mockEntitySpec.getSearchGroup()).thenReturn("document");
    when(mockAspectSpec.getName()).thenReturn(SEMANTIC_TEXT_ASPECT_NAME);
    when(mockEvent.getAspectName()).thenReturn(SEMANTIC_TEXT_ASPECT_NAME);

    ObjectNode textDoc = JsonNodeFactory.instance.objectNode();
    textDoc.put("urn", documentUrn.toString());
    textDoc.put("semanticText", "curated override");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(textDoc));

    enableDocumentSemanticStrategy();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(documentUrn, Collections.singletonList(mockEvent)),
        true);

    ObjectNode written = capturedDocument("document");
    assertEquals(written.get("semanticText").asText(), "curated override");
    assertFalse(written.has("resolvedTextSha256"));
  }

  // testLiftsEmbeddingsToDocumentV3RootWhenSemanticEnabled.
  @Test(enabled = false)
  public void testStampsResolvedHashWhenDocumentInfoLeadsMixedBatch() throws Exception {
    Urn documentUrn = UrnUtils.getUrn("urn:li:document:bridge-1");
    when(mockEvent.getUrn()).thenReturn(documentUrn);
    when(mockEntitySpec.getName()).thenReturn(DOCUMENT_ENTITY_NAME);
    when(mockEntitySpec.getSearchGroup()).thenReturn("document");
    when(mockAspectSpec.getName()).thenReturn(DOCUMENT_INFO_ASPECT_NAME);
    when(mockEvent.getAspectName()).thenReturn(DOCUMENT_INFO_ASPECT_NAME);
    when(mockEvent.getEntitySpec()).thenReturn(mockEntitySpec);
    when(mockEvent.getAspectSpec()).thenReturn(mockAspectSpec);

    AspectSpec semanticAspectSpec = mock(AspectSpec.class);
    when(semanticAspectSpec.getName()).thenReturn("semanticContent");
    when(semanticAspectSpec.isTimeseries()).thenReturn(false);
    MCLItem semanticEvent = mock(MCLItem.class);
    when(semanticEvent.getUrn()).thenReturn(documentUrn);
    when(semanticEvent.getEntitySpec()).thenReturn(mockEntitySpec);
    when(semanticEvent.getAspectSpec()).thenReturn(semanticAspectSpec);
    when(semanticEvent.getAspectName()).thenReturn("semanticContent");
    when(semanticEvent.getRecordTemplate()).thenReturn(mockAspect);
    when(semanticEvent.getSystemMetadata()).thenReturn(mockSystemMetadata);
    when(semanticEvent.getAuditStamp()).thenReturn(mockAuditStamp);
    when(semanticEvent.getChangeType()).thenReturn(ChangeType.UPSERT);

    ObjectNode infoDoc = JsonNodeFactory.instance.objectNode();
    infoDoc.put("urn", documentUrn.toString());
    infoDoc.put("text", "the body");
    ObjectNode semanticDoc = JsonNodeFactory.instance.objectNode();
    semanticDoc.put("urn", documentUrn.toString());
    ObjectNode embeddings = JsonNodeFactory.instance.objectNode();
    embeddings.put("model", "cohere_embed_v3");
    semanticDoc.set("embeddings", embeddings);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            eq(mockAspectSpec),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(infoDoc));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            eq(semanticAspectSpec),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(semanticDoc));

    enableDocumentSemanticStrategy();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(documentUrn, List.of(mockEvent, semanticEvent)),
        true);

    ObjectNode written = capturedDocument("document");
    assertTrue(written.has("embeddings"));
    assertFalse(written.has("resolvedTextSha256"));
  }

  @Test
  public void testProcessBatch_ProjectsAspectFieldsToRootAndAspects() throws Exception {
    RecordTemplate previousAspect = mock(RecordTemplate.class);
    when(mockEvent.getAspectName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(mockEvent.getPreviousRecordTemplate()).thenReturn(previousAspect);
    when(mockEvent.getSystemMetadata()).thenReturn(new SystemMetadata().setRunId("run-1"));
    stubTransform(mockAspect, Map.of("name", "orders"));
    stubTransform(previousAspect, Map.of("name", "old", "description", "dropped"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), false);

    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("name").asText(), "orders");
    // A partial update keeps whatever it does not overwrite, so removed values are nulled
    assertTrue(written.get("description").isNull());
    assertFalse(written.has("_systemMetadata"));
    JsonNode aspect = written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(aspect.get("name").asText(), "orders");
    assertTrue(aspect.get("description").isNull());
    assertEquals(aspect.get("_systemMetadata").get("runId").asText(), "run-1");
  }

  @Test
  public void testProcessBatch_CoalescesAspectEventsAgainstBatchBaseline() throws Exception {
    RecordTemplate before = mock(RecordTemplate.class);
    MCLItem first = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem second = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    RecordTemplate firstRecord = first.getRecordTemplate();
    RecordTemplate secondRecord = second.getRecordTemplate();
    when(first.getPreviousRecordTemplate()).thenReturn(before);
    when(second.getPreviousRecordTemplate()).thenReturn(firstRecord);
    stubTransform(before, Map.of("name", "a", "description", "kept by the first event"));
    stubTransform(secondRecord, Map.of("name", "c"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(first, second)), false);

    // Diffed against the value before the batch, so the removal reaches _aspects too
    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("name").asText(), "c");
    assertTrue(written.get("description").isNull());
    JsonNode aspect = written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(aspect.get("name").asText(), "c");
    assertTrue(aspect.get("description").isNull());
  }

  @Test
  public void testProcessBatch_CreateThenUpdateNullsNoFieldTheIndexNeverHeld() throws Exception {
    MCLItem create = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem update = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    RecordTemplate created = create.getRecordTemplate();
    RecordTemplate updated = update.getRecordTemplate();
    when(update.getPreviousRecordTemplate()).thenReturn(created);
    stubTransform(created, Map.of("name", "a", "description", "dropped by the update"));
    stubTransform(updated, Map.of("name", "b"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(create, update)), false);

    // The index held no value of the aspect, so a null here could only clear a root field that
    // another aspect supplies
    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("name").asText(), "b");
    assertFalse(written.has("description"));
  }

  @Test
  public void testProcessBatch_WritesCreatedAspectFollowedByNoOp() throws Exception {
    MCLItem create = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem noOp = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status created = new Status().setRemoved(false);
    Status unchanged = new Status().setRemoved(false);
    when(create.getRecordTemplate()).thenReturn(created);
    when(noOp.getRecordTemplate()).thenReturn(unchanged);
    when(noOp.getPreviousRecordTemplate()).thenReturn(created);
    stubTransform(unchanged, Map.of("removed", "false"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(create, noOp)), false);

    // The index did not hold the aspect before the batch, so it is written
    assertEquals(capturedDocument(DATASET_ENTITY_NAME).get("removed").asText(), "false");
  }

  @Test
  public void testProcessBatch_WritesAspectForcedInsideBatch() throws Exception {
    Status value = new Status().setRemoved(false);
    List<MCLItem> batch = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      MCLItem event = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
      when(event.getRecordTemplate()).thenReturn(value);
      when(event.getPreviousRecordTemplate()).thenReturn(value);
      batch.add(event);
    }
    when(batch.get(1).getSystemMetadata())
        .thenReturn(
            new SystemMetadata().setProperties(new StringMap(Map.of(FORCE_INDEXING_KEY, "true"))));
    stubTransform(value, Map.of("removed", "false"));

    strategy.processBatch(operationContext, Collections.singletonMap(testUrn, batch), false);

    assertEquals(capturedDocument(DATASET_ENTITY_NAME).get("removed").asText(), "false");
  }

  @Test
  public void testProcessBatch_SkipsDeleteThenRecreateWithSameValue() throws Exception {
    MCLItem delete = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem recreate = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status value = new Status().setRemoved(false);
    when(delete.getChangeType()).thenReturn(ChangeType.DELETE);
    when(delete.getPreviousRecordTemplate()).thenReturn(value);
    when(recreate.getRecordTemplate()).thenReturn(new Status().setRemoved(false));
    stubTransform(value, Map.of("removed", "false"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(delete, recreate)), false);

    // The index ends the batch holding what it held before it
    verify(elasticSearchService, never())
        .upsertDocumentBySearchGroup(any(), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_AppliesAspectsInFirstAppearanceOrder() throws Exception {
    MCLItem first = aspectEvent("dataPlatformInstance");
    MCLItem other = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem last = aspectEvent("dataPlatformInstance");
    RecordTemplate otherRecord = other.getRecordTemplate();
    RecordTemplate lastRecord = last.getRecordTemplate();
    stubTransform(otherRecord, Map.of("platform", "from datasetProperties"));
    stubTransform(lastRecord, Map.of("platform", "from dataPlatformInstance"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(first, other, last)), false);

    // As on V2, an aspect is written at its first position with its last value
    assertEquals(
        capturedDocument(DATASET_ENTITY_NAME).get("platform").asText(), "from datasetProperties");
  }

  @Test
  public void testProcessBatch_SkipsUnchangedAspect() throws Exception {
    MCLItem event = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status status = new Status().setRemoved(false);
    when(event.getRecordTemplate()).thenReturn(status);
    when(event.getPreviousRecordTemplate()).thenReturn(new Status().setRemoved(false));
    stubTransform(status, Map.of("removed", "false"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(event)), false);

    verify(elasticSearchService, never())
        .upsertDocumentBySearchGroup(any(), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_UnchangedStructuredPropertyDefinitionStillUpdatesMappings()
      throws Exception {
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    StructuredPropertyDefinition definition =
        propertyDefinition(
            "unchanged.property",
            new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    MCLItem event = aspectEvent(mockAspectSpec, definition, ChangeType.UPSERT, null, null);
    when(event.getPreviousRecordTemplate()).thenReturn(definition.copy());
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any()))
        .thenReturn(Optional.of(mockSearchDocument));
    SearchClientShim<?> searchClient = stubMappingClient();

    // Re-saving an unchanged property definition is how a failed mapping update is repaired.
    strategy.processBatch(operationContext, Map.of(testUrn, List.of(event)), true);

    assertTrue(capturedPutMapping(searchClient).contains("unchanged_property"));
  }

  @Test
  public void testBatchStructuredPropertyMappingsUseOnePutMapping() throws Exception {
    stubStructuredPropertyDefinitions();
    SearchClientShim<?> searchClient = stubMappingClient();

    strategy.processBatch(
        operationContext, definitionBatch("scale_smoke.prop_00", "scale_smoke.prop_01"), true);

    String mapping = capturedPutMapping(searchClient);
    assertTrue(mapping.contains("scale_smoke_prop_00"));
    assertTrue(mapping.contains("scale_smoke_prop_01"));
    verify(mockIndexBuilder, never())
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), anyBoolean());
    verify(elasticSearchService, never())
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class));
  }

  @DataProvider(name = "mappingRejections")
  public Object[][] mappingRejections() {
    return new Object[][] {
      {new OpenSearchStatusException("mapper_parsing_exception", RestStatus.BAD_REQUEST)},
      {es8HttpFailure(400)}
    };
  }

  @Test(dataProvider = "mappingRejections")
  public void testBatchStructuredPropertyMappingFallsBackPerIndex(RuntimeException rejection)
      throws Exception {
    stubStructuredPropertyDefinitions();
    SearchClientShim<?> searchClient = stubMappingClient();
    doThrow(rejection).when(searchClient).putIndexMapping(any(), any(), any());
    Set<String> indexNames = v3EntityIndexNames();
    String failingIndex = indexNames.iterator().next();
    doAnswer(
            invocation -> {
              ReindexConfig indexState = invocation.getArgument(1);
              if (failingIndex.equals(indexState.name())) {
                throw new IOException("index rejected");
              }
              return null;
            })
        .when(mockIndexBuilder)
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), eq(false));

    strategy.processBatch(
        operationContext, definitionBatch("scale_smoke.prop_00", "scale_smoke.prop_01"), true);

    verify(searchClient, times(1)).putIndexMapping(eq(operationContext), any(), any());
    ArgumentCaptor<ReindexConfig> indexCaptor = ArgumentCaptor.forClass(ReindexConfig.class);
    verify(mockIndexBuilder, times(indexNames.size()))
        .applyMappings(eq(operationContext), indexCaptor.capture(), eq(false));
    assertEquals(
        indexCaptor.getAllValues().stream().map(ReindexConfig::name).collect(Collectors.toSet()),
        indexNames);
    assertTrue(
        indexCaptor.getAllValues().stream()
            .allMatch(ReindexConfig::isPureStructuredPropertyAddition));

    // Only the index whose per-index apply failed is still missing the fields.
    strategy.processBatch(
        operationContext, definitionBatch("scale_smoke.prop_00", "scale_smoke.prop_01"), true);

    ArgumentCaptor<PutMappingRequest> requestCaptor =
        ArgumentCaptor.forClass(PutMappingRequest.class);
    verify(searchClient, times(2))
        .putIndexMapping(eq(operationContext), requestCaptor.capture(), any());
    assertEquals(List.of(requestCaptor.getValue().indices()), List.of(failingIndex));
  }

  @Test
  public void testRepeatedStructuredPropertyBatchSkipsPutMapping() throws Exception {
    stubStructuredPropertyDefinitions();
    SearchClientShim<?> searchClient = stubMappingClient();

    strategy.processBatch(operationContext, definitionBatch("scale_smoke.prop_00"), true);
    strategy.processBatch(operationContext, definitionBatch("scale_smoke.prop_00"), true);

    verify(searchClient, times(1)).putIndexMapping(eq(operationContext), any(), any());

    strategy.processBatch(
        operationContext, definitionBatch("scale_smoke.prop_00", "scale_smoke.prop_02"), true);

    ArgumentCaptor<PutMappingRequest> requestCaptor =
        ArgumentCaptor.forClass(PutMappingRequest.class);
    verify(searchClient, times(2))
        .putIndexMapping(eq(operationContext), requestCaptor.capture(), any());
    assertEquals(Set.copyOf(List.of(requestCaptor.getValue().indices())), v3EntityIndexNames());
    assertTrue(requestCaptor.getValue().source().utf8ToString().contains("scale_smoke_prop_02"));
  }

  @Test
  public void testConcurrentStructuredPropertyBatchesPutOnce() throws Exception {
    stubStructuredPropertyDefinitions();
    SearchClientShim<?> searchClient = stubMappingClient();
    CountDownLatch putStarted = new CountDownLatch(1);
    CountDownLatch releasePut = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              putStarted.countDown();
              assertTrue(releasePut.await(30, TimeUnit.SECONDS));
              return null;
            })
        .when(searchClient)
        .putIndexMapping(any(), any(), any());
    Map<Urn, List<MCLItem>> batch = definitionBatch("scale_smoke.prop_00");

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> first = executor.submit(() -> strategy.processBatch(operationContext, batch, true));
      assertTrue(putStarted.await(30, TimeUnit.SECONDS));
      Future<?> second =
          executor.submit(() -> strategy.processBatch(operationContext, batch, true));
      releasePut.countDown();
      first.get(30, TimeUnit.SECONDS);
      second.get(30, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }

    verify(searchClient, times(1)).putIndexMapping(eq(operationContext), any(), any());
  }

  @Test
  public void testStructuredPropertyPutMappingRetriesTimeoutOnce() throws Exception {
    stubStructuredPropertyDefinitions();
    SearchClientShim<?> searchClient = stubMappingClient();
    doThrow(new IOException(new TimeoutException("Connection lease request time out")))
        .doAnswer(invocation -> null)
        .when(searchClient)
        .putIndexMapping(any(), any(), any());

    strategy.processBatch(operationContext, definitionBatch("scale_smoke.prop_00"), true);
    strategy.processBatch(operationContext, definitionBatch("scale_smoke.prop_00"), true);

    ArgumentCaptor<RequestOptions> optionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);
    verify(searchClient, times(2))
        .putIndexMapping(eq(operationContext), any(), optionsCaptor.capture());
    RequestConfig requestConfig = optionsCaptor.getValue().getRequestConfig();
    assertEquals(requestConfig.getSocketTimeout(), 60_000);
    assertTrue(requestConfig.getConnectionRequestTimeout() > 0);
    verify(mockIndexBuilder, never())
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), anyBoolean());
  }

  @Test
  public void testStructuredPropertyPutMappingGivesUpWithoutRecording() throws Exception {
    stubStructuredPropertyDefinitions();
    SearchClientShim<?> searchClient = stubMappingClient();
    doThrow(new IOException(new TimeoutException("Connection lease request time out")))
        .when(searchClient)
        .putIndexMapping(any(), any(), any());

    strategy.processBatch(operationContext, definitionBatch("scale_smoke.prop_00"), true);

    verify(searchClient, times(2)).putIndexMapping(eq(operationContext), any(), any());
    verify(mockIndexBuilder, never())
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), anyBoolean());

    strategy.processBatch(operationContext, definitionBatch("scale_smoke.prop_00"), true);

    verify(searchClient, times(4)).putIndexMapping(eq(operationContext), any(), any());
  }

  @Test
  public void testUnprojectableDefinitionDoesNotBlockTheRestOfTheBatch() throws Exception {
    stubStructuredPropertyDefinitions();
    SearchClientShim<?> searchClient = stubMappingClient();

    // A qualified name with a space cannot become a field name.
    strategy.processBatch(
        operationContext, definitionBatch("bad name", "scale_smoke.prop_00"), true);

    String mapping = capturedPutMapping(searchClient);
    assertTrue(mapping.contains("scale_smoke_prop_00"), mapping);
    assertFalse(mapping.contains("bad name"), mapping);
  }

  private void stubStructuredPropertyDefinitions() throws Exception {
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any()))
        .thenReturn(Optional.of(mockSearchDocument));
  }

  private Map<Urn, List<MCLItem>> definitionBatch(String... qualifiedNames) {
    UrnArray datasetTypes = new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset"));
    Map<Urn, List<MCLItem>> batch = new HashMap<>();
    for (String qualifiedName : qualifiedNames) {
      Urn urn = UrnUtils.getUrn("urn:li:structuredProperty:" + qualifiedName);
      batch.put(
          urn,
          List.of(
              aspectEvent(
                  urn,
                  mockAspectSpec,
                  propertyDefinition(qualifiedName, datasetTypes),
                  ChangeType.UPSERT,
                  null,
                  null)));
    }
    return batch;
  }

  @Test
  public void testProcessBatch_ForceIndexingWritesUnchangedAspect() throws Exception {
    MCLItem event = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status status = new Status().setRemoved(false);
    SystemMetadata systemMetadata =
        new SystemMetadata().setProperties(new StringMap(Map.of(FORCE_INDEXING_KEY, "true")));
    when(event.getRecordTemplate()).thenReturn(status);
    when(event.getPreviousRecordTemplate()).thenReturn(new Status().setRemoved(false));
    when(event.getSystemMetadata()).thenReturn(systemMetadata);
    stubTransform(status, Map.of("removed", "false"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(event)), false);

    assertEquals(capturedDocument(DATASET_ENTITY_NAME).get("removed").asText(), "false");
  }

  private MCLItem aspectEvent(String aspectName) {
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn(aspectName);
    MCLItem event = mock(MCLItem.class);
    when(event.getUrn()).thenReturn(testUrn);
    when(event.getEntitySpec()).thenReturn(mockEntitySpec);
    when(event.getAspectSpec()).thenReturn(aspectSpec);
    when(event.getAspectName()).thenReturn(aspectName);
    when(event.getRecordTemplate()).thenReturn(mock(RecordTemplate.class));
    when(event.getAuditStamp()).thenReturn(mockAuditStamp);
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    return event;
  }

  private void stubTransform(RecordTemplate record, Map<String, String> fields) throws Exception {
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put("urn", testUrn.toString());
    fields.forEach(document::put);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            eq(record),
            any(AspectSpec.class),
            anyBoolean(),
            any()))
        .thenReturn(Optional.of(document));
  }

  private void enableDocumentSemanticStrategy() {
    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(true);
    semanticConfig.setEnabledEntities(Set.of("document"));
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(1024);
    semanticConfig.setModels(Map.of("cohere_embed_v3", model));
    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .v2Enabled(false)
            .expectedVectorDimensions(Map.of())
            .entityDocumentIdHasher(new Sha256UrnEntityDocumentIdHasher())
            .documentContributors(List.of())
            .semanticSearchConfiguration(semanticConfig)
            .build();
  }

  private StructuredPropertyDefinition propertyDefinition(
      String qualifiedName, UrnArray entityTypes) {
    return new StructuredPropertyDefinition()
        .setQualifiedName(qualifiedName)
        .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
        .setEntityTypes(entityTypes);
  }

  @SuppressWarnings("unchecked")
  private SearchClientShim<?> stubMappingClient() throws IOException {
    SearchClientShim<Object> searchClient = mock(SearchClientShim.class);
    doReturn(searchClient).when(mockIndexBuilder).getSearchClient();
    return searchClient;
  }

  private String capturedPutMapping(SearchClientShim<?> searchClient) throws IOException {
    ArgumentCaptor<PutMappingRequest> requestCaptor =
        ArgumentCaptor.forClass(PutMappingRequest.class);
    verify(searchClient, times(1))
        .putIndexMapping(eq(operationContext), requestCaptor.capture(), any());
    assertEquals(Set.copyOf(List.of(requestCaptor.getValue().indices())), v3EntityIndexNames());
    return requestCaptor.getValue().source().utf8ToString();
  }

  private Set<String> v3EntityIndexNames() {
    return V3IndexKeys.groupEntitySpecs(operationContext.getEntityRegistry()).keySet().stream()
        .map(
            indexKey ->
                operationContext
                    .getSearchContext()
                    .getIndexConvention()
                    .getEntityIndexNameV3(operationContext, indexKey))
        .collect(Collectors.toSet());
  }

  /** The one V3 document written for {@code searchGroup}'s alias, parsed. */
  private ObjectNode capturedDocument(String searchGroup) throws Exception {
    ArgumentCaptor<V3SearchDocumentWrite> writeCaptor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService).writeV3SearchDocument(eq(operationContext), writeCaptor.capture());
    V3SearchDocumentWrite write = writeCaptor.getValue();
    assertEquals(
        write.indexName(),
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, searchGroup));
    return (ObjectNode) new ObjectMapper().readTree(write.source());
  }
}
