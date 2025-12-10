package com.linkedin.datahub.upgrade.semanticsearch;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.semantic.EntityTextGenerator;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SearchEmbeddingsUpdateStepTest {

  @Mock private SearchClientShim<?> mockSearchClient;
  @Mock private EntityTextGenerator mockEntityTextGenerator;
  @Mock private EmbeddingProvider mockEmbeddingProvider;
  @Mock private ESBulkProcessor mockEsBulkProcessor;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private UpgradeReport mockUpgradeReport;

  private OperationContext operationContext;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);
    operationContext = TestOperationContexts.systemContextNoValidate();

    when(mockUpgradeContext.report()).thenReturn(mockUpgradeReport);
    doNothing().when(mockUpgradeReport).addLine(anyString());
  }

  @Test
  public void testExecute_MultipleEntitiesAndModels_Success() throws Exception {
    // Arrange: Create configuration with 2 entity types and 2 models
    ElasticSearchConfiguration esConfig = createTestConfig();
    SearchEmbeddingsUpdateStep step =
        new SearchEmbeddingsUpdateStep(
            operationContext,
            mockSearchClient,
            esConfig,
            mockEntityTextGenerator,
            mockEmbeddingProvider,
            mockEsBulkProcessor);

    // Mock UpgradeContext to return empty args (use config defaults)
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    when(mockUpgradeContext.parsedArgs()).thenReturn(parsedArgs);

    // Mock index existence checks - both indices exist
    when(mockSearchClient.indexExists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(true);

    // Mock search responses: 2 documents in first batch, empty in second batch
    setupMockSearchResponses();

    // Mock EntityTextGenerator to return different text per entity type
    when(mockEntityTextGenerator.generateText(eq("dataset"), any()))
        .thenReturn("Dataset entity text");
    when(mockEntityTextGenerator.generateText(eq("chart"), any())).thenReturn("Chart entity text");

    // Mock EmbeddingProvider to return sample embeddings
    when(mockEmbeddingProvider.embed(anyString(), any()))
        .thenReturn(new float[] {0.1f, 0.2f, 0.3f});

    // Mock ESBulkProcessor
    when(mockEsBulkProcessor.add(any(UpdateRequest.class))).thenReturn(mockEsBulkProcessor);
    doNothing().when(mockEsBulkProcessor).flush();

    // Act: Execute the upgrade step
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    // Assert: Verify result status
    assertEquals(result.stepId(), "SearchEmbeddingsUpdateStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify EmbeddingProvider called 8 times (2 entities × 2 models × 2 documents)
    ArgumentCaptor<String> textCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockEmbeddingProvider, times(8)).embed(textCaptor.capture(), any());

    // Verify correct text was used for embeddings
    long datasetCalls =
        textCaptor.getAllValues().stream().filter(t -> t.equals("Dataset entity text")).count();
    long chartCalls =
        textCaptor.getAllValues().stream().filter(t -> t.equals("Chart entity text")).count();
    assertEquals(datasetCalls, 4); // 2 models × 2 documents
    assertEquals(chartCalls, 4); // 2 models × 2 documents

    // Verify ESBulkProcessor.add() called 8 times
    ArgumentCaptor<UpdateRequest> updateRequestCaptor =
        ArgumentCaptor.forClass(UpdateRequest.class);
    verify(mockEsBulkProcessor, times(8)).add(updateRequestCaptor.capture());

    // Verify update requests have correct structure
    for (UpdateRequest updateRequest : updateRequestCaptor.getAllValues()) {
      assertNotNull(updateRequest.index());
      assertNotNull(updateRequest.id());
      assertEquals(updateRequest.retryOnConflict(), 3);
    }

    // Verify ESBulkProcessor.flush() called 8 times (once per batch per entity/model combo)
    verify(mockEsBulkProcessor, times(8)).flush();
  }

  private ElasticSearchConfiguration createTestConfig() {
    // Create model configurations
    ModelEmbeddingConfig cohereModel = new ModelEmbeddingConfig();
    cohereModel.setVectorDimension(1024);
    cohereModel.setKnnEngine("faiss");
    cohereModel.setSpaceType("cosinesimil");
    cohereModel.setEfConstruction(128);
    cohereModel.setM(16);

    ModelEmbeddingConfig openaiModel = new ModelEmbeddingConfig();
    openaiModel.setVectorDimension(1536);
    openaiModel.setKnnEngine("faiss");
    openaiModel.setSpaceType("cosinesimil");
    openaiModel.setEfConstruction(128);
    openaiModel.setM(16);

    Map<String, ModelEmbeddingConfig> models = new HashMap<>();
    models.put("cohere_embed_v3", cohereModel);
    models.put("openai_ada_002", openaiModel);

    // Create semantic search configuration
    SemanticSearchConfiguration semanticSearchConfig = new SemanticSearchConfiguration();
    semanticSearchConfig.setEnabled(true);
    semanticSearchConfig.setEnabledEntities(Set.of("dataset", "chart"));
    semanticSearchConfig.setModels(models);

    // Create entity index configuration
    EntityIndexConfiguration entityIndexConfig = new EntityIndexConfiguration();
    entityIndexConfig.setSemanticSearch(semanticSearchConfig);

    // Create and return ElasticSearch configuration
    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setEntityIndex(entityIndexConfig);

    return esConfig;
  }

  private void setupMockSearchResponses() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();

    // First batch: return 2 documents
    SearchResponse firstBatchResponse = mock(SearchResponse.class);
    SearchHits firstBatchHits = mock(SearchHits.class);
    SearchHit hit1 = mock(SearchHit.class);
    SearchHit hit2 = mock(SearchHit.class);

    ObjectNode doc1Source = objectMapper.createObjectNode();
    doc1Source.put("urn", "urn:li:dataset:1");
    doc1Source.put("name", "Dataset 1");

    ObjectNode doc2Source = objectMapper.createObjectNode();
    doc2Source.put("urn", "urn:li:dataset:2");
    doc2Source.put("name", "Dataset 2");

    when(hit1.getId()).thenReturn("doc1");
    when(hit1.getSourceAsString()).thenReturn(objectMapper.writeValueAsString(doc1Source));
    when(hit2.getId()).thenReturn("doc2");
    when(hit2.getSourceAsString()).thenReturn(objectMapper.writeValueAsString(doc2Source));

    when(firstBatchHits.getHits()).thenReturn(new SearchHit[] {hit1, hit2});
    when(firstBatchResponse.getHits()).thenReturn(firstBatchHits);

    // Second batch: return empty (no more documents)
    SearchResponse secondBatchResponse = mock(SearchResponse.class);
    SearchHits secondBatchHits = mock(SearchHits.class);
    when(secondBatchHits.getHits()).thenReturn(new SearchHit[0]);
    when(secondBatchResponse.getHits()).thenReturn(secondBatchHits);

    // Configure mock to return first batch, then second batch for each entity/model combo
    when(mockSearchClient.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(firstBatchResponse, secondBatchResponse) // dataset + cohere_embed_v3
        .thenReturn(firstBatchResponse, secondBatchResponse) // dataset + openai_ada_002
        .thenReturn(firstBatchResponse, secondBatchResponse) // chart + cohere_embed_v3
        .thenReturn(firstBatchResponse, secondBatchResponse); // chart + openai_ada_002
  }
}
