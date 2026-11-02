package com.linkedin.datahub.upgrade.cleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.ComponentClusterConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.indices.GetIndexRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteElasticsearchIndicesStepTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;

  @Mock
  @SuppressWarnings("rawtypes")
  private SearchClientShim searchClient;

  @Mock private IndexConvention indexConvention;
  @Mock private ElasticSearchConfiguration esConfig;
  @Mock private IndexConfiguration indexConfig;
  @Mock private GetIndexResponse getIndexResponse;
  @Mock private GetAliasesResponse getAliasesResponse;
  @Mock private RawResponse rawResponse;
  @Mock private ResponseException responseException;

  private UpgradeContext mockContext;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    mockContext = mock(UpgradeContext.class);

    when(esComponents.getSearchClient()).thenReturn(searchClient);
    when(esComponents.getIndexConvention()).thenReturn(indexConvention);
    when(esComponents.getConfig()).thenReturn(esConfig);
    when(esConfig.getIndex()).thenReturn(indexConfig);
    when(indexConfig.getFinalPrefix()).thenReturn("test_");

    OperationContext opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoValidate(), searchClient);
    when(mockContext.opContext()).thenReturn(opContext);

    // IndexConvention returns DataHub-specific patterns and names
    when(indexConvention.getAllEntityIndicesPatterns(any(OperationFingerprint.class)))
        .thenReturn(List.of("test_*index_v2"));
    when(indexConvention.getAllTimeseriesAspectIndicesPattern(any(OperationFingerprint.class)))
        .thenReturn("test_*aspect_v1");
    when(indexConvention.getAllSemanticEntityIndicesPattern(any(OperationFingerprint.class)))
        .thenReturn("test_*index_v2_semantic");
    when(indexConvention.getIndexName(any(OperationFingerprint.class), anyString()))
        .thenAnswer(inv -> "test_" + inv.getArgument(1));
    when(indexConvention.getIndexName(
            any(OperationFingerprint.class), any(SearchComponent.class), anyString()))
        .thenAnswer(inv -> "test_" + inv.getArgument(2));

    // Default: patterns match no indices — simulate the 404 ResponseException the real client
    // throws
    org.opensearch.client.Response notFoundResponse = mock(org.opensearch.client.Response.class);
    when(notFoundResponse.getStatusLine()).thenReturn(createStatusLine(404, "Not Found"));
    when(responseException.getResponse()).thenReturn(notFoundResponse);
    doThrow(responseException).when(searchClient).getIndex(any(), any(), any());

    // Default: low-level requests succeed
    when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));
    when(searchClient.performLowLevelRequest(any(), any())).thenReturn(rawResponse);
  }

  @Test
  public void testId() {
    assertEquals(
        new DeleteElasticsearchIndicesStep(esComponents).id(), "DeleteElasticsearchIndicesStep");
  }

  @Test
  public void testRetryCount() {
    assertEquals(new DeleteElasticsearchIndicesStep(esComponents).retryCount(), 2);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSucceedsWithElasticsearchNoIndices() throws Exception {
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSucceedsWithOpenSearchNoIndices() throws Exception {
    when(searchClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSucceedsWhenPatternEnumerationThrowsNonNotFound() throws Exception {
    // Non-404 IOException from getIndex: step logs warning but still succeeds
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    doThrow(new IOException("connection refused")).when(searchClient).getIndex(any(), any(), any());

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSucceedsWhenConcreteIndicesFoundAndDeleted() throws Exception {
    // getIndex returns a concrete index; IndexDeletionUtils resolves and deletes it
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(getIndexResponse.getIndices()).thenReturn(new String[] {"test_datasetindex_v2"});
    doReturn(getIndexResponse).when(searchClient).getIndex(any(), any(), any());

    // IndexDeletionUtils: not an alias, index exists, delete succeeds
    when(getAliasesResponse.getAliases()).thenReturn(Collections.emptyMap());
    doReturn(getAliasesResponse).when(searchClient).getIndexAliases(any(), any(), any());
    doReturn(true).when(searchClient).indexExists(any(), any(), any());
    doReturn(new AcknowledgedResponse(true)).when(searchClient).deleteIndex(any(), any(), any());

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(searchClient, atLeastOnce()).deleteIndex(any(), any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSucceedsWhenIndexDeletionFails() throws Exception {
    // getIndex returns a concrete index but deleteIndex throws — safeDeleteIndex swallows it
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(getIndexResponse.getIndices()).thenReturn(new String[] {"test_datasetindex_v2"});
    doReturn(getIndexResponse).when(searchClient).getIndex(any(), any(), any());

    when(getAliasesResponse.getAliases()).thenReturn(Collections.emptyMap());
    doReturn(getAliasesResponse).when(searchClient).getIndexAliases(any(), any(), any());
    doReturn(true).when(searchClient).indexExists(any(), any(), any());
    doThrow(new IOException("cluster_block_exception"))
        .when(searchClient)
        .deleteIndex(any(), any(), any());

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSucceedsWhenLowLevelDeleteReturns404() throws Exception {
    // safeDeleteLowLevel: ResponseException with 404 is treated as already-absent, not an error
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(responseException.getResponse()).thenReturn(mock(org.opensearch.client.Response.class));
    when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(404, "Not Found"));
    when(searchClient.performLowLevelRequest(any(), any())).thenThrow(responseException);

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSucceedsWhenLowLevelDeleteReturns500() throws Exception {
    // safeDeleteLowLevel: non-404 ResponseException is logged as a warning, step still succeeds
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(responseException.getResponse()).thenReturn(mock(org.opensearch.client.Response.class));
    when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));
    when(searchClient.performLowLevelRequest(any(), any())).thenThrow(responseException);

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFailsWhenGetEngineTypeThrows() {
    // Exception before any deletion starts — propagates to top-level catch → FAILED
    when(searchClient.getEngineType()).thenThrow(new RuntimeException("client unavailable"));

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeletesOnEachUniqueClusterClient() throws Exception {
    SearchClientShim<?> secondary = mock(SearchClientShim.class);
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(secondary.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    doThrow(responseException).when(secondary).getIndex(any(), any(), any());
    when(secondary.performLowLevelRequest(any(), any())).thenReturn(rawResponse);

    SearchClusterAccess access =
        component -> component == SearchComponent.USAGE ? secondary : searchClient;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoValidate(), access);
    when(mockContext.opContext()).thenReturn(opContext);

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(searchClient, atLeastOnce()).performLowLevelRequest(any(), any());
    verify(secondary, atLeastOnce()).performLowLevelRequest(any(), any());
    verify(secondary, never()).getIndex(any(), any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeletesPerClusterRolePrefix() throws Exception {
    SearchClientShim<?> secondary = mock(SearchClientShim.class);
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(secondary.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    doThrow(responseException).when(secondary).getIndex(any(), any(), any());
    when(secondary.performLowLevelRequest(any(), any())).thenReturn(rawResponse);

    IndexConfiguration overlayIndex = mock(IndexConfiguration.class);
    when(overlayIndex.getFinalPrefix()).thenReturn("overlay_");

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(
                esComponents,
                registryWithClients(searchClient, secondary, indexConfig, overlayIndex))
            .executable()
            .apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<Request> overlayRequests = ArgumentCaptor.forClass(Request.class);
    verify(secondary, atLeastOnce()).performLowLevelRequest(any(), overlayRequests.capture());
    assertTrue(
        overlayRequests.getAllValues().stream()
            .anyMatch(request -> request.getEndpoint().contains("overlay_access")));
    assertTrue(
        overlayRequests.getAllValues().stream()
            .noneMatch(request -> request.getEndpoint().contains("test_access")));

    ArgumentCaptor<Request> primaryRequests = ArgumentCaptor.forClass(Request.class);
    verify(searchClient, atLeastOnce()).performLowLevelRequest(any(), primaryRequests.capture());
    assertTrue(
        primaryRequests.getAllValues().stream()
            .anyMatch(request -> request.getEndpoint().contains("test_access")));
    assertTrue(
        primaryRequests.getAllValues().stream()
            .noneMatch(request -> request.getEndpoint().contains("overlay_access")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeletesSemanticIndexOnSemanticClient() throws Exception {
    SearchClientShim<?> semantic = mock(SearchClientShim.class);
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(semantic.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    doThrow(responseException).when(semantic).getIndex(any(), any(), any());
    when(semantic.performLowLevelRequest(any(), any())).thenReturn(rawResponse);

    SearchClusterAccess access =
        component -> component == SearchComponent.SEMANTIC ? semantic : searchClient;
    OperationContext opContext =
        TestOperationContexts.withSearchClusterAccess(
            TestOperationContexts.systemContextNoValidate(), access);
    when(mockContext.opContext()).thenReturn(opContext);

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<GetIndexRequest> semanticPatterns =
        ArgumentCaptor.forClass(GetIndexRequest.class);
    verify(semantic, atLeastOnce()).getIndex(any(), semanticPatterns.capture(), any());
    assertTrue(
        semanticPatterns.getAllValues().stream()
            .anyMatch(
                request ->
                    java.util.Arrays.asList(request.indices())
                        .contains("test_*index_v2_semantic")));
    verify(searchClient, never())
        .getIndex(
            any(),
            argThat(
                request ->
                    java.util.Arrays.asList(request.indices()).contains("test_*index_v2_semantic")),
            any());
  }

  private static SearchClusterRegistry registryWithClients(
      SearchClientShim<?> primaryClient,
      SearchClientShim<?> secondaryClient,
      IndexConfiguration primaryIndex,
      IndexConfiguration secondaryIndex) {
    ElasticSearchConfiguration routing = mock(ElasticSearchConfiguration.class);
    when(routing.getComponentCluster()).thenReturn(ComponentClusterConfiguration.builder().build());
    Map<String, SearchClusterRegistry.ClusterConnection> connections = new LinkedHashMap<>();
    connections.put(
        "primary",
        new SearchClusterRegistry.ClusterConnection(
            "primary",
            elasticSearchConfig(primaryIndex),
            primaryClient,
            mock(ESBulkProcessor.class),
            mock(ESIndexBuilder.class)));
    connections.put(
        "secondary",
        new SearchClusterRegistry.ClusterConnection(
            "secondary",
            elasticSearchConfig(secondaryIndex),
            secondaryClient,
            mock(ESBulkProcessor.class),
            mock(ESIndexBuilder.class)));
    return new SearchClusterRegistry(routing, connections);
  }

  private static ElasticSearchConfiguration elasticSearchConfig(IndexConfiguration indexConfig) {
    ElasticSearchConfiguration config = mock(ElasticSearchConfiguration.class);
    when(config.getIndex()).thenReturn(indexConfig);
    return config;
  }

  private StatusLine createStatusLine(int statusCode, String reasonPhrase) {
    return new StatusLine() {
      @Override
      public int getStatusCode() {
        return statusCode;
      }

      @Override
      public String getReasonPhrase() {
        return reasonPhrase;
      }

      @Override
      public ProtocolVersion getProtocolVersion() {
        return new ProtocolVersion("HTTP", 1, 1);
      }
    };
  }
}
