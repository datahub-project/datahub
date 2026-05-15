package com.linkedin.datahub.upgrade.cleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.ResponseException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteElasticsearchIndicesStepTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;

  @Mock
  @SuppressWarnings("rawtypes")
  private SearchClientShim searchClient;

  @Mock private IndexConvention indexConvention;
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

    // IndexConvention returns DataHub-specific patterns and names
    when(indexConvention.getAllEntityIndicesPatterns()).thenReturn(List.of("test_*index_v2"));
    when(indexConvention.getAllTimeseriesAspectIndicesPattern()).thenReturn("test_*aspect_v1");
    when(indexConvention.getIndexName(anyString())).thenAnswer(inv -> "test_" + inv.getArgument(0));

    // Default: patterns match no indices — simulate the 404 ResponseException the real client
    // throws
    org.opensearch.client.Response notFoundResponse = mock(org.opensearch.client.Response.class);
    when(notFoundResponse.getStatusLine()).thenReturn(createStatusLine(404, "Not Found"));
    when(responseException.getResponse()).thenReturn(notFoundResponse);
    doThrow(responseException).when(searchClient).getIndex(any(), any());

    // Default: low-level requests succeed
    when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));
    when(searchClient.performLowLevelRequest(any())).thenReturn(rawResponse);
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
    doThrow(new IOException("connection refused")).when(searchClient).getIndex(any(), any());

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
    doReturn(getIndexResponse).when(searchClient).getIndex(any(), any());

    // IndexDeletionUtils: not an alias, index exists, delete succeeds
    when(getAliasesResponse.getAliases()).thenReturn(Collections.emptyMap());
    doReturn(getAliasesResponse).when(searchClient).getIndexAliases(any(), any());
    doReturn(true).when(searchClient).indexExists(any(), any());
    doReturn(new AcknowledgedResponse(true)).when(searchClient).deleteIndex(any(), any());

    UpgradeStepResult result =
        new DeleteElasticsearchIndicesStep(esComponents).executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(searchClient, atLeastOnce()).deleteIndex(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSucceedsWhenIndexDeletionFails() throws Exception {
    // getIndex returns a concrete index but deleteIndex throws — safeDeleteIndex swallows it
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(getIndexResponse.getIndices()).thenReturn(new String[] {"test_datasetindex_v2"});
    doReturn(getIndexResponse).when(searchClient).getIndex(any(), any());

    when(getAliasesResponse.getAliases()).thenReturn(Collections.emptyMap());
    doReturn(getAliasesResponse).when(searchClient).getIndexAliases(any(), any());
    doReturn(true).when(searchClient).indexExists(any(), any());
    doThrow(new IOException("cluster_block_exception"))
        .when(searchClient)
        .deleteIndex(any(), any());

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
    when(searchClient.performLowLevelRequest(any())).thenThrow(responseException);

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
    when(searchClient.performLowLevelRequest(any())).thenThrow(responseException);

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
