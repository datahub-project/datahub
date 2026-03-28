package com.linkedin.datahub.upgrade.cleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.upgrade.DataHubUpgradeState;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.ResponseException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteElasticsearchIndicesStepTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;

  @Mock
  @SuppressWarnings("rawtypes")
  private SearchClientShim searchClient;

  @Mock private ConfigurationProvider configurationProvider;
  @Mock private ElasticSearchConfiguration elasticSearchConfiguration;
  @Mock private IndexConfiguration indexConfiguration;
  @Mock private RawResponse rawResponse;
  @Mock private ResponseException responseException;

  private UpgradeContext mockContext;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    mockContext = mock(UpgradeContext.class);

    when(esComponents.getSearchClient()).thenReturn(searchClient);
    when(configurationProvider.getElasticSearch()).thenReturn(elasticSearchConfiguration);
    when(elasticSearchConfiguration.getIndex()).thenReturn(indexConfiguration);
    when(indexConfiguration.getFinalPrefix()).thenReturn("test_");
  }

  @Test
  public void testId() {
    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    assertEquals(step.id(), "DeleteElasticsearchIndicesStep");
  }

  @Test
  public void testRetryCount() {
    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    assertEquals(step.retryCount(), 2);
  }

  @Test
  public void testSucceedsWithElasticsearch() throws Exception {
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));
    when(searchClient.performLowLevelRequest(any())).thenReturn(rawResponse);

    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSucceedsWithOpenSearch() throws Exception {
    when(searchClient.getEngineType()).thenReturn(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));
    when(searchClient.performLowLevelRequest(any())).thenReturn(rawResponse);

    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSafeDeleteIgnores404ResponseException() throws Exception {
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    // First call (deleteAllIndices) succeeds, subsequent safeDelete calls get 404
    when(searchClient.performLowLevelRequest(any()))
        .thenReturn(rawResponse)
        .thenThrow(responseException);
    when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));
    when(responseException.getResponse()).thenReturn(mock(org.opensearch.client.Response.class));
    when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(404, "Not Found"));

    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSafeDeleteIgnoresNon404ResponseException() throws Exception {
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    // First call (deleteAllIndices) succeeds, subsequent safeDelete calls get 500
    when(searchClient.performLowLevelRequest(any()))
        .thenReturn(rawResponse)
        .thenThrow(responseException);
    when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));
    when(responseException.getResponse()).thenReturn(mock(org.opensearch.client.Response.class));
    when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Server Error"));

    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    UpgradeStepResult result = step.executable().apply(mockContext);

    // safeDelete catches non-404 ResponseException and logs warning — step still succeeds
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSucceedsWhenDeleteAllIndicesThrows() throws Exception {
    // deleteAllIndices catches all exceptions internally, so step still succeeds
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(searchClient.performLowLevelRequest(any()))
        .thenThrow(new RuntimeException("ES unavailable"));

    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSucceedsWhenDeleteReturnsErrorStatusCode() throws Exception {
    // performDelete throws RuntimeException on status >= 400, caught by deleteAllIndices/safeDelete
    when(searchClient.getEngineType())
        .thenReturn(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    when(rawResponse.getStatusLine()).thenReturn(createStatusLine(500, "Server Error"));
    when(searchClient.performLowLevelRequest(any())).thenReturn(rawResponse);

    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testFailsWhenConfigProviderThrows() {
    when(configurationProvider.getElasticSearch()).thenThrow(new RuntimeException("Config error"));

    DeleteElasticsearchIndicesStep step =
        new DeleteElasticsearchIndicesStep(esComponents, configurationProvider);
    UpgradeStepResult result = step.executable().apply(mockContext);

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
