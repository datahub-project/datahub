package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.PlatformAnalyticsConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateUsageEventIndicesStepTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  @Mock private ConfigurationProvider configurationProvider;
  @Mock private SearchClientShim searchClient;
  @Mock private SearchClientShim.SearchEngineType searchEngineType;
  @Mock private ESIndexBuilder indexBuilder;
  @Mock private UpgradeContext upgradeContext;
  @Mock private PlatformAnalyticsConfiguration platformAnalytics;
  @Mock private ElasticSearchConfiguration elasticSearch;
  @Mock private IndexConfiguration index;
  @Mock private RawResponse rawResponse;
  @Mock private CreateIndexResponse createIndexResponse;

  private CreateUsageEventIndicesStep step;
  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @BeforeMethod
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);

    // Setup common mocks
    Mockito.when(esComponents.getSearchClient()).thenReturn(searchClient);
    Mockito.when(searchClient.getEngineType()).thenReturn(searchEngineType);
    Mockito.when(esComponents.getIndexBuilder()).thenReturn(indexBuilder);
    Mockito.when(indexBuilder.getNumShards()).thenReturn(2);
    Mockito.when(indexBuilder.getNumReplicas()).thenReturn(1);

    Mockito.when(configurationProvider.getPlatformAnalytics()).thenReturn(platformAnalytics);
    Mockito.when(configurationProvider.getElasticSearch()).thenReturn(elasticSearch);
    Mockito.when(elasticSearch.getIndex()).thenReturn(index);
    Mockito.when(index.getPrefix()).thenReturn("test_");

    Mockito.when(upgradeContext.opContext()).thenReturn(opContext);

    // Mock client responses for utility methods
    setupMockClientResponses();

    step = new CreateUsageEventIndicesStep(esComponents, configurationProvider);
  }

  private void setupMockClientResponses() throws IOException {
    // Mock RawResponse for low-level requests (ILM/ISM policies, index templates)
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(
            new org.apache.http.StatusLine() {
              @Override
              public int getStatusCode() {
                return 200;
              }

              @Override
              public String getReasonPhrase() {
                return "OK";
              }

              @Override
              public org.apache.http.ProtocolVersion getProtocolVersion() {
                return null;
              }
            });

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);

    // Mock index existence check
    Mockito.when(
            searchClient.indexExists(
                Mockito.any(GetIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(false);

    // Mock index creation
    Mockito.when(createIndexResponse.isAcknowledged()).thenReturn(true);
    Mockito.when(
            searchClient.createIndex(
                Mockito.any(CreateIndexRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(createIndexResponse);
  }

  @Test
  public void testId() {
    // Act
    String id = step.id();

    // Assert
    Assert.assertEquals(id, "CreateUsageEventIndicesStep");
  }

  @Test
  public void testRetryCount() {
    // Act
    int retryCount = step.retryCount();

    // Assert
    Assert.assertEquals(retryCount, 3);
  }

  @Test
  public void testExecutable_AnalyticsDisabled() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(false);

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that no Elasticsearch operations were called
    Mockito.verify(searchClient, Mockito.never()).getEngineType();
  }

  @Test
  public void testExecutable_ElasticsearchPath_Success() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify Elasticsearch path was taken
    Mockito.verify(searchEngineType).isOpenSearch();
    Mockito.verify(indexBuilder).getNumShards();
    Mockito.verify(indexBuilder).getNumReplicas();
  }

  @Test
  public void testExecutable_OpenSearchPath_Success() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify OpenSearch path was taken
    Mockito.verify(searchEngineType).isOpenSearch();
    Mockito.verify(indexBuilder).getNumShards();
    Mockito.verify(indexBuilder).getNumReplicas();
  }

  @Test
  public void testExecutable_ElasticsearchPath_Exception() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(indexBuilder.getNumShards())
        .thenThrow(new RuntimeException("Elasticsearch setup failed"));

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_OpenSearchPath_Exception() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);
    Mockito.when(indexBuilder.getNumShards())
        .thenThrow(new RuntimeException("OpenSearch setup failed"));

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_ConfigurationProviderException() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenThrow(new RuntimeException("Config error"));

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_EngineTypeException() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchClient.getEngineType()).thenThrow(new RuntimeException("Engine type error"));

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_WithEmptyPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getPrefix()).thenReturn(""); // Empty prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify empty prefix was used and no underscore separator was added
    Mockito.verify(index).getPrefix();

    // Verify that the low-level requests were made with correct names (no underscore prefix)
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request -> request.getEndpoint().equals("_ilm/policy/datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("_index_template/datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_WithNullPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getPrefix()).thenReturn(null); // Null prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify null prefix was handled and no underscore separator was added
    Mockito.verify(index).getPrefix();

    // Verify that the low-level requests were made with correct names (no underscore prefix)
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request -> request.getEndpoint().equals("_ilm/policy/datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("_index_template/datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_WithNonEmptyPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getPrefix()).thenReturn("prod"); // Non-empty prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify non-empty prefix was used and underscore separator was added
    Mockito.verify(index).getPrefix();

    // Verify that the low-level requests were made with correct names (with underscore prefix)
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request.getEndpoint().equals("_ilm/policy/prod_datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("_index_template/prod_datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_WithSpecificPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getPrefix())
        .thenReturn("kbcpyv7ss3-staging-test"); // Specific prefix from issue

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify specific prefix was used and underscore separator was added
    Mockito.verify(index).getPrefix();

    // Verify that the low-level requests were made with correct names (with underscore prefix)
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("_ilm/policy/kbcpyv7ss3-staging-test_datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals(
                            "_index_template/kbcpyv7ss3-staging-test_datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_OpenSearchPath_WithEmptyPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);
    Mockito.when(index.getPrefix()).thenReturn(""); // Empty prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify OpenSearch path was taken and empty prefix was used
    Mockito.verify(searchEngineType).isOpenSearch();
    Mockito.verify(index).getPrefix();

    // Verify that the low-level requests were made with correct names (no underscore prefix)
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("_plugins/_ism/policies/datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request.getEndpoint().equals("_template/datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_OpenSearchPath_WithNonEmptyPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);
    Mockito.when(index.getPrefix()).thenReturn("prod"); // Non-empty prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify OpenSearch path was taken and non-empty prefix was used
    Mockito.verify(searchEngineType).isOpenSearch();
    Mockito.verify(index).getPrefix();

    // Verify that the low-level requests were made with correct names (with underscore prefix)
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("_plugins/_ism/policies/prod_datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("_template/prod_datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_WithCustomShardsAndReplicas() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(indexBuilder.getNumShards()).thenReturn(5);
    Mockito.when(indexBuilder.getNumReplicas()).thenReturn(3);

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify custom shards and replicas were retrieved
    Mockito.verify(indexBuilder).getNumShards();
    Mockito.verify(indexBuilder).getNumReplicas();
  }

  @Test
  public void testConstructor() {
    // Act
    CreateUsageEventIndicesStep newStep =
        new CreateUsageEventIndicesStep(esComponents, configurationProvider);

    // Assert
    Assert.assertNotNull(newStep);
    Assert.assertEquals(newStep.id(), "CreateUsageEventIndicesStep");
  }

  @Test
  public void testExecutable_MultipleCalls() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result1 = executable.apply(upgradeContext);
    UpgradeStepResult result2 = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result1);
    Assert.assertNotNull(result2);
    Assert.assertEquals(result1.result(), DataHubUpgradeState.SUCCEEDED);
    Assert.assertEquals(result2.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that the executable function can be called multiple times
    Mockito.verify(searchEngineType, Mockito.times(2)).isOpenSearch();
  }
}
