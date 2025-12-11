/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.Mockito.atLeastOnce;

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
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
  @Mock private GetAliasesResponse getAliasesResponse;

  private CreateUsageEventIndicesStep step;
  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @BeforeMethod
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);

    // Setup common mocks
    Mockito.when(esComponents.getSearchClient()).thenReturn(searchClient);
    Mockito.when(searchClient.getEngineType()).thenReturn(searchEngineType);
    Mockito.when(esComponents.getIndexBuilder()).thenReturn(indexBuilder);

    Mockito.when(configurationProvider.getPlatformAnalytics()).thenReturn(platformAnalytics);
    Mockito.when(configurationProvider.getElasticSearch()).thenReturn(elasticSearch);
    Mockito.when(elasticSearch.getIndex()).thenReturn(index);
    Mockito.when(index.getNumShards()).thenReturn(2);
    Mockito.when(index.getNumReplicas()).thenReturn(1);
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");

    Mockito.when(upgradeContext.opContext()).thenReturn(opContext);

    // Mock client responses for utility methods
    setupMockClientResponses();

    step = new CreateUsageEventIndicesStep(esComponents, configurationProvider);
  }

  @BeforeClass
  public void setup() {
    System.setProperty("ENABLE_SYSTEM_UPDATE_DUE", "true");
  }

  @AfterClass
  public void cleanup() {
    System.clearProperty("ENABLE_SYSTEM_UPDATE_DUE");
  }

  private void setupMockClientResponses() throws IOException {
    // Mock ShimConfiguration for AWS detection
    SearchClientShim.ShimConfiguration shimConfig =
        Mockito.mock(SearchClientShim.ShimConfiguration.class);
    Mockito.when(searchClient.getShimConfiguration()).thenReturn(shimConfig);
    Mockito.when(shimConfig.getHost())
        .thenReturn("localhost"); // Non-AWS host for self-hosted OpenSearch

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

    // Mock alias check - return empty map by default (no alias exists)
    Mockito.when(
            searchClient.getIndexAliases(
                Mockito.any(GetAliasesRequest.class), Mockito.any(RequestOptions.class)))
        .thenReturn(getAliasesResponse);
    Mockito.when(getAliasesResponse.getAliases()).thenReturn(java.util.Collections.emptyMap());

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
  public void testSkip_AnalyticsDisabled() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(false);

    // Act
    boolean shouldSkip = step.skip(upgradeContext);

    // Assert
    Assert.assertTrue(shouldSkip);
    Mockito.verify(platformAnalytics).isEnabled();
  }

  @Test
  public void testSkip_AnalyticsEnabled() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);

    // Act
    boolean shouldSkip = step.skip(upgradeContext);

    // Assert
    Assert.assertFalse(shouldSkip);
    Mockito.verify(platformAnalytics).isEnabled();
  }

  @Test
  public void testSkip_ConfigurationProviderException() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenThrow(new RuntimeException("Config error"));

    // Act & Assert
    Assert.assertThrows(RuntimeException.class, () -> step.skip(upgradeContext));
    Mockito.verify(platformAnalytics).isEnabled();
  }

  @Test
  public void testSkip_EnvVarNotSet_DefaultsToEnabled() throws Exception {
    // This test verifies that when SKIP_CREATE_USAGE_EVENT_INDICES_STEP is not set (default),
    // the step respects the analytics enabled flag.
    // Arrange
    System.clearProperty("SKIP_CREATE_USAGE_EVENT_INDICES_STEP");
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);

    // Act
    boolean shouldSkip = step.skip(upgradeContext);

    // Assert - Should not skip when analytics is enabled and env var is not set
    Assert.assertFalse(shouldSkip);
    Mockito.verify(platformAnalytics).isEnabled();
  }

  @Test
  public void testSkip_EnvVarSetToTrue() throws Exception {
    // Arrange
    System.setProperty("SKIP_CREATE_USAGE_EVENT_INDICES_STEP", "true");
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);

    try {
      // Act
      boolean shouldSkip = step.skip(upgradeContext);

      // Assert - Should skip when env var is set to true, even if analytics is enabled
      Assert.assertTrue(shouldSkip);
      // Should not check analytics when env var skips
      Mockito.verify(platformAnalytics, Mockito.never()).isEnabled();
    } finally {
      System.clearProperty("SKIP_CREATE_USAGE_EVENT_INDICES_STEP");
    }
  }

  @Test
  public void testSkip_EnvVarSetToFalse() throws Exception {
    // Arrange
    System.setProperty("SKIP_CREATE_USAGE_EVENT_INDICES_STEP", "false");
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);

    try {
      // Act
      boolean shouldSkip = step.skip(upgradeContext);

      // Assert - Should not skip when env var is set to false, check analytics instead
      Assert.assertFalse(shouldSkip);
      Mockito.verify(platformAnalytics).isEnabled();
    } finally {
      System.clearProperty("SKIP_CREATE_USAGE_EVENT_INDICES_STEP");
    }
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
    Mockito.verify(index).getNumShards();
    Mockito.verify(index).getNumReplicas();
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
    Mockito.verify(index).getNumShards();
    Mockito.verify(index).getNumReplicas();
  }

  @Test
  public void testExecutable_ElasticsearchPath_Exception() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getNumShards())
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
    Mockito.when(index.getNumShards()).thenThrow(new RuntimeException("OpenSearch setup failed"));

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
    // Throw exception in a method that executable() actually calls
    Mockito.when(esComponents.getSearchClient().getEngineType())
        .thenThrow(new RuntimeException("Engine type error"));

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
    Mockito.when(index.getFinalPrefix()).thenReturn(""); // Empty prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify empty prefix was used and no underscore separator was added
    Mockito.verify(index, atLeastOnce()).getFinalPrefix();

    // Verify that the low-level requests were made with correct names (no underscore prefix)
    Mockito.verify(searchClient, Mockito.atLeast(2)).performLowLevelRequest(Mockito.any());

    // Verify specific endpoint calls were made
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request.getEndpoint().equals("/_ilm/policy/datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("/_index_template/datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_WithNonEmptyPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getFinalPrefix()).thenReturn("prod_"); // Non-empty prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify non-empty prefix was used and underscore separator was added
    Mockito.verify(index, atLeastOnce()).getFinalPrefix();

    // Verify that the low-level requests were made with correct names (with underscore prefix)
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request.getEndpoint().equals("/_ilm/policy/prod_datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("/_index_template/prod_datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_WithSpecificPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getFinalPrefix())
        .thenReturn("kbcpyv7ss3-staging-test_"); // Specific prefix from issue

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify specific prefix was used and underscore separator was added
    Mockito.verify(index, atLeastOnce()).getFinalPrefix();

    // Verify that the low-level requests were made with correct names (with underscore prefix)
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals(
                            "/_ilm/policy/kbcpyv7ss3-staging-test_datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals(
                            "/_index_template/kbcpyv7ss3-staging-test_datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_OpenSearchPath_WithEmptyPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);
    Mockito.when(index.getFinalPrefix()).thenReturn(""); // Empty prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify OpenSearch path was taken and empty prefix was used
    Mockito.verify(searchEngineType).isOpenSearch();
    Mockito.verify(index, atLeastOnce()).getFinalPrefix();

    // Verify that the low-level requests were made with correct names (no underscore prefix)
    // Note: createIsmPolicy makes 2 calls - one for creation and one for update attempt
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("/_plugins/_ism/policies/datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("/_index_template/datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_OpenSearchPath_WithNonEmptyPrefix() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);
    Mockito.when(index.getFinalPrefix()).thenReturn("prod_"); // Non-empty prefix

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify OpenSearch path was taken and non-empty prefix was used
    Mockito.verify(searchEngineType).isOpenSearch();
    Mockito.verify(index, atLeastOnce()).getFinalPrefix();

    // Verify that the low-level requests were made with correct names (with underscore prefix)
    // Note: createIsmPolicy makes 2 calls - one for creation and one for update attempt
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("/_plugins/_ism/policies/prod_datahub_usage_event_policy")));
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request
                        .getEndpoint()
                        .equals("/_index_template/prod_datahub_usage_event_index_template")));
  }

  @Test
  public void testExecutable_WithCustomShardsAndReplicas() throws Exception {
    // Arrange
    Mockito.when(platformAnalytics.isEnabled()).thenReturn(true);
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getNumShards()).thenReturn(5);
    Mockito.when(index.getNumReplicas()).thenReturn(3);

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateUsageEventIndicesStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify custom shards and replicas were retrieved
    Mockito.verify(index).getNumShards();
    Mockito.verify(index).getNumReplicas();
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
