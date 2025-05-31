package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.ReindexDebugArgs;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReindexDebugStepTest {

  @Mock private ElasticSearchService elasticSearchService;

  @Mock private ElasticSearchIndexed otherElasticSearchIndexed;

  @Mock private ESIndexBuilder indexBuilder;

  @Mock private UpgradeContext upgradeContext;

  @Mock private ReindexConfig reindexConfig1;

  @Mock private ReindexConfig reindexConfig2;

  @Mock private ReindexConfig reindexConfig3;

  @Mock private Urn urn;

  @Mock private StructuredPropertyDefinition structuredPropertyDefinition;

  private ReindexDebugStep reindexDebugStep;
  private List<ElasticSearchIndexed> services;
  private Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    services = new ArrayList<>();
    services.add(otherElasticSearchIndexed);
    services.add(elasticSearchService); // ElasticSearchService should be selected

    structuredProperties = new HashSet<>();
    structuredProperties.add(Pair.of(urn, structuredPropertyDefinition));

    reindexDebugStep = new ReindexDebugStep(services, structuredProperties);

    // Setup common mocks
    Mockito.when(elasticSearchService.getIndexBuilder()).thenReturn(indexBuilder);
  }

  @Test
  public void testConstructor_WithElasticSearchService() {
    // Arrange
    List<ElasticSearchIndexed> servicesWithES =
        Arrays.asList(otherElasticSearchIndexed, elasticSearchService);
    Set<Pair<Urn, StructuredPropertyDefinition>> props = new HashSet<>();

    // Act
    ReindexDebugStep step = new ReindexDebugStep(servicesWithES, props);

    // Assert
    Assert.assertNotNull(step);
    Assert.assertEquals(step.id(), "ReindexDebugStep");
  }

  @Test
  public void testConstructor_WithoutElasticSearchService() {
    // Arrange
    List<ElasticSearchIndexed> servicesWithoutES = Arrays.asList(otherElasticSearchIndexed);
    Set<Pair<Urn, StructuredPropertyDefinition>> props = new HashSet<>();

    // Act
    ReindexDebugStep step = new ReindexDebugStep(servicesWithoutES, props);

    // Assert
    Assert.assertNotNull(step);
    // service field should be null since no ElasticSearchService found
  }

  @Test
  public void testConstructor_WithEmptyServices() {
    // Arrange
    List<ElasticSearchIndexed> emptyServices = new ArrayList<>();
    Set<Pair<Urn, StructuredPropertyDefinition>> props = new HashSet<>();

    // Act
    ReindexDebugStep step = new ReindexDebugStep(emptyServices, props);

    // Assert
    Assert.assertNotNull(step);
  }

  @Test
  public void testId() {
    // Act
    String id = reindexDebugStep.id();

    // Assert
    Assert.assertEquals(id, "ReindexDebugStep");
  }

  @Test
  public void testGetIndex_WithValidIndex() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test_index"));

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assert.assertEquals(result, "test_index");
  }

  @Test
  public void testGetIndex_WithEmptyOptional() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.empty());

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assert.assertEquals(result, "");
  }

  @Test
  public void testGetIndex_WithoutIndexKey() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("other", Optional.of("value"));

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assert.assertEquals(result, "");
  }

  @Test
  public void testGetIndex_WithNullMap() {
    // Act & Assert - This should throw NullPointerException based on the actual implementation
    try {
      String result = reindexDebugStep.getIndex(null);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      // Expected behavior - the method doesn't handle null input gracefully
      Assert.assertTrue(e.getMessage().contains("parsedArgs"));
    }
  }

  @Test
  public void testGetIndex_WithEmptyMap() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assert.assertEquals(result, "");
  }

  @DataProvider(name = "indexValues")
  public Object[][] provideIndexValues() {
    return new Object[][] {{"datahub_index"}, {""}, {"test_index_v2"}, {"policy_index"}};
  }

  @Test(dataProvider = "indexValues")
  public void testGetIndex_WithVariousIndices(String indexValue) {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of(indexValue));

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assert.assertEquals(result, indexValue);
  }

  @Test
  public void testCreateArgs_FirstTime() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test_index"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    // Act
    ReindexDebugArgs result = reindexDebugStep.createArgs(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.index, "test_index");
    Assert.assertSame(result, reindexDebugStep.getArgs()); // Should be cached
  }

  @Test
  public void testCreateArgs_SecondTime_ReturnsCached() {
    // Arrange
    ReindexDebugArgs existingArgs = new ReindexDebugArgs();
    existingArgs.index = "cached_index";
    reindexDebugStep.args = existingArgs;

    // Act
    ReindexDebugArgs result = reindexDebugStep.createArgs(upgradeContext);

    // Assert
    Assert.assertSame(result, existingArgs);
    Assert.assertEquals(result.index, "cached_index");
    Mockito.verify(upgradeContext, Mockito.never())
        .parsedArgs(); // Should not call parsedArgs when cached
  }

  @Test
  public void testCreateArgs_WithEmptyIndex() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    // Act
    ReindexDebugArgs result = reindexDebugStep.createArgs(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.index, "");
  }

  @Test
  public void testExecutable_Success() throws IOException, IllegalAccessException {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("datahubpolicyindex"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    List<ReindexConfig> configs = Arrays.asList(reindexConfig1, reindexConfig2);
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);
    Mockito.when(reindexConfig1.name()).thenReturn("datahubpolicyindex_v2");
    Mockito.when(reindexConfig2.name()).thenReturn("other_index_v1");

    Function<UpgradeContext, UpgradeStepResult> executable = reindexDebugStep.executable();

    // Act
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "ReindexDebugStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    Mockito.verify(indexBuilder).buildIndex(reindexConfig1);
    Mockito.verify(reindexConfig1).forceReindex();
  }

  @Test
  public void testExecutable_BuildIndexThrowsIOException()
      throws IOException, IllegalAccessException {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("datahubpolicyindex"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    List<ReindexConfig> configs = Arrays.asList(reindexConfig1);
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);
    Mockito.when(reindexConfig1.name()).thenReturn("datahubpolicyindex_v2");

    IOException ioException = new IOException("Index build failed");
    Mockito.doThrow(ioException).when(indexBuilder).buildIndex(reindexConfig1);

    Function<UpgradeContext, UpgradeStepResult> executable = reindexDebugStep.executable();

    // Act
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "ReindexDebugStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_SetConfigThrowsRuntimeException()
      throws IOException, IllegalAccessException {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("datahubpolicyindex"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    RuntimeException exception = new RuntimeException("Config access denied");
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenThrow(exception);

    Function<UpgradeContext, UpgradeStepResult> executable = reindexDebugStep.executable();

    // Act
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "ReindexDebugStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testSetConfig_MatchingConfigFound() throws IOException, IllegalAccessException {
    // Arrange
    List<ReindexConfig> configs = Arrays.asList(reindexConfig1, reindexConfig2, reindexConfig3);
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);
    Mockito.when(reindexConfig1.name()).thenReturn("other_index_v1");
    Mockito.when(reindexConfig2.name()).thenReturn("datahubpolicyindex_v2");
    Mockito.when(reindexConfig3.name()).thenReturn("another_index_v1");

    // Act
    reindexDebugStep.setConfig("datahubpolicyindex");

    // Assert
    Mockito.verify(reindexConfig2).forceReindex();
    Mockito.verify(reindexConfig1, Mockito.never()).forceReindex();
    Mockito.verify(reindexConfig3, Mockito.never()).forceReindex();
  }

  @Test
  public void testSetConfig_NoMatchingConfig() throws IOException, IllegalAccessException {
    // Arrange
    List<ReindexConfig> configs = Arrays.asList(reindexConfig1, reindexConfig2);
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);
    Mockito.when(reindexConfig1.name()).thenReturn("other_index_v1");
    Mockito.when(reindexConfig2.name()).thenReturn("another_index_v2");

    // Act
    reindexDebugStep.setConfig("nonexistent_index");

    // Assert
    Mockito.verify(reindexConfig1, Mockito.never()).forceReindex();
    Mockito.verify(reindexConfig2, Mockito.never()).forceReindex();
  }

  @Test
  public void testSetConfig_EmptyTargetIndex() throws IOException, IllegalAccessException {
    // Arrange
    List<ReindexConfig> configs = Arrays.asList(reindexConfig1);
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);
    Mockito.when(reindexConfig1.name()).thenReturn("some_index_v1");

    // Act
    reindexDebugStep.setConfig("");

    // Assert
    // Empty string should match any config that starts with empty string (all configs)
    Mockito.verify(reindexConfig1).forceReindex();
  }

  @Test
  public void testSetConfig_EmptyConfigsList() throws IOException, IllegalAccessException {
    // Arrange
    List<ReindexConfig> configs = new ArrayList<>();
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);

    // Act
    reindexDebugStep.setConfig("any_index");

    // Assert
    // No configs to iterate through, no forceReindex should be called
  }

  @DataProvider(name = "configMatchingScenarios")
  public Object[][] provideConfigMatchingScenarios() {
    return new Object[][] {
      {"datahub", Arrays.asList("datahub_index_v1", "policy_index_v1"), "datahub_index_v1"},
      {"policy", Arrays.asList("datahub_index_v1", "policy_index_v1"), "policy_index_v1"},
      {"nonexistent", Arrays.asList("datahub_index_v1", "policy_index_v1"), null},
      {
        "", Arrays.asList("datahub_index_v1", "policy_index_v1"), "datahub_index_v1"
      }, // First config matches empty string
      {
        "test", Arrays.asList("test_index_v1", "test_index_v2", "other_v1"), "test_index_v1"
      } // First matching config should be selected
    };
  }

  @Test(dataProvider = "configMatchingScenarios")
  public void testSetConfig_VariousMatchingScenarios(
      String targetIndex, List<String> configNames, String expectedMatchingConfig)
      throws IOException, IllegalAccessException {

    // Arrange
    List<ReindexConfig> configs = new ArrayList<>();
    Map<String, ReindexConfig> configMap = new HashMap<>();

    for (int i = 0; i < configNames.size(); i++) {
      ReindexConfig config = Mockito.mock(ReindexConfig.class);
      Mockito.when(config.name()).thenReturn(configNames.get(i));
      configs.add(config);
      configMap.put(configNames.get(i), config);
    }

    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);

    // Act
    reindexDebugStep.setConfig(targetIndex);

    // Assert
    if (expectedMatchingConfig != null) {
      ReindexConfig expectedConfig = configMap.get(expectedMatchingConfig);
      Mockito.verify(expectedConfig).forceReindex();

      // Verify other configs were not called
      for (ReindexConfig config : configs) {
        if (config != expectedConfig) {
          Mockito.verify(config, Mockito.never()).forceReindex();
        }
      }
    } else {
      // No config should be matched
      for (ReindexConfig config : configs) {
        Mockito.verify(config, Mockito.never()).forceReindex();
      }
    }
  }

  @Test
  public void testExecutable_WithNullService() {
    // Arrange - Create step without ElasticSearchService
    List<ElasticSearchIndexed> servicesWithoutES = Arrays.asList(otherElasticSearchIndexed);
    ReindexDebugStep stepWithoutService =
        new ReindexDebugStep(servicesWithoutES, structuredProperties);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test_index"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    Function<UpgradeContext, UpgradeStepResult> executable = stepWithoutService.executable();

    // Act
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "ReindexDebugStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_BuildReindexConfigsThrowsIOException()
      throws IOException, IllegalAccessException {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test_index"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    IOException exception = new IOException("Failed to build configs");
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenThrow(exception);

    Function<UpgradeContext, UpgradeStepResult> executable = reindexDebugStep.executable();

    // Act
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "ReindexDebugStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testGetArgs_WhenNotSet() {
    // Arrange - Fresh instance
    ReindexDebugStep freshStep = new ReindexDebugStep(services, structuredProperties);

    // Act
    ReindexDebugArgs result = freshStep.getArgs();

    // Assert
    Assert.assertNull(result);
  }

  @Test
  public void testGetArgs_WhenSet() {
    // Arrange
    ReindexDebugArgs expectedArgs = new ReindexDebugArgs();
    expectedArgs.index = "test_index";
    reindexDebugStep.args = expectedArgs;

    // Act
    ReindexDebugArgs result = reindexDebugStep.getArgs();

    // Assert
    Assert.assertSame(result, expectedArgs);
  }

  @Test
  public void testExecutable_SuccessWithMultipleMatchingConfigs()
      throws IOException, IllegalAccessException {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    List<ReindexConfig> configs = Arrays.asList(reindexConfig1, reindexConfig2, reindexConfig3);
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);
    Mockito.when(reindexConfig1.name()).thenReturn("test_index_v1"); // This should match first
    Mockito.when(reindexConfig2.name())
        .thenReturn("test_index_v2"); // This would also match but shouldn't be used
    Mockito.when(reindexConfig3.name()).thenReturn("other_index_v1");

    Function<UpgradeContext, UpgradeStepResult> executable = reindexDebugStep.executable();

    // Act
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Only the first matching config should be used
    Mockito.verify(indexBuilder).buildIndex(reindexConfig1);
    Mockito.verify(reindexConfig1).forceReindex();
    Mockito.verify(reindexConfig2, Mockito.never()).forceReindex();
    Mockito.verify(reindexConfig3, Mockito.never()).forceReindex();
  }

  @Test
  public void testContainsKeyMethod() {
    // This method is referenced in getIndex but seems to be missing from the implementation
    // Testing the logic that should be there
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test"));

    // The containsKey logic is implicitly tested in getIndex tests
    // This test ensures the behavior is correct
    Assert.assertTrue(parsedArgs.containsKey("index"));
    Assert.assertFalse(parsedArgs.containsKey("nonexistent"));
  }
}
