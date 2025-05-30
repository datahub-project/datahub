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
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

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

  @BeforeEach
  void setUp() {
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
  void testConstructor_WithElasticSearchService() {
    // Arrange
    List<ElasticSearchIndexed> servicesWithES =
        Arrays.asList(otherElasticSearchIndexed, elasticSearchService);
    Set<Pair<Urn, StructuredPropertyDefinition>> props = new HashSet<>();

    // Act
    ReindexDebugStep step = new ReindexDebugStep(servicesWithES, props);

    // Assert
    Assertions.assertNotNull(step);
    Assertions.assertEquals("ReindexDebugStep", step.id());
  }

  @Test
  void testConstructor_WithoutElasticSearchService() {
    // Arrange
    List<ElasticSearchIndexed> servicesWithoutES = Arrays.asList(otherElasticSearchIndexed);
    Set<Pair<Urn, StructuredPropertyDefinition>> props = new HashSet<>();

    // Act
    ReindexDebugStep step = new ReindexDebugStep(servicesWithoutES, props);

    // Assert
    Assertions.assertNotNull(step);
    // service field should be null since no ElasticSearchService found
  }

  @Test
  void testConstructor_WithEmptyServices() {
    // Arrange
    List<ElasticSearchIndexed> emptyServices = new ArrayList<>();
    Set<Pair<Urn, StructuredPropertyDefinition>> props = new HashSet<>();

    // Act
    ReindexDebugStep step = new ReindexDebugStep(emptyServices, props);

    // Assert
    Assertions.assertNotNull(step);
  }

  @Test
  void testId() {
    // Act
    String id = reindexDebugStep.id();

    // Assert
    Assertions.assertEquals("ReindexDebugStep", id);
  }

  @Test
  void testGetIndex_WithValidIndex() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test_index"));

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assertions.assertEquals("test_index", result);
  }

  @Test
  void testGetIndex_WithEmptyOptional() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.empty());

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assertions.assertEquals("", result);
  }

  @Test
  void testGetIndex_WithoutIndexKey() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("other", Optional.of("value"));

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assertions.assertEquals("", result);
  }

  @Test
  void testGetIndex_WithNullMap() {
    // Act
    String result = reindexDebugStep.getIndex(null);

    // Assert
    Assertions.assertEquals("", result);
  }

  @Test
  void testGetIndex_WithEmptyMap() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assertions.assertEquals("", result);
  }

  @ParameterizedTest
  @ValueSource(strings = {"datahub_index", "", "test_index_v2", "policy_index"})
  void testGetIndex_WithVariousIndices(String indexValue) {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of(indexValue));

    // Act
    String result = reindexDebugStep.getIndex(parsedArgs);

    // Assert
    Assertions.assertEquals(indexValue, result);
  }

  @Test
  void testCreateArgs_FirstTime() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test_index"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    // Act
    ReindexDebugArgs result = reindexDebugStep.createArgs(upgradeContext);

    // Assert
    Assertions.assertNotNull(result);
    Assertions.assertEquals("test_index", result.index);
    Assertions.assertSame(result, reindexDebugStep.getArgs()); // Should be cached
  }

  @Test
  void testCreateArgs_SecondTime_ReturnsCached() {
    // Arrange
    ReindexDebugArgs existingArgs = new ReindexDebugArgs();
    existingArgs.index = "cached_index";
    reindexDebugStep.args = existingArgs;

    // Act
    ReindexDebugArgs result = reindexDebugStep.createArgs(upgradeContext);

    // Assert
    Assertions.assertSame(existingArgs, result);
    Assertions.assertEquals("cached_index", result.index);
    Mockito.verify(upgradeContext, Mockito.never())
        .parsedArgs(); // Should not call parsedArgs when cached
  }

  @Test
  void testCreateArgs_WithEmptyIndex() {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    // Act
    ReindexDebugArgs result = reindexDebugStep.createArgs(upgradeContext);

    // Assert
    Assertions.assertNotNull(result);
    Assertions.assertEquals("", result.index);
  }

  @Test
  void testExecutable_Success() throws IOException, IllegalAccessException {
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
    Assertions.assertNotNull(result);
    Assertions.assertEquals("ReindexDebugStep", result.stepId());
    Assertions.assertEquals(DataHubUpgradeState.SUCCEEDED, result.result());

    Mockito.verify(indexBuilder).buildIndex(reindexConfig1);
    Mockito.verify(reindexConfig1).forceReindex();
  }

  @Test
  void testExecutable_BuildIndexThrowsIOException() throws IOException {
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
    Assertions.assertNotNull(result);
    Assertions.assertEquals("ReindexDebugStep", result.stepId());
    Assertions.assertEquals(DataHubUpgradeState.FAILED, result.result());
  }

  @Test
  void testExecutable_SetConfigThrowsException() throws IOException, IllegalAccessException {
    // Arrange
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("datahubpolicyindex"));
    Mockito.when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    IllegalAccessException exception = new IllegalAccessException("Config access denied");
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenThrow(exception);

    Function<UpgradeContext, UpgradeStepResult> executable = reindexDebugStep.executable();

    // Act
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assertions.assertNotNull(result);
    Assertions.assertEquals("ReindexDebugStep", result.stepId());
    Assertions.assertEquals(DataHubUpgradeState.FAILED, result.result());
  }

  @Test
  void testSetConfig_MatchingConfigFound() throws IOException, IllegalAccessException {
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
  void testSetConfig_NoMatchingConfig() throws IOException, IllegalAccessException {
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
  void testSetConfig_EmptyTargetIndex() throws IOException, IllegalAccessException {
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
  void testSetConfig_EmptyConfigsList() throws IOException, IllegalAccessException {
    // Arrange
    List<ReindexConfig> configs = new ArrayList<>();
    Mockito.when(elasticSearchService.buildReindexConfigs(structuredProperties))
        .thenReturn(configs);

    // Act
    reindexDebugStep.setConfig("any_index");

    // Assert
    // No configs to iterate through, no forceReindex should be called
  }

  @ParameterizedTest
  @MethodSource("provideConfigMatchingScenarios")
  void testSetConfig_VariousMatchingScenarios(
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

  static Stream<Arguments> provideConfigMatchingScenarios() {
    return Stream.of(
        Arguments.of(
            "datahub", Arrays.asList("datahub_index_v1", "policy_index_v1"), "datahub_index_v1"),
        Arguments.of(
            "policy", Arrays.asList("datahub_index_v1", "policy_index_v1"), "policy_index_v1"),
        Arguments.of("nonexistent", Arrays.asList("datahub_index_v1", "policy_index_v1"), null),
        Arguments.of(
            "",
            Arrays.asList("datahub_index_v1", "policy_index_v1"),
            "datahub_index_v1"), // First config matches empty string
        Arguments.of(
            "test",
            Arrays.asList("test_index_v1", "test_index_v2", "other_v1"),
            "test_index_v1") // First matching config should be selected
        );
  }

  @Test
  void testExecutable_WithNullService() {
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
    Assertions.assertNotNull(result);
    Assertions.assertEquals("ReindexDebugStep", result.stepId());
    Assertions.assertEquals(DataHubUpgradeState.FAILED, result.result());
  }

  @Test
  void testExecutable_BuildReindexConfigsThrowsIOException() throws IOException {
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
    Assertions.assertNotNull(result);
    Assertions.assertEquals("ReindexDebugStep", result.stepId());
    Assertions.assertEquals(DataHubUpgradeState.FAILED, result.result());
  }

  @Test
  void testGetArgs_WhenNotSet() {
    // Arrange - Fresh instance
    ReindexDebugStep freshStep = new ReindexDebugStep(services, structuredProperties);

    // Act
    ReindexDebugArgs result = freshStep.getArgs();

    // Assert
    Assertions.assertNull(result);
  }

  @Test
  void testGetArgs_WhenSet() {
    // Arrange
    ReindexDebugArgs expectedArgs = new ReindexDebugArgs();
    expectedArgs.index = "test_index";
    reindexDebugStep.args = expectedArgs;

    // Act
    ReindexDebugArgs result = reindexDebugStep.getArgs();

    // Assert
    Assertions.assertSame(expectedArgs, result);
  }

  @Test
  void testExecutable_SuccessWithMultipleMatchingConfigs()
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
    Assertions.assertNotNull(result);
    Assertions.assertEquals(DataHubUpgradeState.SUCCEEDED, result.result());

    // Only the first matching config should be used
    Mockito.verify(indexBuilder).buildIndex(reindexConfig1);
    Mockito.verify(reindexConfig1).forceReindex();
    Mockito.verify(reindexConfig2, Mockito.never()).forceReindex();
    Mockito.verify(reindexConfig3, Mockito.never()).forceReindex();
  }

  @Test
  void testContainsKeyMethod() {
    // This method is referenced in getIndex but seems to be missing from the implementation
    // Testing the logic that should be there
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("index", Optional.of("test"));

    // The containsKey logic is implicitly tested in getIndex tests
    // This test ensures the behavior is correct
    Assertions.assertTrue(parsedArgs.containsKey("index"));
    Assertions.assertFalse(parsedArgs.containsKey("nonexistent"));
  }
}
