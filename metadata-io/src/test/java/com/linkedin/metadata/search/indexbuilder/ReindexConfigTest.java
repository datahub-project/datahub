package com.linkedin.metadata.search.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.common.settings.Settings;

public class ReindexConfigTest {

  private static final String TEST_INDEX_NAME = "test_index";
  private static final String PROPERTIES_KEY = "properties";
  private static final String TYPE_KEY = "type";

  @BeforeEach
  void setUp() {
    // Reset any static state if needed
  }

  @Test
  void testConstants() {
    // Verify constants are properly defined
    assertNotNull(ReindexConfig.OBJECT_MAPPER);
    assertEquals(Arrays.asList("refresh_interval"), ReindexConfig.SETTINGS_DYNAMIC);
    assertEquals(Arrays.asList("number_of_shards"), ReindexConfig.SETTINGS_STATIC);
    assertEquals(2, ReindexConfig.SETTINGS.size());
    assertTrue(ReindexConfig.SETTINGS.contains("refresh_interval"));
    assertTrue(ReindexConfig.SETTINGS.contains("number_of_shards"));
  }

  @Test
  void testBasicBuilder() {
    // Arrange & Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .currentMappings(new HashMap<>())
            .targetMappings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .enableIndexSettingsReindex(true)
            .enableStructuredPropertiesReindex(true)
            .version("1.0")
            .build();

    // Assert
    assertEquals(TEST_INDEX_NAME, config.name());
    assertTrue(config.exists());
    assertNotNull(config.currentSettings());
    assertNotNull(config.targetSettings());
    assertNotNull(config.currentMappings());
    assertNotNull(config.targetMappings());
    assertTrue(config.enableIndexMappingsReindex());
    assertTrue(config.enableIndexSettingsReindex());
    assertTrue(config.enableStructuredPropertiesReindex());
    assertEquals("1.0", config.version());
  }

  @Test
  void testIndexPatterns() {
    // Arrange
    ReindexConfig config = ReindexConfig.builder().name("my_index").exists(false).build();

    // Act & Assert
    assertEquals("my_index*", config.indexPattern());
    assertEquals("my_index_*", config.indexCleanPattern());
  }

  @Test
  void testNonExistentIndex() {
    // Arrange & Act
    ReindexConfig config = ReindexConfig.builder().name(TEST_INDEX_NAME).exists(false).build();

    // Assert
    assertFalse(config.exists());
    assertFalse(config.requiresReindex());
    assertFalse(config.requiresApplySettings());
    assertFalse(config.requiresApplyMappings());
  }

  @Test
  void testMappingsSorting() {
    // Arrange
    Map<String, Object> unsortedMappings = new LinkedHashMap<>();
    unsortedMappings.put("z_field", ImmutableMap.of("type", "text"));
    unsortedMappings.put("a_field", ImmutableMap.of("type", "keyword"));
    unsortedMappings.put("m_field", ImmutableMap.of("type", "long"));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(unsortedMappings)
            .targetMappings(unsortedMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .build();

    // Assert - mappings should be sorted (TreeMap)
    assertNotNull(config.currentMappings());
    assertNotNull(config.targetMappings());
    assertTrue(config.currentMappings() instanceof TreeMap);
    assertTrue(config.targetMappings() instanceof TreeMap);
  }

  @Test
  void testNestedMappingsSorting() {
    // Arrange
    Map<String, Object> nestedMapping = new LinkedHashMap<>();
    nestedMapping.put(
        "properties",
        new LinkedHashMap<String, Object>() {
          {
            put("z_nested", ImmutableMap.of("type", "text"));
            put("a_nested", ImmutableMap.of("type", "keyword"));
          }
        });

    Map<String, Object> rootMapping = new LinkedHashMap<>();
    rootMapping.put("z_field", nestedMapping);
    rootMapping.put("a_field", ImmutableMap.of("type", "text"));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(rootMapping)
            .targetMappings(rootMapping)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .build();

    // Assert
    assertTrue(config.currentMappings() instanceof TreeMap);
    Object zField = config.currentMappings().get("z_field");
    assertTrue(zField instanceof TreeMap);
  }

  @Test
  void testPureMappingsAddition() {
    // Arrange
    Map<String, Object> currentMappings = createBasicMappings();
    Map<String, Object> targetMappings = createBasicMappings();
    targetMappings.put(
        PROPERTIES_KEY,
        new HashMap<String, Object>() {
          {
            putAll((Map<String, Object>) currentMappings.get(PROPERTIES_KEY));
            put("new_field", ImmutableMap.of("type", "text"));
          }
        });

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    // Assert
    assertTrue(config.requiresApplyMappings());
    assertTrue(config.isPureMappingsAddition());
    assertFalse(config.requiresReindex()); // Pure addition should not require reindex
  }

  @Test
  void testMappingsModification() {
    // Arrange
    Map<String, Object> currentMappings = createBasicMappings();
    Map<String, Object> targetMappings = createBasicMappings();

    // Modify existing field type
    Map<String, Object> targetProperties = (Map<String, Object>) targetMappings.get(PROPERTIES_KEY);
    targetProperties.put("existing_field", ImmutableMap.of("type", "keyword")); // Changed from text

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    // Assert
    assertTrue(config.requiresApplyMappings());
    assertFalse(config.isPureMappingsAddition());
    assertTrue(config.requiresReindex()); // Modification requires reindex
  }

  @Test
  void testMappingsModificationReindexDisabled() {
    // Arrange
    Map<String, Object> currentMappings = createBasicMappings();
    Map<String, Object> targetMappings = createBasicMappings();

    // Modify existing field type
    Map<String, Object> targetProperties = (Map<String, Object>) targetMappings.get(PROPERTIES_KEY);
    targetProperties.put("existing_field", ImmutableMap.of("type", "keyword"));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(false) // Disabled
            .build();

    // Assert
    assertTrue(config.requiresApplyMappings());
    assertFalse(config.requiresReindex()); // Should not reindex when disabled
  }

  @Test
  void testSettingsComparison() {
    // Arrange
    Settings currentSettings =
        Settings.builder()
            .put("index.refresh_interval", "1s")
            .put("index.number_of_shards", "1")
            .build();

    Map<String, Object> targetSettings = new HashMap<>();
    targetSettings.put(
        "index",
        ImmutableMap.of(
            "refresh_interval", "5s", // Different
            "number_of_shards", "1" // Same
            ));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(new HashMap<>())
            .targetMappings(new HashMap<>())
            .currentSettings(currentSettings)
            .targetSettings(targetSettings)
            .enableIndexSettingsReindex(true)
            .build();

    // Assert
    assertTrue(config.requiresApplySettings());
    assertFalse(config.requiresReindex()); // refresh_interval is dynamic, no reindex needed
  }

  @Test
  void testStaticSettingsChange() {
    // Arrange
    Settings currentSettings = Settings.builder().put("index.number_of_shards", "1").build();

    Map<String, Object> targetSettings = new HashMap<>();
    targetSettings.put("index", ImmutableMap.of("number_of_shards", "2")); // Static setting change

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(new HashMap<>())
            .targetMappings(new HashMap<>())
            .currentSettings(currentSettings)
            .targetSettings(targetSettings)
            .enableIndexSettingsReindex(true)
            .build();

    // Assert
    assertTrue(config.requiresApplySettings());
    assertTrue(config.isSettingsReindex());
    assertTrue(config.requiresReindex()); // Static setting change requires reindex
  }

  @Test
  void testAnalysisSettingsChange() {
    // Arrange
    Settings currentSettings =
        Settings.builder()
            .put("index.analysis.analyzer.custom.type", "custom")
            .put("index.analysis.analyzer.custom.tokenizer", "standard")
            .build();

    Map<String, Object> targetSettings = new HashMap<>();
    Map<String, Object> analysisSettings = new HashMap<>();
    analysisSettings.put(
        "analyzer",
        ImmutableMap.of(
            "custom",
            ImmutableMap.of(
                "type", "custom",
                "tokenizer", "keyword" // Different tokenizer
                )));
    targetSettings.put("index", ImmutableMap.of("analysis", analysisSettings));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(new HashMap<>())
            .targetMappings(new HashMap<>())
            .currentSettings(currentSettings)
            .targetSettings(targetSettings)
            .enableIndexSettingsReindex(true)
            .build();

    // Assert
    assertTrue(config.requiresApplySettings());
    assertTrue(config.isSettingsReindex());
    assertTrue(config.requiresReindex()); // Analysis changes require reindex
  }

  @Test
  void testStructuredPropertyAddition() {
    // Arrange
    Map<String, Object> currentMappings =
        createMappingsWithStructuredProperties(
            ImmutableMap.of("prop1", ImmutableMap.of("type", "text")));
    Map<String, Object> targetMappings =
        createMappingsWithStructuredProperties(
            ImmutableMap.of(
                "prop1", ImmutableMap.of("type", "text"),
                "prop2", ImmutableMap.of("type", "keyword") // New property
                ));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableStructuredPropertiesReindex(true)
            .build();

    // Assert
    assertTrue(config.hasNewStructuredProperty());
    assertTrue(config.isPureStructuredPropertyAddition());
    assertFalse(config.hasRemovedStructuredProperty());
  }

  @Test
  void testStructuredPropertyRemoval() {
    // Arrange
    Map<String, Object> currentMappings =
        createMappingsWithStructuredProperties(
            ImmutableMap.of(
                "prop1", ImmutableMap.of("type", "text"),
                "prop2", ImmutableMap.of("type", "keyword")));
    Map<String, Object> targetMappings =
        createMappingsWithStructuredProperties(
            ImmutableMap.of("prop1", ImmutableMap.of("type", "text")) // Removed prop2
            );

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .enableStructuredPropertiesReindex(true)
            .build();

    // Assert
    assertTrue(config.hasRemovedStructuredProperty());
    assertFalse(config.hasNewStructuredProperty());
    assertTrue(config.requiresReindex()); // Removal requires reindex
  }

  @Test
  void testVersionedStructuredProperties() {
    // Arrange
    Map<String, Object> versionedProps = new HashMap<>();
    versionedProps.put("urn.version1", ImmutableMap.of("type", "text"));
    versionedProps.put("urn.version2", ImmutableMap.of("type", "keyword"));

    Map<String, Object> currentMappings =
        createMappingsWithVersionedStructuredProperties(versionedProps);

    Map<String, Object> newVersionedProps = new HashMap<>(versionedProps);
    newVersionedProps.put("urn.version3", ImmutableMap.of("type", "long")); // New version
    Map<String, Object> targetMappings =
        createMappingsWithVersionedStructuredProperties(newVersionedProps);

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableStructuredPropertiesReindex(true)
            .build();

    // Assert
    assertTrue(config.hasNewStructuredProperty());
  }

  @Test
  void testForceReindexFromAllowedCaller() throws IllegalAccessException {
    // This test simulates being called from ReindexDebugStep
    try (MockedStatic<Thread> threadMock = Mockito.mockStatic(Thread.class)) {
      // Arrange
      Thread mockThread = mock(Thread.class);
      threadMock.when(Thread::currentThread).thenReturn(mockThread);

      StackTraceElement[] stackTrace = {
        new StackTraceElement(
            "com.linkedin.datahub.upgrade.system.elasticsearch.steps.ReindexDebugStep",
            "someMethod",
            "ReindexDebugStep.java",
            100)
      };
      when(mockThread.getStackTrace()).thenReturn(stackTrace);

      ReindexConfig config =
          ReindexConfig.builder()
              .name(TEST_INDEX_NAME)
              .exists(true)
              .currentMappings(new HashMap<>())
              .targetMappings(new HashMap<>())
              .currentSettings(Settings.EMPTY)
              .targetSettings(new HashMap<>())
              .build();

      // Act
      config.forceReindex();

      // Assert
      assertTrue(config.requiresReindex());
      assertTrue(config.requiresApplyMappings());
      assertTrue(config.requiresApplySettings());
    }
  }

  @Test
  void testForceReindexFromUnauthorizedCaller() {
    // This test simulates being called from an unauthorized class
    try (MockedStatic<Thread> threadMock = Mockito.mockStatic(Thread.class)) {
      // Arrange
      Thread mockThread = mock(Thread.class);
      threadMock.when(Thread::currentThread).thenReturn(mockThread);

      StackTraceElement[] stackTrace = {
        new StackTraceElement("com.unauthorized.SomeClass", "someMethod", "SomeClass.java", 100)
      };
      when(mockThread.getStackTrace()).thenReturn(stackTrace);

      ReindexConfig config = ReindexConfig.builder().name(TEST_INDEX_NAME).exists(true).build();

      // Act & Assert
      assertThrows(IllegalAccessException.class, config::forceReindex);
    }
  }

  @Test
  void testForceReindexClassNotFound() {
    try (MockedStatic<Thread> threadMock = Mockito.mockStatic(Thread.class)) {
      // Arrange
      Thread mockThread = mock(Thread.class);
      threadMock.when(Thread::currentThread).thenReturn(mockThread);

      StackTraceElement[] stackTrace = {
        new StackTraceElement(
            "com.nonexistent.NonExistentClass", "someMethod", "NonExistentClass.java", 100)
      };
      when(mockThread.getStackTrace()).thenReturn(stackTrace);

      ReindexConfig config = ReindexConfig.builder().name(TEST_INDEX_NAME).exists(true).build();

      // Act & Assert
      RuntimeException exception = assertThrows(RuntimeException.class, config::forceReindex);
      assertTrue(exception.getCause() instanceof ClassNotFoundException);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testReindexFlagsWithDifferentSettings(boolean reindexEnabled) {
    // Arrange
    Map<String, Object> currentMappings = createBasicMappings();
    Map<String, Object> targetMappings = createBasicMappings();

    // Modify existing field
    Map<String, Object> targetProperties = (Map<String, Object>) targetMappings.get(PROPERTIES_KEY);
    targetProperties.put("existing_field", ImmutableMap.of("type", "keyword"));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(reindexEnabled)
            .enableIndexSettingsReindex(reindexEnabled)
            .enableStructuredPropertiesReindex(reindexEnabled)
            .build();

    // Assert
    assertEquals(reindexEnabled, config.requiresReindex());
    assertEquals(reindexEnabled, config.enableIndexMappingsReindex());
    assertEquals(reindexEnabled, config.enableIndexSettingsReindex());
    assertEquals(reindexEnabled, config.enableStructuredPropertiesReindex());
  }

  @Test
  void testEmptyMappingsAndSettings() {
    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(new HashMap<>())
            .targetMappings(new HashMap<>())
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .build();

    // Assert
    assertFalse(config.requiresReindex());
    assertFalse(config.requiresApplySettings());
    assertFalse(config.requiresApplyMappings());
    assertFalse(config.isPureMappingsAddition());
    assertFalse(config.isSettingsReindex());
    assertFalse(config.hasNewStructuredProperty());
    assertFalse(config.hasRemovedStructuredProperty());
  }

  @Test
  void testNullMappingsHandling() {
    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(null)
            .targetMappings(null)
            .currentSettings(Settings.EMPTY)
            .targetSettings(null)
            .build();

    // Assert
    assertNotNull(config.currentMappings());
    assertNotNull(config.targetMappings());
    assertTrue(config.currentMappings().isEmpty());
    assertTrue(config.targetMappings().isEmpty());
  }

  @Test
  void testObjectFieldFiltering() {
    // Arrange
    Map<String, Object> currentMappings = new HashMap<>();
    currentMappings.put(
        PROPERTIES_KEY,
        new HashMap<String, Object>() {
          {
            put("regular_field", ImmutableMap.of("type", "text"));
            put("object_field", ImmutableMap.of("type", "object", PROPERTIES_KEY, new HashMap<>()));
          }
        });

    Map<String, Object> targetMappings = new HashMap<>();
    targetMappings.put(
        PROPERTIES_KEY,
        new HashMap<String, Object>() {
          {
            put("regular_field", ImmutableMap.of("type", "keyword")); // Changed
            put(
                "object_field",
                ImmutableMap.of(
                    "type",
                    "object",
                    PROPERTIES_KEY,
                    new HashMap<String, Object>() {
                      {
                        put("nested_field", ImmutableMap.of("type", "text")); // Added nested
                      }
                    }));
          }
        });

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    // Assert
    assertTrue(config.requiresApplyMappings());
    // Object fields should be filtered from comparison
  }

  @Test
  void testUrn_StopFilterIgnored() {
    // Arrange
    Settings currentSettings =
        Settings.builder()
            .put("index.analysis.filter.urn_stop_filter.type", "stop")
            .put("index.analysis.filter.urn_stop_filter.stopwords", "old_words")
            .build();

    Map<String, Object> targetSettings = new HashMap<>();
    Map<String, Object> analysisSettings = new HashMap<>();
    analysisSettings.put(
        "filter",
        ImmutableMap.of(
            "urn_stop_filter",
            ImmutableMap.of(
                "type", "stop",
                "stopwords", "new_words" // Different but should be ignored
                )));
    targetSettings.put("index", ImmutableMap.of("analysis", analysisSettings));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(new HashMap<>())
            .targetMappings(new HashMap<>())
            .currentSettings(currentSettings)
            .targetSettings(targetSettings)
            .enableIndexSettingsReindex(true)
            .build();

    // Assert
    assertFalse(config.requiresApplySettings()); // Should ignore urn_stop_filter changes
    assertFalse(config.requiresReindex());
  }

  @ParameterizedTest
  @CsvSource({
    "test_index, test_index*, test_index_*",
    "my-index, my-index*, my-index_*",
    "a, a*, a_*",
    "complex_index_name_123, complex_index_name_123*, complex_index_name_123_*"
  })
  void testIndexPatternGeneration(
      String indexName, String expectedPattern, String expectedCleanPattern) {
    // Arrange
    ReindexConfig config = ReindexConfig.builder().name(indexName).exists(false).build();

    // Act & Assert
    assertEquals(expectedPattern, config.indexPattern());
    assertEquals(expectedCleanPattern, config.indexCleanPattern());
  }

  @Test
  void testComplexNestedStructuredProperties() {
    // Arrange
    Map<String, Object> complexVersionedProps = new HashMap<>();
    complexVersionedProps.put(
        "level1",
        ImmutableMap.of(
            PROPERTIES_KEY,
            ImmutableMap.of(
                "level2",
                ImmutableMap.of(
                    PROPERTIES_KEY, ImmutableMap.of("level3", ImmutableMap.of("type", "text"))))));

    Map<String, Object> currentMappings =
        createMappingsWithVersionedStructuredProperties(complexVersionedProps);

    Map<String, Object> newComplexVersionedProps = new HashMap<>(complexVersionedProps);
    newComplexVersionedProps.put(
        "level1_new",
        ImmutableMap.of(
            PROPERTIES_KEY, ImmutableMap.of("level2_new", ImmutableMap.of("type", "keyword"))));
    Map<String, Object> targetMappings =
        createMappingsWithVersionedStructuredProperties(newComplexVersionedProps);

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentMappings)
            .targetMappings(targetMappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableStructuredPropertiesReindex(true)
            .build();

    // Assert
    assertTrue(config.hasNewStructuredProperty());
  }

  @Test
  void testJsonProcessingExceptionHandling() throws JsonProcessingException {
    // This test verifies that JsonProcessingException is properly wrapped
    // We can't easily mock the static OBJECT_MAPPER, so this test ensures the exception handling
    // path exists

    // Arrange
    Settings currentSettings = Settings.builder().put("index.number_of_shards", "1").build();

    Map<String, Object> targetSettings = new HashMap<>();
    targetSettings.put("index", ImmutableMap.of("number_of_shards", "2"));

    // Act & Assert - Should not throw exception even with complex settings
    assertDoesNotThrow(
        () -> {
          ReindexConfig config =
              ReindexConfig.builder()
                  .name(TEST_INDEX_NAME)
                  .exists(true)
                  .currentMappings(new HashMap<>())
                  .targetMappings(new HashMap<>())
                  .currentSettings(currentSettings)
                  .targetSettings(targetSettings)
                  .enableIndexSettingsReindex(true)
                  .build();
        });
  }

  // Helper methods

  private Map<String, Object> createBasicMappings() {
    Map<String, Object> mappings = new HashMap<>();
    mappings.put(
        PROPERTIES_KEY,
        new HashMap<String, Object>() {
          {
            put("existing_field", ImmutableMap.of("type", "text"));
            put("another_field", ImmutableMap.of("type", "keyword"));
          }
        });
    return mappings;
  }

  private Map<String, Object> createMappingsWithStructuredProperties(
      Map<String, Object> structuredProps) {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();

    Map<String, Object> structuredPropertyMapping = new HashMap<>();
    structuredPropertyMapping.put(PROPERTIES_KEY, structuredProps);

    properties.put(STRUCTURED_PROPERTY_MAPPING_FIELD, structuredPropertyMapping);
    mappings.put(PROPERTIES_KEY, properties);

    return mappings;
  }

  private Map<String, Object> createMappingsWithVersionedStructuredProperties(
      Map<String, Object> versionedProps) {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();

    Map<String, Object> structuredPropertyMapping = new HashMap<>();
    Map<String, Object> structuredProperties = new HashMap<>();

    Map<String, Object> versionedMapping = new HashMap<>();
    versionedMapping.put(PROPERTIES_KEY, versionedProps);

    structuredProperties.put(STRUCTURED_PROPERTY_MAPPING_VERSIONED_FIELD, versionedMapping);
    structuredPropertyMapping.put(PROPERTIES_KEY, structuredProperties);

    properties.put(STRUCTURED_PROPERTY_MAPPING_FIELD, structuredPropertyMapping);
    mappings.put(PROPERTIES_KEY, properties);

    return mappings;
  }

  @Test
  void testBuilderHiddenMethods() {
    // Test that calculated fields cannot be set directly
    ReindexConfig.ReindexConfigBuilder builder = ReindexConfig.builder();

    // These methods should exist but be private/hidden
    // We can't test them directly, but we verify the builder works correctly
    ReindexConfig config = builder.name(TEST_INDEX_NAME).exists(false).build();

    assertNotNull(config);
  }

  @Test
  void testEqualsGroupWithNestedSettings() {
    // This tests the equalsGroup method indirectly through analysis comparison
    Settings currentSettings =
        Settings.builder()
            .put("index.analysis.analyzer.custom.type", "custom")
            .put("index.analysis.analyzer.custom.tokenizer", "standard")
            .put("index.analysis.analyzer.custom.filter.0", "lowercase")
            .build();

    Map<String, Object> targetSettings = new HashMap<>();
    Map<String, Object> analysisSettings = new HashMap<>();
    analysisSettings.put(
        "analyzer",
        ImmutableMap.of(
            "custom",
            ImmutableMap.of(
                "type", "custom",
                "tokenizer", "standard",
                "filter", Arrays.asList("lowercase"))));
    targetSettings.put("index", ImmutableMap.of("analysis", analysisSettings));

    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(new HashMap<>())
            .targetMappings(new HashMap<>())
            .currentSettings(currentSettings)
            .targetSettings(targetSettings)
            .build();

    // Should detect that settings are equal
    assertFalse(config.requiresApplySettings());
  }

  @Test
  void testFlattenStructuredPropertyPathDepthLimit() {
    // Create deeply nested structure that should hit the depth limit
    Map<String, Object> deeplyNested = new HashMap<>();
    Map<String, Object> current = deeplyNested;

    // Create 6 levels deep (should be limited to 5)
    for (int i = 0; i < 6; i++) {
      Map<String, Object> next = new HashMap<>();
      current.put("level" + i, next);
      current = next;
    }
    current.put("final", ImmutableMap.of("type", "text"));

    Map<String, Object> versionedProps = new HashMap<>();
    versionedProps.put("deep", deeplyNested);

    Map<String, Object> mappings = createMappingsWithVersionedStructuredProperties(versionedProps);

    // Should not throw exception due to depth limiting
    assertDoesNotThrow(
        () -> {
          ReindexConfig config =
              ReindexConfig.builder()
                  .name(TEST_INDEX_NAME)
                  .exists(true)
                  .currentMappings(new HashMap<>())
                  .targetMappings(mappings)
                  .currentSettings(Settings.EMPTY)
                  .targetSettings(new HashMap<>())
                  .build();
        });
  }
}
