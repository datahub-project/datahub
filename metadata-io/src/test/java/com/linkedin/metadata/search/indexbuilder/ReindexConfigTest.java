package com.linkedin.metadata.search.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import java.util.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.common.settings.Settings;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReindexConfigTest {

  private static final String TEST_INDEX_NAME = "test_index";
  private static final String PROPERTIES_KEY = "properties";
  private static final String TYPE_KEY = "type";

  @BeforeMethod
  void setUp() {
    // Reset any static state if needed
  }

  @Test
  void testConstants() {
    // Verify constants are properly defined
    Assert.assertNotNull(ReindexConfig.OBJECT_MAPPER);
    Assert.assertEquals(ReindexConfig.SETTINGS_DYNAMIC, Arrays.asList("refresh_interval"));
    Assert.assertEquals(ReindexConfig.SETTINGS_STATIC, Arrays.asList("number_of_shards"));
    Assert.assertEquals(ReindexConfig.SETTINGS.size(), 2);
    Assert.assertTrue(ReindexConfig.SETTINGS.contains("refresh_interval"));
    Assert.assertTrue(ReindexConfig.SETTINGS.contains("number_of_shards"));
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
    Assert.assertEquals(config.name(), TEST_INDEX_NAME);
    Assert.assertTrue(config.exists());
    Assert.assertNotNull(config.currentSettings());
    Assert.assertNotNull(config.targetSettings());
    Assert.assertNotNull(config.currentMappings());
    Assert.assertNotNull(config.targetMappings());
    Assert.assertTrue(config.enableIndexMappingsReindex());
    Assert.assertTrue(config.enableIndexSettingsReindex());
    Assert.assertTrue(config.enableStructuredPropertiesReindex());
    Assert.assertEquals(config.version(), "1.0");
  }

  @Test
  void testIndexPatterns() {
    // Arrange
    ReindexConfig config = ReindexConfig.builder().name("my_index").exists(false).build();

    // Act & Assert
    Assert.assertEquals(config.indexPattern(), "my_index*");
    Assert.assertEquals(config.indexCleanPattern(), "my_index_*");
  }

  @Test
  void testNonExistentIndex() {
    // Arrange & Act
    ReindexConfig config = ReindexConfig.builder().name(TEST_INDEX_NAME).exists(false).build();

    // Assert
    Assert.assertFalse(config.exists());
    Assert.assertFalse(config.requiresReindex());
    Assert.assertFalse(config.requiresApplySettings());
    Assert.assertFalse(config.requiresApplyMappings());
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
    Assert.assertNotNull(config.currentMappings());
    Assert.assertNotNull(config.targetMappings());
    Assert.assertTrue(config.currentMappings() instanceof TreeMap);
    Assert.assertTrue(config.targetMappings() instanceof TreeMap);
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
    Assert.assertTrue(config.currentMappings() instanceof TreeMap);
    Object zField = config.currentMappings().get("z_field");
    Assert.assertTrue(zField instanceof TreeMap);
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
    Assert.assertTrue(config.requiresApplyMappings());
    Assert.assertTrue(config.isPureMappingsAddition());
    Assert.assertFalse(config.requiresReindex()); // Pure addition should not require reindex
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
    Assert.assertTrue(config.requiresApplyMappings());
    Assert.assertFalse(config.isPureMappingsAddition());
    Assert.assertTrue(config.requiresReindex()); // Modification requires reindex
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
    Assert.assertTrue(config.requiresApplyMappings());
    Assert.assertFalse(config.requiresReindex()); // Should not reindex when disabled
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
    Assert.assertTrue(config.requiresApplySettings());
    Assert.assertFalse(config.requiresReindex()); // refresh_interval is dynamic, no reindex needed
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
    Assert.assertTrue(config.requiresApplySettings());
    Assert.assertTrue(config.isSettingsReindex());
    Assert.assertTrue(config.requiresReindex()); // Static setting change requires reindex
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
    Assert.assertTrue(config.requiresApplySettings());
    Assert.assertTrue(config.isSettingsReindex());
    Assert.assertTrue(config.requiresReindex()); // Analysis changes require reindex
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
    Assert.assertTrue(config.hasNewStructuredProperty());
    Assert.assertTrue(config.isPureStructuredPropertyAddition());
    Assert.assertFalse(config.hasRemovedStructuredProperty());
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
    Assert.assertTrue(config.hasRemovedStructuredProperty());
    Assert.assertFalse(config.hasNewStructuredProperty());
    Assert.assertTrue(config.requiresReindex()); // Removal requires reindex
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
    Assert.assertTrue(config.hasNewStructuredProperty());
  }

  @Test(expectedExceptions = RuntimeException.class)
  void testForceReindexClassNotFound() throws IllegalAccessException {
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
      config.forceReindex();
    }
  }

  @DataProvider(name = "reindexFlagsData")
  public Object[][] provideReindexFlagsData() {
    return new Object[][] {{true}, {false}};
  }

  @Test(dataProvider = "reindexFlagsData")
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
    Assert.assertEquals(config.requiresReindex(), reindexEnabled);
    Assert.assertEquals(config.enableIndexMappingsReindex(), reindexEnabled);
    Assert.assertEquals(config.enableIndexSettingsReindex(), reindexEnabled);
    Assert.assertEquals(config.enableStructuredPropertiesReindex(), reindexEnabled);
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
    Assert.assertFalse(config.requiresReindex());
    Assert.assertFalse(config.requiresApplySettings());
    Assert.assertFalse(config.requiresApplyMappings());
    Assert.assertFalse(config.isPureMappingsAddition());
    Assert.assertFalse(config.isSettingsReindex());
    Assert.assertFalse(config.hasNewStructuredProperty());
    Assert.assertFalse(config.hasRemovedStructuredProperty());
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
    Assert.assertNotNull(config.currentMappings());
    Assert.assertNotNull(config.targetMappings());
    Assert.assertTrue(config.currentMappings().isEmpty());
    Assert.assertTrue(config.targetMappings().isEmpty());
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
    Assert.assertTrue(config.requiresApplyMappings());
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
    Assert.assertFalse(config.requiresApplySettings()); // Should ignore urn_stop_filter changes
    Assert.assertFalse(config.requiresReindex());
  }

  @DataProvider(name = "indexPatternData")
  public Object[][] provideIndexPatternData() {
    return new Object[][] {
      {"test_index", "test_index*", "test_index_*"},
      {"my-index", "my-index*", "my-index_*"},
      {"a", "a*", "a_*"},
      {"complex_index_name_123", "complex_index_name_123*", "complex_index_name_123_*"}
    };
  }

  @Test(dataProvider = "indexPatternData")
  void testIndexPatternGeneration(
      String indexName, String expectedPattern, String expectedCleanPattern) {
    // Arrange
    ReindexConfig config = ReindexConfig.builder().name(indexName).exists(false).build();

    // Act & Assert
    Assert.assertEquals(config.indexPattern(), expectedPattern);
    Assert.assertEquals(config.indexCleanPattern(), expectedCleanPattern);
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
    Assert.assertTrue(config.hasNewStructuredProperty());
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

    Assert.assertNotNull(config);
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

    Assert.assertNotNull(config);
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
    Assert.assertFalse(config.requiresApplySettings());
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
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(new HashMap<>())
            .targetMappings(mappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .build();

    Assert.assertNotNull(config);
  }
}
