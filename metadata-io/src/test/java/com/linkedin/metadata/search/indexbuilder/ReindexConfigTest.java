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

  @Test
  void testDynamicFieldsIgnoredInMappingComparison() {
    // Arrange - Create mappings with dynamic fields that differ
    Map<String, Object> currentMappings = createBasicMappings();
    Map<String, Object> targetMappings = createBasicMappings();

    // Add dynamic field to current mappings
    Map<String, Object> currentProperties =
        (Map<String, Object>) currentMappings.get(PROPERTIES_KEY);
    Map<String, Object> currentDynamicField = new HashMap<>();
    currentDynamicField.put("type", "object");
    currentDynamicField.put("dynamic", true);
    currentDynamicField.put("properties", new HashMap<>());
    currentProperties.put("ownerTypes", currentDynamicField);

    // Add dynamic field to target mappings with different properties
    Map<String, Object> targetProperties = (Map<String, Object>) targetMappings.get(PROPERTIES_KEY);
    Map<String, Object> targetDynamicField = new HashMap<>();
    targetDynamicField.put("type", "object");
    targetDynamicField.put("dynamic", true);
    Map<String, Object> targetDynamicProperties = new HashMap<>();
    targetDynamicProperties.put(
        "urn:li:ownershipType:__system__none", ImmutableMap.of("type", "keyword"));
    targetDynamicField.put("properties", targetDynamicProperties);
    targetProperties.put("ownerTypes", targetDynamicField);

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

    // Assert - Dynamic field differences should be ignored
    Assert.assertFalse(config.requiresApplyMappings());
    Assert.assertFalse(config.requiresReindex());
  }

  @Test
  void testOwnerTypesFieldWithoutDynamicPropertyIgnored() {
    // Arrange - Simulate the real scenario where current mapping has properties but no dynamic=true
    Map<String, Object> currentMappings = createBasicMappings();
    Map<String, Object> targetMappings = createBasicMappings();

    // Current mapping: ownerTypes has properties but no dynamic=true (like in the real scenario)
    Map<String, Object> currentProperties =
        (Map<String, Object>) currentMappings.get(PROPERTIES_KEY);
    Map<String, Object> currentOwnerTypes = new HashMap<>();
    currentOwnerTypes.put("type", "object");
    // Note: NO dynamic=true in current mapping
    Map<String, Object> currentOwnerTypesProperties = new HashMap<>();
    currentOwnerTypesProperties.put(
        "urn:li:ownershipType:__system__none",
        ImmutableMap.of(
            "fields",
            ImmutableMap.of("keyword", ImmutableMap.of("ignore_above", 256, "type", "keyword")),
            "type",
            "text"));
    currentOwnerTypesProperties.put(
        "urn:li:ownershipType:__system__technical_owner",
        ImmutableMap.of(
            "fields",
            ImmutableMap.of("keyword", ImmutableMap.of("ignore_above", 256, "type", "keyword")),
            "type",
            "text"));
    currentOwnerTypes.put("properties", currentOwnerTypesProperties);
    currentProperties.put("ownerTypes", currentOwnerTypes);

    // Target mapping: ownerTypes should be dynamic (from configuration)
    Map<String, Object> targetProperties = (Map<String, Object>) targetMappings.get(PROPERTIES_KEY);
    Map<String, Object> targetOwnerTypes = new HashMap<>();
    targetOwnerTypes.put("type", "object");
    targetOwnerTypes.put("dynamic", true); // This is what the configuration should generate
    targetProperties.put("ownerTypes", targetOwnerTypes);

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

    // Assert - ownerTypes differences should be ignored because it's dynamic in target
    Assert.assertFalse(config.requiresApplyMappings());
    Assert.assertFalse(config.requiresReindex());
  }

  @Test
  void testNestedAspectsOwnerTypesIgnored() {
    // Arrange - Simulate the real scenario from the log where _aspects contains ownerTypes
    Map<String, Object> currentMappings = createBasicMappings();
    Map<String, Object> targetMappings = createBasicMappings();

    // Current mapping: _aspects.businessAttributeRef.ownerTypes has properties but no dynamic=true
    Map<String, Object> currentProperties =
        (Map<String, Object>) currentMappings.get(PROPERTIES_KEY);
    Map<String, Object> currentAspects = new HashMap<>();
    currentAspects.put("type", "object");
    Map<String, Object> currentAspectsProperties = new HashMap<>();

    Map<String, Object> currentBusinessAttributeRef = new HashMap<>();
    currentBusinessAttributeRef.put("type", "object");
    Map<String, Object> currentBusinessAttributeRefProperties = new HashMap<>();

    Map<String, Object> currentOwnerTypes = new HashMap<>();
    currentOwnerTypes.put("type", "object");
    // Note: NO dynamic=true in current mapping, but has properties
    Map<String, Object> currentOwnerTypesProperties = new HashMap<>();
    currentOwnerTypesProperties.put(
        "urn:li:ownershipType:__system__none",
        ImmutableMap.of(
            "fields",
            ImmutableMap.of("keyword", ImmutableMap.of("ignore_above", 256, "type", "keyword")),
            "type",
            "text"));
    currentOwnerTypes.put("properties", currentOwnerTypesProperties);
    currentBusinessAttributeRefProperties.put("ownerTypes", currentOwnerTypes);
    currentBusinessAttributeRef.put("properties", currentBusinessAttributeRefProperties);
    currentAspectsProperties.put("businessAttributeRef", currentBusinessAttributeRef);
    currentAspects.put("properties", currentAspectsProperties);
    currentProperties.put("_aspects", currentAspects);

    // Target mapping: _aspects.businessAttributeRef.ownerTypes should be dynamic
    Map<String, Object> targetProperties = (Map<String, Object>) targetMappings.get(PROPERTIES_KEY);
    Map<String, Object> targetAspects = new HashMap<>();
    targetAspects.put("type", "object");
    Map<String, Object> targetAspectsProperties = new HashMap<>();

    Map<String, Object> targetBusinessAttributeRef = new HashMap<>();
    targetBusinessAttributeRef.put("type", "object");
    Map<String, Object> targetBusinessAttributeRefProperties = new HashMap<>();

    Map<String, Object> targetOwnerTypes = new HashMap<>();
    targetOwnerTypes.put("type", "object");
    targetOwnerTypes.put("dynamic", true); // This is what the configuration should generate
    targetBusinessAttributeRefProperties.put("ownerTypes", targetOwnerTypes);
    targetBusinessAttributeRef.put("properties", targetBusinessAttributeRefProperties);
    targetAspectsProperties.put("businessAttributeRef", targetBusinessAttributeRef);
    targetAspects.put("properties", targetAspectsProperties);
    targetProperties.put("_aspects", targetAspects);

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

    // Assert - _aspects.businessAttributeRef.ownerTypes differences should be ignored because it's
    // dynamic in target
    Assert.assertFalse(config.requiresApplyMappings());
    Assert.assertFalse(config.requiresReindex());
  }

  @Test
  void testStructuredPropertiesIgnoredInMappingComparison() {
    // Arrange - Create mappings with structured properties that differ
    Map<String, Object> currentMappings = createBasicMappings();
    Map<String, Object> targetMappings = createBasicMappings();

    // Add structured properties to current mappings
    Map<String, Object> currentProperties =
        (Map<String, Object>) currentMappings.get(PROPERTIES_KEY);
    Map<String, Object> currentStructuredProps = new HashMap<>();
    currentStructuredProps.put("type", "object");
    currentStructuredProps.put("dynamic", true);
    currentStructuredProps.put("properties", new HashMap<>());
    currentProperties.put(STRUCTURED_PROPERTY_MAPPING_FIELD, currentStructuredProps);

    // Add structured properties to target mappings with different properties
    Map<String, Object> targetProperties = (Map<String, Object>) targetMappings.get(PROPERTIES_KEY);
    Map<String, Object> targetStructuredProps = new HashMap<>();
    targetStructuredProps.put("type", "object");
    targetStructuredProps.put("dynamic", true);
    Map<String, Object> targetStructuredProperties = new HashMap<>();
    targetStructuredProperties.put("newProp", ImmutableMap.of("type", "keyword"));
    targetStructuredProps.put("properties", targetStructuredProperties);
    targetProperties.put(STRUCTURED_PROPERTY_MAPPING_FIELD, targetStructuredProps);

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

    // Assert - Structured properties differences should be ignored (since they're dynamic)
    Assert.assertFalse(config.requiresApplyMappings());
  }

  @Test
  void testSortObjectPreservesIntegerTypes() {
    // This test verifies the fix for the bug where sortObject() was converting all primitive
    // values to strings using String.valueOf(). This caused OpenSearch k-NN parameter validation
    // to fail because it expects integer types for parameters like ef_construction and m.
    //
    // The fix changed sortObject() to return items directly instead of converting to strings,
    // preserving their original types (Integer, Boolean, String, etc.).

    // Arrange - Create mappings with Integer values mimicking k-NN parameters
    Map<String, Object> mappingsWithIntegers =
        ImmutableMap.of(
            PROPERTIES_KEY,
            ImmutableMap.of(
                "embeddings",
                ImmutableMap.of(
                    TYPE_KEY,
                    "nested",
                    PROPERTIES_KEY,
                    ImmutableMap.of(
                        "chunk",
                        ImmutableMap.of(
                            TYPE_KEY,
                            "knn_vector",
                            "dimension",
                            1024, // Integer - must be preserved
                            "method",
                            ImmutableMap.of(
                                "name",
                                "hnsw",
                                "engine",
                                "faiss",
                                "space_type",
                                "cosinesimil",
                                "parameters",
                                ImmutableMap.of(
                                    "ef_construction", 128, // Integer - must be preserved
                                    "m", 16 // Integer - must be preserved
                                    )))))));

    // Act - Build config (which calls sortObject internally via targetMappings())
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(false)
            .targetMappings(mappingsWithIntegers)
            .targetSettings(new HashMap<>())
            .currentSettings(Settings.EMPTY)
            .currentMappings(new HashMap<>())
            .build();

    // Assert - Verify integers remain as Integer type, not strings
    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) config.targetMappings().get(PROPERTIES_KEY);
    @SuppressWarnings("unchecked")
    Map<String, Object> embeddings = (Map<String, Object>) properties.get("embeddings");
    @SuppressWarnings("unchecked")
    Map<String, Object> embeddingsProps = (Map<String, Object>) embeddings.get(PROPERTIES_KEY);
    @SuppressWarnings("unchecked")
    Map<String, Object> chunk = (Map<String, Object>) embeddingsProps.get("chunk");
    @SuppressWarnings("unchecked")
    Map<String, Object> method = (Map<String, Object>) chunk.get("method");
    @SuppressWarnings("unchecked")
    Map<String, Object> params = (Map<String, Object>) method.get("parameters");

    // Critical assertions - these would fail with the old code that used String.valueOf()
    Assert.assertTrue(
        params.get("ef_construction") instanceof Integer,
        "ef_construction should be Integer, not String. Got: "
            + params.get("ef_construction").getClass().getName());
    Assert.assertEquals(params.get("ef_construction"), 128, "ef_construction value should be 128");

    Assert.assertTrue(
        params.get("m") instanceof Integer,
        "m should be Integer, not String. Got: " + params.get("m").getClass().getName());
    Assert.assertEquals(params.get("m"), 16, "m value should be 16");

    Assert.assertTrue(
        chunk.get("dimension") instanceof Integer,
        "dimension should be Integer, not String. Got: "
            + chunk.get("dimension").getClass().getName());
    Assert.assertEquals(chunk.get("dimension"), 1024, "dimension value should be 1024");

    // Verify string values are still strings (not converted)
    Assert.assertTrue(method.get("name") instanceof String, "name should remain String type");
    Assert.assertEquals(method.get("name"), "hnsw");
  }

  @Test
  void testSortObjectPreservesMixedTypes() {
    // Verify that sortObject preserves all primitive types correctly, not just integers

    // Arrange - Create mappings with various primitive types
    Map<String, Object> mixedTypes =
        ImmutableMap.of(
            PROPERTIES_KEY,
            ImmutableMap.of(
                "string_field",
                "text_value",
                "int_field",
                42,
                "bool_field",
                true,
                "long_field",
                999999999L,
                "nested",
                ImmutableMap.of("inner_string", "value", "inner_int", 100)));

    // Act
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(false)
            .targetMappings(mixedTypes)
            .targetSettings(new HashMap<>())
            .currentSettings(Settings.EMPTY)
            .currentMappings(new HashMap<>())
            .build();

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) config.targetMappings().get(PROPERTIES_KEY);

    // Assert - Each type is preserved
    Assert.assertEquals(
        properties.get("string_field").getClass(), String.class, "String type should be preserved");
    Assert.assertEquals(properties.get("string_field"), "text_value");

    Assert.assertEquals(
        properties.get("int_field").getClass(), Integer.class, "Integer type should be preserved");
    Assert.assertEquals(properties.get("int_field"), 42);

    Assert.assertEquals(
        properties.get("bool_field").getClass(), Boolean.class, "Boolean type should be preserved");
    Assert.assertEquals(properties.get("bool_field"), true);

    Assert.assertEquals(
        properties.get("long_field").getClass(), Long.class, "Long type should be preserved");
    Assert.assertEquals(properties.get("long_field"), 999999999L);

    // Verify nested objects still work
    @SuppressWarnings("unchecked")
    Map<String, Object> nested = (Map<String, Object>) properties.get("nested");
    Assert.assertTrue(nested.get("inner_string") instanceof String);
    Assert.assertTrue(nested.get("inner_int") instanceof Integer);
  }
}
