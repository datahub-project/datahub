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
  void testImplicitObjectTypeNormalizedAcrossSides() {
    // ES8 echoes "type":"object" back for any field that has "properties"; ES7 / OpenSearch
    // omit it. Mapping builders that don't emit the explicit type must not cause a perpetual
    // mapping diff (and therefore a perpetual reindex loop) when running against ES8.
    Map<String, Object> currentMappings = new HashMap<>();
    Map<String, Object> currentProperties = new HashMap<>();
    Map<String, Object> currentObjectField = new HashMap<>();
    currentObjectField.put(TYPE_KEY, "object");
    currentObjectField.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "partition", ImmutableMap.of("type", "keyword"),
            "timePartition", ImmutableMap.of("type", "keyword")));
    currentProperties.put("partitionSpec", currentObjectField);
    currentMappings.put(PROPERTIES_KEY, currentProperties);

    Map<String, Object> targetMappings = new HashMap<>();
    Map<String, Object> targetProperties = new HashMap<>();
    targetProperties.put(
        "partitionSpec",
        ImmutableMap.of(
            PROPERTIES_KEY,
            ImmutableMap.of(
                "partition", ImmutableMap.of("type", "keyword"),
                "timePartition", ImmutableMap.of("type", "keyword"))));
    targetMappings.put(PROPERTIES_KEY, targetProperties);

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

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "Implicit vs explicit type:object on a properties-bearing sub-mapping must not be"
            + " treated as a diff");
    Assert.assertFalse(config.requiresReindex());
  }

  @Test
  void testImplicitObjectTypeNormalized_TargetExplicit_CurrentImplicit() {
    // Symmetric to testImplicitObjectTypeNormalizedAcrossSides: confirm normalization works
    // when current is implicit and target is explicit.
    Map<String, Object> currentMappings = new HashMap<>();
    currentMappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "partitionSpec",
            ImmutableMap.of(
                PROPERTIES_KEY, ImmutableMap.of("partition", ImmutableMap.of("type", "keyword")))));

    Map<String, Object> targetMappings = new HashMap<>();
    targetMappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "partitionSpec",
            ImmutableMap.of(
                TYPE_KEY,
                "object",
                PROPERTIES_KEY,
                ImmutableMap.of("partition", ImmutableMap.of("type", "keyword")))));

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

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "Normalization must be symmetric: current implicit + target explicit should match");
    Assert.assertFalse(config.requiresReindex());
  }

  @Test
  void testBothSidesImplicitObjectMatch() {
    // Defensive: when both sides emit implicit object (properties only, no type),
    // they should still compare equal after normalization.
    Map<String, Object> mappings = new HashMap<>();
    mappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "partitionSpec",
            ImmutableMap.of(
                PROPERTIES_KEY, ImmutableMap.of("partition", ImmutableMap.of("type", "keyword")))));

    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(mappings)
            .targetMappings(mappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertFalse(config.requiresApplyMappings(), "Both sides implicit object — should match");
  }

  @Test
  void testNestedTypeNotNormalizedToObject() {
    // type:nested has properties but explicit type — normalization must skip it
    // (TYPE is already present, condition is false).
    Map<String, Object> mappings = new HashMap<>();
    mappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "tags",
            ImmutableMap.of(
                TYPE_KEY,
                "nested",
                PROPERTIES_KEY,
                ImmutableMap.of("name", ImmutableMap.of("type", "keyword")))));

    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(mappings)
            .targetMappings(mappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "Identical nested mappings should match without object normalization interfering");
  }

  @Test
  void testObjectToNestedTransitionDetected() {
    // Real change: implicit object → explicit nested. The fix must NOT hide this diff
    // by normalizing both sides to type:object.
    Map<String, Object> currentMappings = new HashMap<>();
    currentMappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "tags",
            ImmutableMap.of(
                PROPERTIES_KEY, ImmutableMap.of("name", ImmutableMap.of("type", "keyword")))));

    Map<String, Object> targetMappings = new HashMap<>();
    targetMappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "tags",
            ImmutableMap.of(
                TYPE_KEY,
                "nested",
                PROPERTIES_KEY,
                ImmutableMap.of("name", ImmutableMap.of("type", "keyword")))));

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

    Assert.assertTrue(
        config.requiresApplyMappings(),
        "object→nested transition is a real schema change — normalization must not hide it");
    Assert.assertTrue(config.requiresReindex());
  }

  @Test
  void testMultiFieldsNotAffected() {
    // Multi-fields use the 'fields' key (not 'properties'), so the normalization
    // condition is false. Comparison should work normally.
    Map<String, Object> mappings = new HashMap<>();
    mappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "name",
            ImmutableMap.of(
                TYPE_KEY,
                "text",
                "fields",
                ImmutableMap.of("keyword", ImmutableMap.of("type", "keyword")))));

    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(mappings)
            .targetMappings(mappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "Multi-fields ('fields' key, not 'properties') should be unaffected by object normalization");
  }

  @Test
  void testFieldAliasNotAffected() {
    // Field aliases have type:alias and path, but no properties. Normalization
    // should leave them alone.
    Map<String, Object> mappings = new HashMap<>();
    mappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "_entityName",
            ImmutableMap.of(TYPE_KEY, "alias", "path", "name"),
            "name",
            ImmutableMap.of(TYPE_KEY, "keyword")));

    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(mappings)
            .targetMappings(mappings)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "Field aliases have a type but no properties — normalization must not affect them");
  }

  @Test
  void testCombinedTypeAndPropertyChangeStillDetected() {
    // Combined change: current is implicit object, target is explicit nested with a new field.
    // Both the type change AND the property addition must be detected — the object-type
    // injection must not mask either signal.
    Map<String, Object> currentMappings = new HashMap<>();
    currentMappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "tags",
            ImmutableMap.of(
                PROPERTIES_KEY, ImmutableMap.of("name", ImmutableMap.of("type", "keyword")))));

    Map<String, Object> targetMappings = new HashMap<>();
    targetMappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "tags",
            ImmutableMap.of(
                TYPE_KEY,
                "nested",
                PROPERTIES_KEY,
                ImmutableMap.of(
                    "name", ImmutableMap.of("type", "keyword"),
                    "color", ImmutableMap.of("type", "keyword")))));

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

    Assert.assertTrue(
        config.requiresApplyMappings(),
        "Combined type+property change must be detected even when current is implicit object");
    Assert.assertTrue(config.requiresReindex());
    Assert.assertFalse(
        config.isPureMappingsAddition(),
        "Type change makes this non-additive; should require reindex");
  }

  // ---------------------------------------------------------------------------------------------
  // Engine-type round-trip simulations
  //
  // ReindexConfig.normalizeMapForComparison applies its type:object injection universally — it
  // does not depend on engineType. These tests document the mapping shape each search engine
  // typically returns during a round-trip and verify the universal normalization handles all
  // three engines correctly. The "code" side is always assumed to emit implicit-object form
  // (no explicit type when properties is present) — matching V2MappingsBuilder's output.
  // ---------------------------------------------------------------------------------------------

  /**
   * Builds a code-emitted (target) mapping that uses the implicit-object form (no `type` key on
   * properties-bearing nodes). This mirrors what V2MappingsBuilder produces.
   */
  private static Map<String, Object> implicitObjectMapping() {
    Map<String, Object> mappings = new HashMap<>();
    mappings.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "partitionSpec",
            ImmutableMap.of(
                PROPERTIES_KEY,
                ImmutableMap.of(
                    "partition", ImmutableMap.of("type", "keyword"),
                    "timePartition", ImmutableMap.of("type", "keyword")))));
    return mappings;
  }

  /**
   * Builds a stored mapping in the explicit-object shape (ES8-style) — every properties-bearing
   * node has an explicit `"type": "object"` added by ES8's round-trip behaviour.
   */
  private static Map<String, Object> explicitObjectMapping() {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> partitionSpec = new HashMap<>();
    partitionSpec.put(TYPE_KEY, "object");
    partitionSpec.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "partition", ImmutableMap.of("type", "keyword"),
            "timePartition", ImmutableMap.of("type", "keyword")));
    mappings.put(PROPERTIES_KEY, ImmutableMap.of("partitionSpec", partitionSpec));
    return mappings;
  }

  @Test
  void testEngineRoundTrip_ES7_PreservesImplicitObject() {
    // ES7 round-trip: stored mapping looks the same as what was sent.
    // Both sides remain implicit-object form. Comparison must report no diff.
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(implicitObjectMapping())
            .targetMappings(implicitObjectMapping())
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "ES7 preserves implicit-object form on round-trip — both sides should match");
    Assert.assertFalse(config.requiresReindex());
  }

  @Test
  void testEngineRoundTrip_OpenSearch_PreservesImplicitObject() {
    // OpenSearch behaves like ES7 on object round-trips: implicit form preserved.
    // The universal normalization should be a no-op (both sides already match).
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(implicitObjectMapping())
            .targetMappings(implicitObjectMapping())
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "OpenSearch preserves implicit-object form on round-trip — both sides should match");
    Assert.assertFalse(config.requiresReindex());
  }

  @Test
  void testEngineRoundTrip_ES8_AddsExplicitTypeObject_ResolvedByNormalization() {
    // ES8 round-trip: stored mapping gains "type":"object" automatically.
    // Code still emits implicit-object form. Without the universal fix this would be a
    // perpetual diff (the PFP-3594 loop). With the fix, normalization adds type:object to
    // the implicit (target) side, making them equivalent.
    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(explicitObjectMapping()) // ES8-style stored shape
            .targetMappings(implicitObjectMapping()) // code-emitted shape
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "PFP-3594 fix: ES8 round-trip drift must NOT produce a synthetic mapping diff");
    Assert.assertFalse(
        config.requiresReindex(), "No real schema change — must not trigger reindex on ES8");
  }

  @Test
  void testEngineRoundTrip_ES8_RealMappingChangeStillDetected() {
    // Even with the ES8 round-trip drift in play, a real schema change underneath must still
    // be detected. Here current (ES8 stored) has explicit type:object + one inner field, but
    // target (code) has implicit + an additional inner field. The added field must surface.
    Map<String, Object> currentEs8 = new HashMap<>();
    Map<String, Object> currentPartitionSpec = new HashMap<>();
    currentPartitionSpec.put(TYPE_KEY, "object");
    currentPartitionSpec.put(
        PROPERTIES_KEY, ImmutableMap.of("partition", ImmutableMap.of("type", "keyword")));
    currentEs8.put(PROPERTIES_KEY, ImmutableMap.of("partitionSpec", currentPartitionSpec));

    Map<String, Object> targetCode = new HashMap<>();
    targetCode.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "partitionSpec",
            ImmutableMap.of(
                PROPERTIES_KEY,
                ImmutableMap.of(
                    "partition", ImmutableMap.of("type", "keyword"),
                    "timePartition", ImmutableMap.of("type", "keyword") // new field
                    ))));

    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentEs8)
            .targetMappings(targetCode)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertTrue(
        config.requiresApplyMappings(),
        "Real schema change (new inner field) must still be detected on ES8 even when "
            + "the type:object round-trip drift would otherwise mask it");
  }

  @Test
  void testEngineRoundTrip_ES7_RealMappingChangeStillDetected() {
    // Sanity check that the universal normalization does not inadvertently mask real changes
    // on ES7/OpenSearch either.
    Map<String, Object> currentEs7 = implicitObjectMapping();

    Map<String, Object> targetCode = new HashMap<>();
    targetCode.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "partitionSpec",
            ImmutableMap.of(
                PROPERTIES_KEY,
                ImmutableMap.of(
                    "partition", ImmutableMap.of("type", "keyword"),
                    "timePartition", ImmutableMap.of("type", "keyword"),
                    "extra", ImmutableMap.of("type", "long") // new field
                    ))));

    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(currentEs7)
            .targetMappings(targetCode)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertTrue(
        config.requiresApplyMappings(),
        "Real schema change must still be detected on ES7/OpenSearch — universal "
            + "normalization is mathematically incapable of masking a content diff");
  }

  @Test
  void testEngineRoundTrip_ES8_NestedTypeNotMisIdentified() {
    // ES8 returns "type":"nested" for nested fields just as it was sent. The universal
    // normalization must not misclassify a nested field as object even when both sides have
    // properties.
    Map<String, Object> nestedMapping = new HashMap<>();
    nestedMapping.put(
        PROPERTIES_KEY,
        ImmutableMap.of(
            "tags",
            ImmutableMap.of(
                TYPE_KEY,
                "nested",
                PROPERTIES_KEY,
                ImmutableMap.of("name", ImmutableMap.of("type", "keyword")))));

    ReindexConfig config =
        ReindexConfig.builder()
            .name(TEST_INDEX_NAME)
            .exists(true)
            .currentMappings(nestedMapping)
            .targetMappings(nestedMapping)
            .currentSettings(Settings.EMPTY)
            .targetSettings(new HashMap<>())
            .enableIndexMappingsReindex(true)
            .build();

    Assert.assertFalse(
        config.requiresApplyMappings(),
        "type:nested fields must remain nested through normalization — must not be auto-promoted to object");
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
  void testStructuredPropertyAdditionWithDynamicTrue() {
    // Verify new SP detection works when structuredProperties has dynamic=true,
    // matching real V2MappingsBuilder output. This was a bug where calculateMapDifference
    // stripped structuredProperties from the diff because it was dynamic.
    Map<String, Object> currentMappings =
        createMappingsWithDynamicStructuredProperties(
            ImmutableMap.of("prop1", ImmutableMap.of("type", "text")));
    Map<String, Object> targetMappings =
        createMappingsWithDynamicStructuredProperties(
            ImmutableMap.of(
                "prop1", ImmutableMap.of("type", "text"),
                "prop2", ImmutableMap.of("type", "keyword")));

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

    Assert.assertTrue(config.hasNewStructuredProperty());
    Assert.assertTrue(config.isPureStructuredPropertyAddition());
    Assert.assertFalse(config.hasRemovedStructuredProperty());
  }

  @Test
  void testStructuredPropertyRemovalWithDynamicTrue() {
    Map<String, Object> currentMappings =
        createMappingsWithDynamicStructuredProperties(
            ImmutableMap.of(
                "prop1", ImmutableMap.of("type", "text"),
                "prop2", ImmutableMap.of("type", "keyword")));
    Map<String, Object> targetMappings =
        createMappingsWithDynamicStructuredProperties(
            ImmutableMap.of("prop1", ImmutableMap.of("type", "text")));

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

    Assert.assertFalse(config.hasNewStructuredProperty());
    Assert.assertTrue(config.hasRemovedStructuredProperty());
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

  private Map<String, Object> createMappingsWithDynamicStructuredProperties(
      Map<String, Object> structuredProps) {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();

    Map<String, Object> structuredPropertyMapping = new HashMap<>();
    structuredPropertyMapping.put("type", "object");
    structuredPropertyMapping.put("dynamic", true);
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
}
