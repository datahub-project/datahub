package com.linkedin.metadata.search.elasticsearch.index;

import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for DelegatingMappingsBuilder with merging functionality. */
public class DelegatingMappingsBuilderTest {

  private DelegatingMappingsBuilder delegatingMappingsBuilder;
  private EntityIndexConfiguration entityIndexConfiguration;
  private OperationContext operationContext;

  @BeforeMethod
  public void setUp() {
    // Use the real configuration from SearchTestUtils
    entityIndexConfiguration = V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;

    // Use the real OperationContext from TestOperationContexts
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    // Initialize DelegatingMappingsBuilder with actual builders
    List<MappingsBuilder> builders = new ArrayList<>();
    if (entityIndexConfiguration.getV2().isEnabled()) {
      builders.add(new V2MappingsBuilder(entityIndexConfiguration));
    }
    if (entityIndexConfiguration.getV3().isEnabled()) {
      try {
        builders.add(new MultiEntityMappingsBuilder(entityIndexConfiguration));
      } catch (IOException e) {
        throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
      }
    }
    if (builders.isEmpty()) {
      builders.add(new NoOpMappingsBuilder());
    }
    delegatingMappingsBuilder = new DelegatingMappingsBuilder(builders);
  }

  @Test
  public void testConstructorWithValidBuilders() {
    // Test that constructor works with valid builders
    assertNotNull(
        delegatingMappingsBuilder, "DelegatingMappingsBuilder should be created successfully");
  }

  @Test
  public void testConstructorWithDisabledVersions() {
    // Test that constructor works when both v2 and v3 are disabled
    EntityIndexConfiguration disabledConfig = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(false);
    when(disabledConfig.getV2()).thenReturn(v2Config);
    when(disabledConfig.getV3()).thenReturn(v3Config);

    // Create DelegatingMappingsBuilder with empty builders list (should add NoOpMappingsBuilder)
    List<MappingsBuilder> builders = new ArrayList<>();
    DelegatingMappingsBuilder disabledBuilder = new DelegatingMappingsBuilder(builders);
    assertNotNull(
        disabledBuilder, "DelegatingMappingsBuilder should be created with disabled versions");
  }

  @Test
  public void testGetIndexMappingsWithV2V3Enabled() {
    // Test getMappings when both v2 and v3 are enabled
    Collection<IndexMapping> result = delegatingMappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(result, "Result should not be null");
    // Should have mappings from both v2 and v3 builders
    assertFalse(result.isEmpty(), "Result should not be empty when both versions are enabled");
  }

  @Test
  public void testGetIndexMappingsWithOnlyV2Enabled() {
    // Test getMappings when only v2 is enabled
    EntityIndexConfiguration v2OnlyConfig = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(false);
    when(v2OnlyConfig.getV2()).thenReturn(v2Config);
    when(v2OnlyConfig.getV3()).thenReturn(v3Config);

    // Create DelegatingMappingsBuilder with only v2 builder
    List<MappingsBuilder> builders = new ArrayList<>();
    builders.add(new V2MappingsBuilder(v2OnlyConfig));
    DelegatingMappingsBuilder v2OnlyBuilder = new DelegatingMappingsBuilder(builders);
    Collection<IndexMapping> result = v2OnlyBuilder.getIndexMappings(operationContext);

    assertNotNull(result, "Result should not be null");
    // Should have mappings from v2 builder only
    assertFalse(result.isEmpty(), "Result should not be empty when v2 is enabled");
  }

  @Test
  public void testGetIndexMappingsWithOnlyV3Enabled() {
    // Test getMappings when only v3 is enabled
    EntityIndexConfiguration v3OnlyConfig =
        EntityIndexConfiguration.builder()
            .v2(EntityIndexVersionConfiguration.builder().enabled(false).build())
            .v3(
                EntityIndexVersionConfiguration.builder()
                    .enabled(true)
                    .cleanup(true)
                    .analyzerConfig("search_entity_analyzer_config.yaml")
                    .mappingConfig("search_entity_mapping_config.yaml")
                    .build())
            .build();

    // Create DelegatingMappingsBuilder with only v3 builder
    List<MappingsBuilder> builders = new ArrayList<>();
    try {
      builders.add(new MultiEntityMappingsBuilder(v3OnlyConfig));
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
    }
    DelegatingMappingsBuilder v3OnlyBuilder = new DelegatingMappingsBuilder(builders);
    Collection<IndexMapping> result = v3OnlyBuilder.getIndexMappings(operationContext);

    assertNotNull(result, "Result should not be null");
    // Should have mappings from v3 builder only
    assertFalse(result.isEmpty(), "Result should not be empty when v3 is enabled");
  }

  @Test
  public void testGetIndexMappingsWithStructuredProperties() {
    // Test getIndexMappings with structured properties
    Collection<IndexMapping> result =
        delegatingMappingsBuilder.getIndexMappings(operationContext, Collections.emptySet());

    assertNotNull(result, "Result should not be null");
    // Should have mappings from both v2 and v3 builders
    assertFalse(result.isEmpty(), "Result should not be empty when both versions are enabled");
  }

  @Test
  public void testGetIndexMappingsForStructuredProperty() {
    // Test getMappingsForStructuredProperty
    Map<String, Object> result =
        delegatingMappingsBuilder.getIndexMappingsForStructuredProperty(Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    // Result may be empty if no structured properties are provided, which is valid
    // The important thing is that the method doesn't throw an exception
  }

  @Test
  public void testMappingsConsistency() {
    // Test that mappings from different builders are consistent
    Collection<IndexMapping> mappings1 =
        delegatingMappingsBuilder.getIndexMappings(operationContext);
    Collection<IndexMapping> mappings2 =
        delegatingMappingsBuilder.getIndexMappings(operationContext);

    // Should be consistent between calls
    assertEquals(mappings1.size(), mappings2.size(), "Mappings should be consistent between calls");

    // Convert to maps for easier comparison
    Map<String, IndexMapping> map1 = new HashMap<>();
    Map<String, IndexMapping> map2 = new HashMap<>();
    mappings1.forEach(mapping -> map1.put(mapping.getIndexName(), mapping));
    mappings2.forEach(mapping -> map2.put(mapping.getIndexName(), mapping));

    assertEquals(map1.keySet(), map2.keySet(), "Index names should be consistent");
  }

  @Test
  public void testMappingsMerging() {
    // Test that mappings from v2 and v3 are properly merged
    Collection<IndexMapping> result = delegatingMappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(result, "Result should not be null");

    // Should have mappings from both v2 and v3
    Map<String, IndexMapping> resultMap = new HashMap<>();
    result.forEach(mapping -> resultMap.put(mapping.getIndexName(), mapping));

    // Should have both v2 and v3 indices (based on naming convention)
    boolean hasV2Indices = resultMap.keySet().stream().anyMatch(name -> name.contains("_v2"));
    boolean hasV3Indices = resultMap.keySet().stream().anyMatch(name -> name.contains("_v3"));

    // At least one should be true (depending on configuration)
    assertTrue(hasV2Indices || hasV3Indices, "Should have either v2 or v3 indices");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testConstructorWithV3EnabledAndInvalidMappingConfig() {
    // Test that constructor throws RuntimeException when MultiEntityMappingsBuilder fails to
    // initialize
    EntityIndexConfiguration invalidConfig = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("non-existent-config.yaml");
    when(invalidConfig.getV2()).thenReturn(v2Config);
    when(invalidConfig.getV3()).thenReturn(v3Config);

    // This should throw RuntimeException wrapping IOException
    List<MappingsBuilder> builders = new ArrayList<>();
    try {
      builders.add(new MultiEntityMappingsBuilder(invalidConfig));
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
    }
    new DelegatingMappingsBuilder(builders);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testConstructorWithV2V3EnabledAndV3Fails() {
    // Test that constructor throws RuntimeException when v3 fails but v2 is enabled
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("invalid-mapping-config.yaml");
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // This should throw RuntimeException wrapping IOException from MultiEntityMappingsBuilder
    List<MappingsBuilder> builders = new ArrayList<>();
    builders.add(new V2MappingsBuilder(config));
    try {
      builders.add(new MultiEntityMappingsBuilder(config));
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
    }
    new DelegatingMappingsBuilder(builders);
  }

  @Test
  public void testConstructorWithV3EnabledAndNullMappingConfig() {
    // Test that constructor works when v3 is enabled but mappingConfig is null
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn(null);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // This should work fine as mappingConfig is null
    List<MappingsBuilder> builders = new ArrayList<>();
    try {
      builders.add(new MultiEntityMappingsBuilder(config));
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
    }
    DelegatingMappingsBuilder builder = new DelegatingMappingsBuilder(builders);
    assertNotNull(
        builder,
        "DelegatingMappingsBuilder should be created successfully with null mappingConfig");
  }

  @Test
  public void testConstructorWithV3EnabledAndEmptyMappingConfig() {
    // Test that constructor works when v3 is enabled but mappingConfig is empty
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("");
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // This should work fine as mappingConfig is empty
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);
    assertNotNull(
        builder,
        "DelegatingMappingsBuilder should be created successfully with empty mappingConfig");
  }

  @Test
  public void testConstructorWithV3EnabledAndWhitespaceMappingConfig() {
    // Test that constructor works when v3 is enabled but mappingConfig is whitespace
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("   ");
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // This should work fine as mappingConfig is whitespace
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);
    assertNotNull(
        builder,
        "DelegatingMappingsBuilder should be created successfully with whitespace mappingConfig");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testConstructorWithV3EnabledAndMalformedMappingConfig() {
    // Test that constructor throws RuntimeException when mappingConfig points to malformed file
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("malformed-config.yaml");
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // This should throw RuntimeException wrapping IOException from malformed config
    createDelegatingMappingsBuilder(config);
  }

  @Test
  public void testExceptionMessageContainsOriginalIOException() {
    // Test that the RuntimeException message contains information about the original IOException
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("non-existent-config.yaml");
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    try {
      createDelegatingMappingsBuilder(config);
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the exception message contains the expected text
      assertTrue(
          e.getMessage().contains("Failed to initialize MultiEntityMappingsBuilder"),
          "Exception message should contain 'Failed to initialize MultiEntityMappingsBuilder'");

      // Verify the cause is IOException
      assertTrue(e.getCause() instanceof IOException, "Exception cause should be IOException");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithFailingFirstBuilder() {
    // Test that getIndexMappings throws RuntimeException when first builder fails
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(false);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with v2 enabled only
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Mock the first builder to throw an exception
    MappingsBuilder mockBuilder = mock(MappingsBuilder.class);
    when(mockBuilder.getIndexMappings(any(OperationContext.class), any()))
        .thenThrow(new RuntimeException("Builder failed"));

    // Use reflection to replace the first builder with our mock
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder);
    } catch (Exception e) {
      fail("Failed to set up mock builder: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping the builder exception
    builder.getIndexMappings(operationContext, null);
  }

  @Test
  public void testGetIndexMappingsExceptionMessageAndCause() {
    // Test that the RuntimeException message and cause are properly set when first builder fails
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(false);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with v2 enabled only
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Mock the first builder to throw a specific exception
    RuntimeException originalException = new RuntimeException("Builder initialization failed");
    MappingsBuilder mockBuilder = mock(MappingsBuilder.class);
    when(mockBuilder.getIndexMappings(any(OperationContext.class), any()))
        .thenThrow(originalException);

    // Use reflection to replace the first builder with our mock
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder);
    } catch (Exception e) {
      fail("Failed to set up mock builder: " + e.getMessage());
    }

    try {
      builder.getIndexMappings(operationContext, null);
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the exception message contains the expected text
      assertTrue(
          e.getMessage().contains("Failed to get reference mappings"),
          "Exception message should contain 'Failed to get reference mappings'");

      // Verify the cause is the original exception
      assertEquals(
          e.getCause(),
          originalException,
          "Exception cause should be the original RuntimeException");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithIOExceptionFromBuilder() {
    // Test that getIndexMappings throws RuntimeException when first builder throws IOException
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(false);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with v2 enabled only
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Mock the first builder to throw IOException
    IOException ioException = new IOException("File not found");
    MappingsBuilder mockBuilder = mock(MappingsBuilder.class);
    when(mockBuilder.getIndexMappings(any(OperationContext.class), any())).thenThrow(ioException);

    // Use reflection to replace the first builder with our mock
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder);
    } catch (Exception e) {
      fail("Failed to set up mock builder: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping the IOException
    builder.getIndexMappings(operationContext, null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithIllegalArgumentExceptionFromBuilder() {
    // Test that getIndexMappings throws RuntimeException when first builder throws
    // IllegalArgumentException
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(false);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with v2 enabled only
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Mock the first builder to throw IllegalArgumentException
    IllegalArgumentException illegalArgException = new IllegalArgumentException("Invalid argument");
    MappingsBuilder mockBuilder = mock(MappingsBuilder.class);
    when(mockBuilder.getIndexMappings(any(OperationContext.class), any()))
        .thenThrow(illegalArgException);

    // Use reflection to replace the first builder with our mock
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder);
    } catch (Exception e) {
      fail("Failed to set up mock builder: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping the IllegalArgumentException
    builder.getIndexMappings(operationContext, null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithInconsistentMappings() {
    // Test that getIndexMappings throws RuntimeException wrapping IllegalStateException when
    // builders return inconsistent mappings
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders that return different mappings
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create different mappings for each builder
    IndexMapping mapping1 = createMockIndexMapping("index1", Map.of("field1", "type1"));
    IndexMapping mapping2 =
        createMockIndexMapping("index1", Map.of("field1", "type2")); // Different type

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping IllegalStateException due to inconsistent
    // mappings
    builder.getIndexMappings(operationContext, null);
  }

  @Test
  public void testGetIndexMappingsInconsistentMappingsExceptionMessage() {
    // Test that the RuntimeException wraps IllegalStateException with the expected information
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders that return different mappings
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create different mappings for each builder
    IndexMapping mapping1 = createMockIndexMapping("index1", Map.of("field1", "type1"));
    IndexMapping mapping2 =
        createMockIndexMapping("index1", Map.of("field1", "type2")); // Different type

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    try {
      builder.getIndexMappings(operationContext, null);
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the RuntimeException message
      assertTrue(
          e.getMessage().contains("Failed to validate mappings"),
          "Exception message should contain 'Failed to validate mappings'");

      // Verify the cause is IllegalStateException
      assertTrue(
          e.getCause() instanceof IllegalStateException,
          "Exception cause should be IllegalStateException");

      // Verify the IllegalStateException message contains the expected text
      IllegalStateException cause = (IllegalStateException) e.getCause();
      assertTrue(
          cause.getMessage().contains("Inconsistent mappings between"),
          "Cause message should contain 'Inconsistent mappings between'");
      assertTrue(
          cause.getMessage().contains("This indicates an illegal configuration state"),
          "Cause message should contain 'This indicates an illegal configuration state'");
    }
  }

  @Test
  public void testGetIndexMappingsWithDifferentIndexNames() {
    // Test that getIndexMappings works correctly when builders return mappings for different
    // indices
    // This should NOT throw an exception because mappingsEqual only compares common indices
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders that return mappings for different indices
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create mappings for different indices
    IndexMapping mapping1 = createMockIndexMapping("index1", Map.of("field1", "type1"));
    IndexMapping mapping2 =
        createMockIndexMapping("index2", Map.of("field1", "type1")); // Different index name

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    // This should NOT throw an exception because mappingsEqual only compares common indices
    // When indices are different, there are no common indices to compare, so they are considered
    // equal
    Collection<IndexMapping> result = builder.getIndexMappings(operationContext, null);

    // Verify that both mappings are returned (merged)
    assertEquals(
        2, result.size(), "Should return both mappings since they have different index names");

    // Verify that both index names are present
    Set<String> indexNames =
        result.stream().map(IndexMapping::getIndexName).collect(Collectors.toSet());
    assertTrue(indexNames.contains("index1"), "Should contain index1");
    assertTrue(indexNames.contains("index2"), "Should contain index2");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithDifferentFieldCounts() {
    // Test that getIndexMappings throws RuntimeException wrapping IllegalStateException when
    // builders return mappings with different field counts
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders that return mappings with different field counts
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create mappings with different field counts
    IndexMapping mapping1 = createMockIndexMapping("index1", Map.of("field1", "type1"));
    IndexMapping mapping2 =
        createMockIndexMapping(
            "index1", Map.of("field1", "type1", "field2", "type2")); // Different field count

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping IllegalStateException due to inconsistent
    // mappings
    builder.getIndexMappings(operationContext, null);
  }

  @Test
  public void testGetIndexMappingsWithEmptyBuildersList() {
    // Test that getIndexMappings returns empty list when builders list is empty
    // This tests the defensive check in getIndexMappings method
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(false);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both versions disabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Use reflection to clear the builders list to test the empty case
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear(); // Clear the builders list to test the empty case
    } catch (Exception e) {
      fail("Failed to clear builders list: " + e.getMessage());
    }

    // This should return an empty list due to the defensive check
    Collection<IndexMapping> result = builder.getIndexMappings(operationContext, null);

    assertNotNull(result, "Result should not be null");
    assertTrue(result.isEmpty(), "Result should be empty when builders list is empty");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithNewStructuredPropertyWithFailingFirstBuilder() {
    // Test that getIndexMappingsWithNewStructuredProperty throws RuntimeException when the first
    // builder fails
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders where the first one fails
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    when(mockBuilder1.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenThrow(new RuntimeException("Builder failed"));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    // This should throw RuntimeException
    builder.getIndexMappingsWithNewStructuredProperty(operationContext, null, null);
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredPropertyExceptionMessageAndCause() {
    // Test that getIndexMappingsWithNewStructuredProperty throws RuntimeException with proper
    // message and cause
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders where the first one fails
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    RuntimeException originalException = new RuntimeException("Builder initialization failed");
    when(mockBuilder1.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenThrow(originalException);

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    try {
      builder.getIndexMappingsWithNewStructuredProperty(operationContext, null, null);
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the RuntimeException message
      assertTrue(
          e.getMessage().contains("Failed to get reference mappings for new structured property"),
          "Exception message should contain 'Failed to get reference mappings for new structured property'");

      // Verify the cause is the original RuntimeException
      assertEquals(
          e.getCause(),
          originalException,
          "Exception cause should be the original RuntimeException");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithNewStructuredPropertyWithIOExceptionFromBuilder() {
    // Test that getIndexMappingsWithNewStructuredProperty throws RuntimeException when the first
    // builder throws IOException
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders where the first one throws IOException
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    when(mockBuilder1.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenThrow(new IOException("IO error"));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping IOException
    builder.getIndexMappingsWithNewStructuredProperty(operationContext, null, null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void
      testGetIndexMappingsWithNewStructuredPropertyWithIllegalArgumentExceptionFromBuilder() {
    // Test that getIndexMappingsWithNewStructuredProperty throws RuntimeException when the first
    // builder throws IllegalArgumentException
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders where the first one throws IllegalArgumentException
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    when(mockBuilder1.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenThrow(new IllegalArgumentException("Invalid argument"));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping IllegalArgumentException
    builder.getIndexMappingsWithNewStructuredProperty(operationContext, null, null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithNewStructuredPropertyWithInconsistentMappings() {
    // Test that getIndexMappingsWithNewStructuredProperty throws RuntimeException wrapping
    // IllegalStateException when builders return inconsistent mappings
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders that return inconsistent mappings
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create mappings with different field types for the same index
    IndexMapping mapping1 = createMockIndexMapping("test_index", Map.of("field1", "type1"));
    IndexMapping mapping2 =
        createMockIndexMapping("test_index", Map.of("field1", "type2")); // Different type

    when(mockBuilder1.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping IllegalStateException
    builder.getIndexMappingsWithNewStructuredProperty(operationContext, null, null);
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredPropertyInconsistentMappingsExceptionMessage() {
    // Test that getIndexMappingsWithNewStructuredProperty throws RuntimeException wrapping
    // IllegalStateException with proper message
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders that return inconsistent mappings
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create mappings with different field types for the same index
    IndexMapping mapping1 = createMockIndexMapping("test_index", Map.of("field1", "type1"));
    IndexMapping mapping2 =
        createMockIndexMapping("test_index", Map.of("field1", "type2")); // Different type

    when(mockBuilder1.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    try {
      builder.getIndexMappingsWithNewStructuredProperty(operationContext, null, null);
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the RuntimeException message
      assertTrue(
          e.getMessage().contains("Failed to validate mappings for new structured property"),
          "Exception message should contain 'Failed to validate mappings for new structured property'");

      // Verify the cause is IllegalStateException
      assertTrue(
          e.getCause() instanceof IllegalStateException,
          "Exception cause should be IllegalStateException");

      // Verify the IllegalStateException message contains the expected text
      IllegalStateException cause = (IllegalStateException) e.getCause();
      assertTrue(
          cause.getMessage().contains("Inconsistent mappings between"),
          "Cause message should contain 'Inconsistent mappings between'");
      assertTrue(
          cause.getMessage().contains("for new structured property"),
          "Cause message should contain 'for new structured property'");
      assertTrue(
          cause.getMessage().contains("This indicates an illegal configuration state"),
          "Cause message should contain 'This indicates an illegal configuration state'");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsWithNewStructuredPropertyWithDifferentFieldCounts() {
    // Test that getIndexMappingsWithNewStructuredProperty throws RuntimeException wrapping
    // IllegalStateException when builders return mappings with different field counts
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    // Create a DelegatingMappingsBuilder with both v2 and v3 enabled
    DelegatingMappingsBuilder builder = createDelegatingMappingsBuilder(config);

    // Create mock builders that return mappings with different field counts
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create mappings with different field counts for the same index
    IndexMapping mapping1 = createMockIndexMapping("test_index", Map.of("field1", "type1"));
    IndexMapping mapping2 =
        createMockIndexMapping(
            "test_index", Map.of("field1", "type1", "field2", "type2")); // Different field count

    when(mockBuilder1.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappingsWithNewStructuredProperty(
            any(OperationContext.class), any(), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Use reflection to replace the builders with our mocks
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      @SuppressWarnings("unchecked")
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      builders.add(mockBuilder1);
      builders.add(mockBuilder2);
    } catch (Exception e) {
      fail("Failed to set up mock builders: " + e.getMessage());
    }

    // This should throw RuntimeException wrapping IllegalStateException
    builder.getIndexMappingsWithNewStructuredProperty(operationContext, null, null);
  }

  // Helper method to create DelegatingMappingsBuilder with configuration
  private DelegatingMappingsBuilder createDelegatingMappingsBuilder(
      EntityIndexConfiguration config) {
    return io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder(config);
  }

  private IndexMapping createMockIndexMapping(String indexName, Map<String, Object> mappings) {
    IndexMapping mockMapping = mock(IndexMapping.class);
    when(mockMapping.getIndexName()).thenReturn(indexName);
    when(mockMapping.getMappings()).thenReturn(mappings);
    return mockMapping;
  }

  // Tests for getMappingsForStructuredProperty exception handling

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetMappingsForStructuredPropertyWithInconsistentIndexIndexMappings() {
    // Test that getMappingsForStructuredProperty throws RuntimeException when builders return
    // inconsistent mappings
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    // Setup mock builders to return different structured property mappings
    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    Map<String, Object> mappings2 = new HashMap<>();
    mappings2.put("property1", Map.of("type", "keyword")); // Different type

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings2);

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping IllegalStateException
    builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
  }

  @Test
  public void testGetMappingsForStructuredPropertyInconsistentIndexIndexMappingsExceptionMessage() {
    // Test that the RuntimeException wraps IllegalStateException with proper message
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    Map<String, Object> mappings2 = new HashMap<>();
    mappings2.put("property1", Map.of("type", "keyword")); // Different type

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings2);

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    try {
      builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the RuntimeException message
      assertEquals(
          e.getMessage(),
          "Failed to validate structured property mappings",
          "RuntimeException should have correct message");

      // Verify the cause is IllegalStateException
      assertTrue(
          e.getCause() instanceof IllegalStateException,
          "RuntimeException cause should be IllegalStateException");

      IllegalStateException cause = (IllegalStateException) e.getCause();
      assertTrue(
          cause.getMessage().contains("Inconsistent structured property mappings"),
          "IllegalStateException should mention inconsistent structured property mappings");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsForStructuredPropertyWithFailingBuilder() {
    // Test that getMappingsForStructuredProperty throws RuntimeException when a builder fails
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any()))
        .thenThrow(new RuntimeException("Builder failure"));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException
    builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
  }

  @Test
  public void testGetIndexMappingsForStructuredPropertyBuilderFailureExceptionMessage() {
    // Test that the RuntimeException has proper message when builder fails
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    RuntimeException originalException = new RuntimeException("Builder failure");
    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any())).thenThrow(originalException);

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    try {
      builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the RuntimeException message
      assertEquals(
          e.getMessage(),
          "Failed to validate structured property mappings",
          "RuntimeException should have correct message");

      // Verify the cause is the original exception
      assertEquals(
          e.getCause(),
          originalException,
          "RuntimeException cause should be the original exception");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsForStructuredPropertyWithIOExceptionFromBuilder() {
    // Test that RuntimeException is thrown when builder throws IOException
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any()))
        .thenThrow(new IOException("IO error"));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping IOException
    builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsForStructuredPropertyWithIllegalArgumentExceptionFromBuilder() {
    // Test that RuntimeException is thrown when builder throws IllegalArgumentException
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any()))
        .thenThrow(new IllegalArgumentException("Invalid argument"));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping IllegalArgumentException
    builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
  }

  @Test
  public void testGetIndexMappingsForStructuredPropertyWithDifferentPropertyCounts() {
    // Test that RuntimeException is thrown when builders return mappings with different property
    // counts
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));
    mappings1.put("property2", Map.of("type", "keyword"));

    Map<String, Object> mappings2 = new HashMap<>();
    mappings2.put("property1", Map.of("type", "text"));
    // Missing property2 - different count

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings2);

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    try {
      builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the RuntimeException wraps IllegalStateException
      assertTrue(
          e.getCause() instanceof IllegalStateException,
          "RuntimeException cause should be IllegalStateException");

      IllegalStateException cause = (IllegalStateException) e.getCause();
      assertTrue(
          cause.getMessage().contains("Inconsistent structured property mappings"),
          "IllegalStateException should mention inconsistent structured property mappings");
    }
  }

  @Test
  public void testGetIndexMappingsForStructuredPropertyWithEmptyBuildersList() {
    // Test that getMappingsForStructuredProperty returns empty map when builders list is empty
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    // Clear the builders list using reflection
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      java.util.List<?> builders = (java.util.List<?>) buildersField.get(builder);
      builders.clear();
    } catch (Exception e) {
      fail("Failed to clear builders list: " + e.getMessage());
    }

    Map<String, Object> result =
        builder.getIndexMappingsForStructuredProperty(Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    assertTrue(result.isEmpty(), "Result should be empty when builders list is empty");
  }

  // Additional edge case tests for the outer catch block

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsForStructuredPropertyWithNullPointerExceptionFromBuilder() {
    // Test that RuntimeException is thrown when builder throws NullPointerException
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any()))
        .thenThrow(new NullPointerException("Null pointer error"));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping NullPointerException
    builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsForStructuredPropertyWithClassCastExceptionFromBuilder() {
    // Test that RuntimeException is thrown when builder throws ClassCastException
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any()))
        .thenThrow(new ClassCastException("Class cast error"));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping ClassCastException
    builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
  }

  @Test(expectedExceptions = OutOfMemoryError.class)
  public void testGetIndexMappingsForStructuredPropertyWithOutOfMemoryErrorFromBuilder() {
    // Test that OutOfMemoryError is thrown when builder throws OutOfMemoryError
    // Note: OutOfMemoryError extends Error, not Exception, so it's not caught by catch(Exception e)
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any()))
        .thenThrow(new OutOfMemoryError("Out of memory error"));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw OutOfMemoryError directly (not wrapped in RuntimeException)
    builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
  }

  @Test
  public void testGetIndexMappingsForStructuredPropertyOuterCatchBlockExceptionMessage() {
    // Test that the outer catch block preserves exception details correctly
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    RuntimeException originalException = new RuntimeException("Original builder error");
    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any())).thenThrow(originalException);

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    try {
      builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
      fail("Expected RuntimeException to be thrown");
    } catch (RuntimeException e) {
      // Verify the outer catch block message
      assertEquals(
          e.getMessage(),
          "Failed to validate structured property mappings",
          "Outer catch block should have correct message");

      // Verify the cause is preserved
      assertEquals(
          e.getCause(), originalException, "Original exception should be preserved as cause");

      // Verify the cause message is preserved
      assertEquals(
          e.getCause().getMessage(),
          "Original builder error",
          "Original exception message should be preserved");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetIndexMappingsForStructuredPropertyWithSecurityExceptionFromBuilder() {
    // Test that RuntimeException is thrown when builder throws SecurityException
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("property1", Map.of("type", "text"));

    when(mockBuilder1.getIndexMappingsForStructuredProperty(any())).thenReturn(mappings1);
    when(mockBuilder2.getIndexMappingsForStructuredProperty(any()))
        .thenThrow(new SecurityException("Security error"));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping SecurityException
    builder.getIndexMappingsForStructuredProperty(Collections.emptyList());
  }

  // Tests for mappingsEqual method comparison logic (tested indirectly through public methods)

  @Test
  public void testMappingsEqualWithIdenticalMappingsForCommonIndices() {
    // Test that mappingsEqual returns true when mappings are identical for common indices
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create identical mappings for the same index
    Map<String, Object> identicalMappings = new HashMap<>();
    identicalMappings.put("field1", Map.of("type", "text", "analyzer", "standard"));
    identicalMappings.put("field2", Map.of("type", "keyword"));

    IndexMapping mapping1 = createMockIndexMapping("test_index", identicalMappings);
    IndexMapping mapping2 = createMockIndexMapping("test_index", identicalMappings);

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should NOT throw an exception because mappings are identical
    Collection<IndexMapping> result = builder.getIndexMappings(operationContext, null);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.size(), 1, "Should have 1 mapping");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMappingsEqualWithDifferentMappingsForSameIndexNames() {
    // Test that mappingsEqual returns false when mappings differ for same index names
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create different mappings for the same index name
    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("field1", Map.of("type", "text", "analyzer", "standard"));

    Map<String, Object> mappings2 = new HashMap<>();
    mappings2.put("field1", Map.of("type", "keyword")); // Different type

    IndexMapping mapping1 = createMockIndexMapping("test_index", mappings1);
    IndexMapping mapping2 = createMockIndexMapping("test_index", mappings2);

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping IllegalStateException
    builder.getIndexMappings(operationContext, null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMappingsEqualWithNullMappingsHandling() {
    // Test that mappingsEqual handles null mappings correctly
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create mappings where one has null values
    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("field1", Map.of("type", "text"));

    Map<String, Object> mappings2 = new HashMap<>();
    mappings2.put("field1", null); // Null mapping

    IndexMapping mapping1 = createMockIndexMapping("test_index", mappings1);
    IndexMapping mapping2 = createMockIndexMapping("test_index", mappings2);

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping IllegalStateException
    builder.getIndexMappings(operationContext, null);
  }

  @Test
  public void testMappingsEqualWithComplexNestedMappings() {
    // Test that mappingsEqual correctly compares complex nested mappings
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create complex nested mappings
    Map<String, Object> complexMappings = new HashMap<>();
    Map<String, Object> nestedObject = new HashMap<>();
    nestedObject.put("subfield1", Map.of("type", "text"));
    nestedObject.put("subfield2", Map.of("type", "keyword"));

    Map<String, Object> properties = new HashMap<>();
    properties.put("nested_field", nestedObject);
    properties.put("simple_field", Map.of("type", "text"));

    complexMappings.put("properties", properties);

    IndexMapping mapping1 = createMockIndexMapping("complex_index", complexMappings);
    IndexMapping mapping2 = createMockIndexMapping("complex_index", complexMappings);

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should NOT throw an exception because mappings are identical
    Collection<IndexMapping> result = builder.getIndexMappings(operationContext, null);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.size(), 1, "Should have 1 mapping");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMappingsEqualWithComplexNestedMappingsDifference() {
    // Test that mappingsEqual detects differences in complex nested mappings
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create complex nested mappings with subtle differences
    Map<String, Object> mappings1 = new HashMap<>();
    Map<String, Object> nestedObject1 = new HashMap<>();
    nestedObject1.put("subfield1", Map.of("type", "text", "analyzer", "standard"));

    Map<String, Object> properties1 = new HashMap<>();
    properties1.put("nested_field", nestedObject1);
    mappings1.put("properties", properties1);

    Map<String, Object> mappings2 = new HashMap<>();
    Map<String, Object> nestedObject2 = new HashMap<>();
    nestedObject2.put(
        "subfield1", Map.of("type", "text", "analyzer", "keyword")); // Different analyzer

    Map<String, Object> properties2 = new HashMap<>();
    properties2.put("nested_field", nestedObject2);
    mappings2.put("properties", properties2);

    IndexMapping mapping1 = createMockIndexMapping("complex_index", mappings1);
    IndexMapping mapping2 = createMockIndexMapping("complex_index", mappings2);

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping IllegalStateException
    builder.getIndexMappings(operationContext, null);
  }

  @Test
  public void testMappingsEqualWithEmptyMappings() {
    // Test that mappingsEqual handles empty mappings correctly
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create empty mappings
    Map<String, Object> emptyMappings = new HashMap<>();

    IndexMapping mapping1 = createMockIndexMapping("empty_index", emptyMappings);
    IndexMapping mapping2 = createMockIndexMapping("empty_index", emptyMappings);

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should NOT throw an exception because empty mappings are equal
    Collection<IndexMapping> result = builder.getIndexMappings(operationContext, null);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.size(), 1, "Should have 1 mapping");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMappingsEqualWithDifferentFieldCountsInSameIndex() {
    // Test that mappingsEqual detects different field counts in the same index
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create mappings with different field counts
    Map<String, Object> mappings1 = new HashMap<>();
    mappings1.put("field1", Map.of("type", "text"));
    mappings1.put("field2", Map.of("type", "keyword"));

    Map<String, Object> mappings2 = new HashMap<>();
    mappings2.put("field1", Map.of("type", "text"));
    // Missing field2

    IndexMapping mapping1 = createMockIndexMapping("test_index", mappings1);
    IndexMapping mapping2 = createMockIndexMapping("test_index", mappings2);

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(mapping2));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should throw RuntimeException wrapping IllegalStateException
    builder.getIndexMappings(operationContext, null);
  }

  @Test
  public void testMappingsEqualWithMultipleIndicesSomeCommon() {
    // Test that mappingsEqual only compares common indices and ignores non-common ones
    DelegatingMappingsBuilder builder = createBuilderWithMockMappingsBuilders();

    MappingsBuilder mockBuilder1 = mock(MappingsBuilder.class);
    MappingsBuilder mockBuilder2 = mock(MappingsBuilder.class);

    // Create mappings with some common indices and some different ones
    Map<String, Object> commonMappings = new HashMap<>();
    commonMappings.put("common_field", Map.of("type", "text"));

    Map<String, Object> differentMappings1 = new HashMap<>();
    differentMappings1.put("different_field1", Map.of("type", "keyword"));

    Map<String, Object> differentMappings2 = new HashMap<>();
    differentMappings2.put("different_field2", Map.of("type", "text"));

    IndexMapping commonMapping1 = createMockIndexMapping("common_index", commonMappings);
    IndexMapping commonMapping2 = createMockIndexMapping("common_index", commonMappings);
    IndexMapping differentMapping1 = createMockIndexMapping("different_index1", differentMappings1);
    IndexMapping differentMapping2 = createMockIndexMapping("different_index2", differentMappings2);

    when(mockBuilder1.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(commonMapping1));
    when(mockBuilder2.getIndexMappings(any(OperationContext.class), any()))
        .thenReturn(Collections.singletonList(commonMapping2));

    // Inject mock builders using reflection
    injectMockBuilders(builder, mockBuilder1, mockBuilder2);

    // This should NOT throw an exception because only common indices are compared
    Collection<IndexMapping> result = builder.getIndexMappings(operationContext, null);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.size(), 1, "Should have 1 mapping (only common index)");
  }

  // Helper methods for testing getMappingsForStructuredProperty

  private DelegatingMappingsBuilder createBuilderWithMockMappingsBuilders() {
    // Create a builder with a configuration that enables both v2 and v3
    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn(null); // Use default config
    when(config.getV2()).thenReturn(v2Config);
    when(config.getV3()).thenReturn(v3Config);

    return createDelegatingMappingsBuilder(config);
  }

  private void injectMockBuilders(
      DelegatingMappingsBuilder builder, MappingsBuilder... mockBuilders) {
    try {
      java.lang.reflect.Field buildersField =
          DelegatingMappingsBuilder.class.getDeclaredField("builders");
      buildersField.setAccessible(true);
      java.util.List<MappingsBuilder> builders =
          (java.util.List<MappingsBuilder>) buildersField.get(builder);
      builders.clear();
      for (MappingsBuilder mockBuilder : mockBuilders) {
        builders.add(mockBuilder);
      }
    } catch (Exception e) {
      fail("Failed to inject mock builders: " + e.getMessage());
    }
  }
}
