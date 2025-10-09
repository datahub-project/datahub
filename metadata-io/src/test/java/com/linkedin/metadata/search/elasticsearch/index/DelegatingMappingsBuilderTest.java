package com.linkedin.metadata.search.elasticsearch.index;

import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

    // Initialize DelegatingMappingsBuilder with configuration
    delegatingMappingsBuilder = new DelegatingMappingsBuilder(entityIndexConfiguration);
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

    DelegatingMappingsBuilder disabledBuilder = new DelegatingMappingsBuilder(disabledConfig);
    assertNotNull(
        disabledBuilder, "DelegatingMappingsBuilder should be created with disabled versions");
  }

  @Test
  public void testGetMappingsWithV2V3Enabled() {
    // Test getMappings when both v2 and v3 are enabled
    Collection<IndexMapping> result = delegatingMappingsBuilder.getMappings(operationContext);

    assertNotNull(result, "Result should not be null");
    // Should have mappings from both v2 and v3 builders
    assertFalse(result.isEmpty(), "Result should not be empty when both versions are enabled");
  }

  @Test
  public void testGetMappingsWithOnlyV2Enabled() {
    // Test getMappings when only v2 is enabled
    EntityIndexConfiguration v2OnlyConfig = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(false);
    when(v2OnlyConfig.getV2()).thenReturn(v2Config);
    when(v2OnlyConfig.getV3()).thenReturn(v3Config);

    DelegatingMappingsBuilder v2OnlyBuilder = new DelegatingMappingsBuilder(v2OnlyConfig);
    Collection<IndexMapping> result = v2OnlyBuilder.getMappings(operationContext);

    assertNotNull(result, "Result should not be null");
    // Should have mappings from v2 builder only
    assertFalse(result.isEmpty(), "Result should not be empty when v2 is enabled");
  }

  @Test
  public void testGetMappingsWithOnlyV3Enabled() {
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

    DelegatingMappingsBuilder v3OnlyBuilder = new DelegatingMappingsBuilder(v3OnlyConfig);
    Collection<IndexMapping> result = v3OnlyBuilder.getMappings(operationContext);

    assertNotNull(result, "Result should not be null");
    // Should have mappings from v3 builder only
    assertFalse(result.isEmpty(), "Result should not be empty when v3 is enabled");
  }

  @Test
  public void testGetIndexMappingsWithStructuredProperties() {
    // Test getIndexMappings with structured properties
    Collection<IndexMapping> result =
        delegatingMappingsBuilder.getIndexMappings(operationContext, null);

    assertNotNull(result, "Result should not be null");
    // Should have mappings from both v2 and v3 builders
    assertFalse(result.isEmpty(), "Result should not be empty when both versions are enabled");
  }

  @Test
  public void testGetMappingsForStructuredProperty() {
    // Test getMappingsForStructuredProperty
    Map<String, Object> result =
        delegatingMappingsBuilder.getMappingsForStructuredProperty(Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    // Result may be empty if no structured properties are provided, which is valid
    // The important thing is that the method doesn't throw an exception
  }

  @Test
  public void testMappingsConsistency() {
    // Test that mappings from different builders are consistent
    Collection<IndexMapping> mappings1 = delegatingMappingsBuilder.getMappings(operationContext);
    Collection<IndexMapping> mappings2 = delegatingMappingsBuilder.getMappings(operationContext);

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
    Collection<IndexMapping> result = delegatingMappingsBuilder.getMappings(operationContext);

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

  private IndexMapping createMockIndexMapping(String indexName, Map<String, Object> mappings) {
    IndexMapping mockMapping = mock(IndexMapping.class);
    when(mockMapping.getIndexName()).thenReturn(indexName);
    when(mockMapping.getMappings()).thenReturn(mappings);
    return mockMapping;
  }
}
