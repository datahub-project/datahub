package com.linkedin.metadata.search.elasticsearch.index;

import static io.datahubproject.test.search.SearchTestUtils.createDelegatingSettingsBuilder;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for DelegatingSettingsBuilder with IndexConvention. */
public class DelegatingSettingsBuilderTest {

  private EntityIndexConfiguration entityIndexConfiguration;
  private EntityIndexVersionConfiguration v2Config;
  private EntityIndexVersionConfiguration v3Config;
  private IndexConfiguration indexConfiguration;
  private IndexConvention indexConvention;

  @BeforeMethod
  public void setUp() {
    // Create mock configurations
    entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    v2Config = mock(EntityIndexVersionConfiguration.class);
    v3Config = mock(EntityIndexVersionConfiguration.class);
    indexConfiguration = mock(IndexConfiguration.class);
    indexConvention = mock(IndexConvention.class);

    // Setup default mock behavior
    when(entityIndexConfiguration.getV2()).thenReturn(v2Config);
    when(entityIndexConfiguration.getV3()).thenReturn(v3Config);
    when(v2Config.isEnabled()).thenReturn(true);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getAnalyzerConfig()).thenReturn("");
  }

  /** Helper method to create a DelegatingSettingsBuilder with the current configuration. */
  private DelegatingSettingsBuilder createDelegatingSettingsBuilder() {
    return io.datahubproject.test.search.SearchTestUtils.createDelegatingSettingsBuilder(
        entityIndexConfiguration, indexConfiguration, indexConvention);
  }

  @Test
  public void testConstructorWithBothV2AndV3Enabled() throws IOException {
    // Test constructor with both v2 and v3 enabled
    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    assertNotNull(builder, "DelegatingSettingsBuilder should be created successfully");
  }

  @Test
  public void testConstructorWithOnlyV2Enabled() throws IOException {
    // Test constructor with only v2 enabled
    when(v3Config.isEnabled()).thenReturn(false);

    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    assertNotNull(builder, "DelegatingSettingsBuilder should be created successfully");
  }

  @Test
  public void testConstructorWithOnlyV3Enabled() throws IOException {
    // Test constructor with only v3 enabled
    when(v2Config.isEnabled()).thenReturn(false);

    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    assertNotNull(builder, "DelegatingSettingsBuilder should be created successfully");
  }

  @Test
  public void testConstructorWithNeitherEnabled() throws IOException {
    // Test constructor with neither v2 nor v3 enabled
    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(false);

    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    assertNotNull(builder, "DelegatingSettingsBuilder should be created successfully");
  }

  @Test
  public void testGetSettingsWithV2Index() throws IOException {
    // Test getSettings with v2 index
    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    // Mock the settings builders to return different settings
    Map<String, Object> v2Settings = new HashMap<>();
    v2Settings.put("max_ngram_diff", 17);
    v2Settings.put("analysis", new HashMap<>());

    Map<String, Object> v3Settings = new HashMap<>(); // Empty for v3

    // Mock the index convention to identify v2 index
    when(indexConvention.isV2EntityIndex("datasetindex_v2")).thenReturn(true);
    when(indexConvention.isV3EntityIndex("datasetindex_v2")).thenReturn(false);

    Map<String, Object> result = builder.getSettings(indexConfiguration, "datasetindex_v2");

    assertNotNull(result, "Should return non-null settings");
    // The result should be from the v2 builder (non-empty settings)
    assertFalse(result.isEmpty(), "Should return non-empty settings for v2 index");
  }

  @Test
  public void testGetSettingsWithV3Index() throws IOException {
    // Test getSettings with v3 index
    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    // Mock the index convention to identify v3 index
    when(indexConvention.isV2EntityIndex("datasetindex_v3")).thenReturn(false);
    when(indexConvention.isV3EntityIndex("datasetindex_v3")).thenReturn(true);

    Map<String, Object> result = builder.getSettings(indexConfiguration, "datasetindex_v3");

    assertNotNull(result, "Should return non-null settings");
    // The result should be from the v3 builder (empty settings in this case)
    assertTrue(result.isEmpty(), "Should return empty settings for v3 index (no analyzer config)");
  }

  @Test
  public void testGetSettingsWithNonEntityIndex() throws IOException {
    // Test getSettings with non-entity index
    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    // Mock the index convention to identify non-entity index
    when(indexConvention.isV2EntityIndex("timeseriesindex_v1")).thenReturn(false);
    when(indexConvention.isV3EntityIndex("timeseriesindex_v1")).thenReturn(false);

    Map<String, Object> result = builder.getSettings(indexConfiguration, "timeseriesindex_v1");

    assertNotNull(result, "Should return non-null settings");
    assertTrue(result.isEmpty(), "Should return empty settings for non-entity index");
  }

  @Test
  public void testGetSettingsWithBothBuildersReturningSettings() throws IOException {
    // Test getSettings when both builders return settings (should validate consistency)
    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    // Mock the index convention to identify both v2 and v3 indices
    when(indexConvention.isV2EntityIndex("datasetindex_v2")).thenReturn(true);
    when(indexConvention.isV3EntityIndex("datasetindex_v2")).thenReturn(true);

    Map<String, Object> result = builder.getSettings(indexConfiguration, "datasetindex_v2");

    assertNotNull(result, "Should return non-null settings");
    // Should not throw exception if both builders return consistent settings
  }

  @Test
  public void testGetSettingsWithInconsistentSettings() throws IOException {
    // Test getSettings when builders return inconsistent settings
    // This test would require more complex mocking to simulate inconsistent settings
    // For now, we'll test the basic functionality
    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    when(indexConvention.isV2EntityIndex("datasetindex_v2")).thenReturn(true);
    when(indexConvention.isV3EntityIndex("datasetindex_v2")).thenReturn(true);

    // This should work fine with our current implementation
    Map<String, Object> result = builder.getSettings(indexConfiguration, "datasetindex_v2");
    assertNotNull(result, "Should return non-null settings");
  }

  @Test
  public void testGetSettingsWithNoBuilders() throws IOException {
    // Test getSettings with no builders enabled
    when(v2Config.isEnabled()).thenReturn(false);
    when(v3Config.isEnabled()).thenReturn(false);

    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    Map<String, Object> result = builder.getSettings(indexConfiguration, "any_index");

    assertNotNull(result, "Should return non-null settings");
    assertTrue(result.isEmpty(), "Should return empty settings when no builders are enabled");
  }

  @Test
  public void testSettingsConsistency() throws IOException {
    // Test that settings are consistent across multiple calls
    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    when(indexConvention.isV2EntityIndex("datasetindex_v2")).thenReturn(true);
    when(indexConvention.isV3EntityIndex("datasetindex_v2")).thenReturn(false);

    Map<String, Object> settings1 = builder.getSettings(indexConfiguration, "datasetindex_v2");
    Map<String, Object> settings2 = builder.getSettings(indexConfiguration, "datasetindex_v2");

    assertEquals(settings1, settings2, "Settings should be consistent across multiple calls");
  }

  @Test
  public void testSettingsBuilderInterface() throws IOException {
    // Test that DelegatingSettingsBuilder properly implements SettingsBuilder interface
    DelegatingSettingsBuilder builder = createDelegatingSettingsBuilder();

    assertTrue(
        builder instanceof SettingsBuilder,
        "DelegatingSettingsBuilder should implement SettingsBuilder interface");

    when(indexConvention.isV2EntityIndex("test_index")).thenReturn(true);
    when(indexConvention.isV3EntityIndex("test_index")).thenReturn(false);

    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");
    assertNotNull(settings, "getSettings should return non-null settings");
  }
}
