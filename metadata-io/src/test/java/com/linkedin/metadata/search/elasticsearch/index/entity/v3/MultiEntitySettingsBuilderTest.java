package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for MultiEntitySettingsBuilder with EntityIndexConfiguration. */
public class MultiEntitySettingsBuilderTest {

  private EntityIndexConfiguration entityIndexConfiguration;
  private EntityIndexVersionConfiguration v3Config;

  @BeforeMethod
  public void setUp() {
    // Create mock EntityIndexConfiguration
    entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    v3Config = mock(EntityIndexVersionConfiguration.class);
    when(entityIndexConfiguration.getV3()).thenReturn(v3Config);
  }

  @Test
  public void testConstructorWithEmptyAnalyzerConfig() throws IOException {
    // Test constructor with empty analyzer configuration
    when(v3Config.getAnalyzerConfig()).thenReturn("");
    IndexConvention indexConvention = mock(IndexConvention.class);

    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);

    assertNotNull(builder, "MultiEntitySettingsBuilder should be created successfully");
    assertNotNull(builder.settings, "Settings should be initialized");
    assertTrue(builder.settings.isEmpty(), "Settings should be empty when no analyzer config");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstructorWithNullAnalyzerConfig() throws IOException {
    // Test constructor with null analyzer configuration - should throw NullPointerException
    when(v3Config.getAnalyzerConfig()).thenReturn(null);

    IndexConvention indexConvention = mock(IndexConvention.class);
    new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
  }

  @Test
  public void testConstructorWithWhitespaceAnalyzerConfig() throws IOException {
    // Test constructor with whitespace-only analyzer configuration
    when(v3Config.getAnalyzerConfig()).thenReturn("   ");

    IndexConvention indexConvention = mock(IndexConvention.class);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);

    assertNotNull(builder, "MultiEntitySettingsBuilder should be created successfully");
    assertNotNull(builder.settings, "Settings should be initialized");
    assertTrue(
        builder.settings.isEmpty(), "Settings should be empty when analyzer config is whitespace");
  }

  @Test
  public void testConstructorWithDefaultAnalyzerConfig() throws IOException {
    // Test constructor with default analyzer configuration
    when(v3Config.getAnalyzerConfig()).thenReturn("search_entity_analyzer_config.yaml");

    IndexConvention indexConvention = mock(IndexConvention.class);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);

    assertNotNull(builder, "MultiEntitySettingsBuilder should be created successfully");
    assertNotNull(builder.settings, "Settings should be initialized");
    assertTrue(builder.settings.containsKey("analysis"), "Should contain analysis configuration");
  }

  @Test(expectedExceptions = IOException.class)
  public void testConstructorWithInvalidAnalyzerConfig() throws IOException {
    // Test constructor with invalid analyzer configuration path
    when(v3Config.getAnalyzerConfig()).thenReturn("non_existent_file.yaml");

    IndexConvention indexConvention = mock(IndexConvention.class);
    new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
  }

  @Test
  public void testConstructorWithNullEntityIndexConfiguration() {
    // Test constructor with null EntityIndexConfiguration
    try {
      IndexConvention indexConvention = mock(IndexConvention.class);
      new MultiEntitySettingsBuilder(null, indexConvention);
      fail("Constructor should not accept null EntityIndexConfiguration");
    } catch (Exception e) {
      // Expected behavior - should throw exception for null configuration
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null configuration");
    }
  }

  @Test
  public void testGetSettingsWithValidConfiguration() throws IOException {
    // Test getSettings with valid configuration
    when(v3Config.getAnalyzerConfig()).thenReturn("");

    IndexConvention indexConvention = mock(IndexConvention.class);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    assertNotNull(settings, "Settings should not be null");
    assertTrue(settings.isEmpty(), "Settings should be empty when no analyzer config");
  }

  @Test
  public void testGetSettingsWithDefaultAnalyzerConfiguration() throws IOException {
    // Test getSettings with default analyzer configuration
    when(v3Config.getAnalyzerConfig()).thenReturn("search_entity_analyzer_config.yaml");

    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV3EntityIndex("test_index")).thenReturn(true);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    assertNotNull(settings, "Settings should not be null");
    assertTrue(settings.containsKey("analysis"), "Should contain analysis configuration");

    @SuppressWarnings("unchecked")
    Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");

    assertNotNull(analysis.get("analyzer"), "Should contain analyzer section");
    assertNotNull(analysis.get("normalizer"), "Should contain normalizer section");
    assertNotNull(analysis.get("filter"), "Should contain filter section");
    assertNotNull(analysis.get("tokenizer"), "Should contain tokenizer section");
  }

  @Test
  public void testGetSettingsConsistency() throws IOException {
    // Test that getSettings returns consistent results
    when(v3Config.getAnalyzerConfig()).thenReturn("");

    IndexConvention indexConvention = mock(IndexConvention.class);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    Map<String, Object> settings1 = builder.getSettings(indexConfiguration, "test_index");
    Map<String, Object> settings2 = builder.getSettings(indexConfiguration, "test_index");

    assertEquals(settings1, settings2, "Settings should be consistent across multiple calls");
  }

  @Test
  public void testGetSettingsWithDifferentIndexNames() throws IOException {
    // Test that getSettings returns the same settings regardless of index name
    when(v3Config.getAnalyzerConfig()).thenReturn("");

    IndexConvention indexConvention = mock(IndexConvention.class);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    Map<String, Object> settings1 = builder.getSettings(indexConfiguration, "index1");
    Map<String, Object> settings2 = builder.getSettings(indexConfiguration, "index2");

    assertEquals(settings1, settings2, "Settings should be the same for different index names");
  }

  @Test
  public void testGetSettingsWithNullIndexConfiguration() throws IOException {
    // Test getSettings with null IndexConfiguration - method ignores the parameter
    when(v3Config.getAnalyzerConfig()).thenReturn("");

    IndexConvention indexConvention = mock(IndexConvention.class);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);

    // The method doesn't validate the IndexConfiguration parameter, it just ignores it
    Map<String, Object> settings = builder.getSettings(null, "test_index");
    assertNotNull(settings, "Settings should be returned even with null IndexConfiguration");
  }

  @Test
  public void testGetSettingsWithNullIndexName() throws IOException {
    // Test getSettings with null index name - method ignores the parameter
    when(v3Config.getAnalyzerConfig()).thenReturn("");

    IndexConvention indexConvention = mock(IndexConvention.class);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    // The method doesn't validate the index name parameter, it just ignores it
    Map<String, Object> settings = builder.getSettings(indexConfiguration, null);
    assertNotNull(settings, "Settings should be returned even with null index name");
  }

  @Test
  public void testDefaultAnalyzerConfigurationStructure() throws IOException {
    // Test that default analyzer configuration has the expected structure
    when(v3Config.getAnalyzerConfig()).thenReturn("search_entity_analyzer_config.yaml");

    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV3EntityIndex("test_index")).thenReturn(true);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    @SuppressWarnings("unchecked")
    Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");

    // Check analyzers
    @SuppressWarnings("unchecked")
    Map<String, Object> analyzers = (Map<String, Object>) analysis.get("analyzer");

    assertNotNull(analyzers.get("full"), "full analyzer should be present");
    assertNotNull(analyzers.get("full_removed_sep"), "full_removed_sep analyzer should be present");
    assertNotNull(analyzers.get("full_stemmer"), "full_stemmer analyzer should be present");
    assertNotNull(
        analyzers.get("browse_path_v2_hierarchy"),
        "browse_path_v2_hierarchy analyzer should be present");

    // Check normalizers
    @SuppressWarnings("unchecked")
    Map<String, Object> normalizers = (Map<String, Object>) analysis.get("normalizer");

    assertNotNull(normalizers.get("keyword_normalizer"), "keyword_normalizer should be present");

    // Check filters
    @SuppressWarnings("unchecked")
    Map<String, Object> filters = (Map<String, Object>) analysis.get("filter");

    assertNotNull(filters.get("stemmer_en"), "stemmer_en filter should be present");
    assertNotNull(filters.get("word_separator_filter"), "word_separator_filter should be present");
    assertNotNull(filters.get("synonyms"), "synonyms filter should be present");

    // Check tokenizers
    @SuppressWarnings("unchecked")
    Map<String, Object> tokenizers = (Map<String, Object>) analysis.get("tokenizer");

    assertNotNull(
        tokenizers.get("alphanumeric_tokenizer"), "alphanumeric_tokenizer should be present");
    assertNotNull(
        tokenizers.get("alphanumeric_tokenizer_full"),
        "alphanumeric_tokenizer_full should be present");
    assertNotNull(
        tokenizers.get("unit_separator_path_tokenizer"),
        "unit_separator_path_tokenizer should be present");
  }

  @Test
  public void testWithRealDefaultConfiguration() throws IOException {
    // Test with actual default configuration (not mocked)
    EntityIndexVersionConfiguration realV3Config = new EntityIndexVersionConfiguration();
    realV3Config.setEnabled(true);
    realV3Config.setAnalyzerConfig("search_entity_analyzer_config.yaml");
    realV3Config.setMappingConfig("search_entity_mapping_config.yaml");

    EntityIndexConfiguration realEntityIndexConfiguration = new EntityIndexConfiguration();
    realEntityIndexConfiguration.setV3(realV3Config);

    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV3EntityIndex("test_index")).thenReturn(true);
    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(realEntityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    assertNotNull(settings, "Settings should not be null");
    assertTrue(
        settings.containsKey("analysis"),
        "Should contain analysis configuration from default config");

    @SuppressWarnings("unchecked")
    Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");

    // Verify the default configuration structure
    assertNotNull(analysis.get("analyzer"), "Should contain analyzer section");
    assertNotNull(analysis.get("normalizer"), "Should contain normalizer section");
    assertNotNull(analysis.get("filter"), "Should contain filter section");
    assertNotNull(analysis.get("tokenizer"), "Should contain tokenizer section");

    // Check specific analyzers from default config
    @SuppressWarnings("unchecked")
    Map<String, Object> analyzers = (Map<String, Object>) analysis.get("analyzer");
    assertNotNull(analyzers.get("full"), "Default config should contain 'full' analyzer");
    assertNotNull(
        analyzers.get("full_removed_sep"),
        "Default config should contain 'full_removed_sep' analyzer");
  }

  @Test
  public void testSettingsBuilderInterface() throws IOException {
    // Test that MultiEntitySettingsBuilder properly implements SettingsBuilder interface
    when(v3Config.getAnalyzerConfig()).thenReturn("");
    IndexConvention indexConvention = mock(IndexConvention.class);

    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    // Test that it implements the interface correctly
    assertTrue(
        builder instanceof com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder,
        "MultiEntitySettingsBuilder should implement SettingsBuilder interface");

    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");
    assertNotNull(settings, "getSettings should return non-null settings");
  }

  @Test
  public void testIndexNameValidation() throws IOException {
    // Test that the MultiEntitySettingsBuilder validates index names
    when(v3Config.getAnalyzerConfig()).thenReturn("");
    IndexConvention indexConvention = mock(IndexConvention.class);

    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    // Test v3 entity index - should return settings
    when(indexConvention.isV3EntityIndex("datasetindex_v3")).thenReturn(true);
    Map<String, Object> v3Settings = builder.getSettings(indexConfiguration, "datasetindex_v3");
    assertTrue(
        v3Settings.isEmpty(),
        "Should return empty settings for v3 entity index (no analyzer config)");

    // Test v2 entity index - should return empty settings
    when(indexConvention.isV3EntityIndex("datasetindex_v2")).thenReturn(false);
    Map<String, Object> v2Settings = builder.getSettings(indexConfiguration, "datasetindex_v2");
    assertTrue(v2Settings.isEmpty(), "Should return empty settings for v2 entity index");

    // Test non-entity index - should return empty settings
    when(indexConvention.isV3EntityIndex("timeseriesindex_v1")).thenReturn(false);
    Map<String, Object> timeseriesSettings =
        builder.getSettings(indexConfiguration, "timeseriesindex_v1");
    assertTrue(timeseriesSettings.isEmpty(), "Should return empty settings for non-entity index");

    // Verify that isV3EntityIndex was called for each index
    verify(indexConvention).isV3EntityIndex("datasetindex_v3");
    verify(indexConvention).isV3EntityIndex("datasetindex_v2");
    verify(indexConvention).isV3EntityIndex("timeseriesindex_v1");
  }

  @Test
  public void testIndexNameValidationWithAnalyzerConfig() throws IOException {
    // Test index name validation with analyzer configuration
    when(v3Config.getAnalyzerConfig()).thenReturn("search_entity_analyzer_config.yaml");
    IndexConvention indexConvention = mock(IndexConvention.class);

    MultiEntitySettingsBuilder builder =
        new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention);
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();

    // Test v3 entity index - should return settings with analysis
    when(indexConvention.isV3EntityIndex("datasetindex_v3")).thenReturn(true);
    Map<String, Object> v3Settings = builder.getSettings(indexConfiguration, "datasetindex_v3");
    assertTrue(
        v3Settings.containsKey("analysis"),
        "Should return settings with analysis for v3 entity index");

    // Test v2 entity index - should return empty settings
    when(indexConvention.isV3EntityIndex("datasetindex_v2")).thenReturn(false);
    Map<String, Object> v2Settings = builder.getSettings(indexConfiguration, "datasetindex_v2");
    assertTrue(v2Settings.isEmpty(), "Should return empty settings for v2 entity index");

    // Verify that isV3EntityIndex was called for each index
    verify(indexConvention).isV3EntityIndex("datasetindex_v3");
    verify(indexConvention).isV3EntityIndex("datasetindex_v2");
  }
}
