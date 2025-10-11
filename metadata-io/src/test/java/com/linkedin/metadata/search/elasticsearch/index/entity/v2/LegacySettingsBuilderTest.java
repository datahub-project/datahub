package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.util.Map;
import org.testng.annotations.Test;

/** Tests for LegacySettingsBuilder with IndexConfiguration. */
public class LegacySettingsBuilderTest {

  @Test
  public void testConstructorWithIndexConvention() {
    // Test that the LegacySettingsBuilder works with IndexConvention
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3); // Set required default value
    IndexConvention indexConvention = mock(IndexConvention.class);

    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);

    // Test v2 entity index - should return settings
    when(indexConvention.isV2EntityIndex("datasetindex_v2")).thenReturn(true);
    Map<String, Object> settings = builder.getSettings(indexConfiguration, "datasetindex_v2");

    // Should have basic settings
    assertNotNull(settings, "Settings should be built");
    assertNotNull(settings.get("max_ngram_diff"), "max_ngram_diff should be present");
    assertEquals(settings.get("max_ngram_diff"), 17);

    // Should have analysis configuration
    assertNotNull(settings.get("analysis"), "Analysis configuration should be present");

    @SuppressWarnings("unchecked")
    Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");

    assertNotNull(analysis.get("analyzer"), "Analyzers should be present");
    assertNotNull(analysis.get("filter"), "Filters should be present");
    assertNotNull(analysis.get("tokenizer"), "Tokenizers should be present");
    assertNotNull(analysis.get("normalizer"), "Normalizers should be present");
  }

  @Test
  public void testIndexNameValidation() {
    // Test that the LegacySettingsBuilder validates index names
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3);
    IndexConvention indexConvention = mock(IndexConvention.class);

    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);

    // Test v2 entity index - should return settings
    when(indexConvention.isV2EntityIndex("datasetindex_v2")).thenReturn(true);
    Map<String, Object> v2Settings = builder.getSettings(indexConfiguration, "datasetindex_v2");
    assertFalse(v2Settings.isEmpty(), "Should return settings for v2 entity index");

    // Test v3 entity index - should return empty settings
    when(indexConvention.isV2EntityIndex("datasetindex_v3")).thenReturn(false);
    Map<String, Object> v3Settings = builder.getSettings(indexConfiguration, "datasetindex_v3");
    assertTrue(v3Settings.isEmpty(), "Should return empty settings for v3 entity index");

    // Test non-entity index - should return empty settings
    when(indexConvention.isV2EntityIndex("timeseriesindex_v1")).thenReturn(false);
    Map<String, Object> timeseriesSettings =
        builder.getSettings(indexConfiguration, "timeseriesindex_v1");
    assertTrue(timeseriesSettings.isEmpty(), "Should return empty settings for non-entity index");

    // Verify that isV2EntityIndex was called for each index
    verify(indexConvention).isV2EntityIndex("datasetindex_v2");
    verify(indexConvention).isV2EntityIndex("datasetindex_v3");
    verify(indexConvention).isV2EntityIndex("timeseriesindex_v1");
  }

  @Test
  public void testConstructorWithNullIndexConvention() {
    // Test that the LegacySettingsBuilder works with null IndexConvention (legacy behavior)
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3);

    // This should not compile with the new constructor, but let's test the old behavior
    // by creating a mock that always returns true
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndex(anyString())).thenReturn(true);

    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);
    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    // Should have basic settings
    assertNotNull(settings, "Settings should be built");
    assertNotNull(settings.get("max_ngram_diff"), "max_ngram_diff should be present");
    assertEquals(settings.get("max_ngram_diff"), 17);
  }

  @Test
  public void testConstructorWithCustomMainTokenizer() {
    // Test with custom main tokenizer configuration
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3); // Set required default value
    indexConfiguration.setMainTokenizer("custom_main_tokenizer");

    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndex(anyString())).thenReturn(true);
    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);
    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    // Should have analysis configuration
    assertNotNull(settings.get("analysis"), "Analysis configuration should be present");

    @SuppressWarnings("unchecked")
    Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");

    assertNotNull(analysis.get("analyzer"), "Analyzers should be present");

    @SuppressWarnings("unchecked")
    Map<String, Object> analyzers = (Map<String, Object>) analysis.get("analyzer");

    // Check that the main tokenizer is used in analyzers
    assertTrue(
        analyzers.containsKey("word_delimited"), "word_delimited analyzer should be present");
    assertTrue(
        analyzers.containsKey("query_word_delimited"),
        "query_word_delimited analyzer should be present");
  }

  @Test
  public void testAnalysisConfigurationStructure() {
    // Test that the analysis configuration has the expected structure
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3); // Set required default value
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndex(anyString())).thenReturn(true);
    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);
    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    @SuppressWarnings("unchecked")
    Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");

    // Check analyzers
    @SuppressWarnings("unchecked")
    Map<String, Object> analyzers = (Map<String, Object>) analysis.get("analyzer");

    assertNotNull(analyzers.get("word_delimited"), "word_delimited analyzer should be present");
    assertNotNull(
        analyzers.get("query_word_delimited"), "query_word_delimited analyzer should be present");
    assertNotNull(
        analyzers.get("browse_path_hierarchy"), "browse_path_hierarchy analyzer should be present");
    assertNotNull(
        analyzers.get("browse_path_v2_hierarchy"),
        "browse_path_v2_hierarchy analyzer should be present");
    assertNotNull(analyzers.get("custom_keyword"), "custom_keyword analyzer should be present");
    assertNotNull(analyzers.get("partial"), "partial analyzer should be present");
    assertNotNull(analyzers.get("slash_pattern"), "slash_pattern analyzer should be present");
    assertNotNull(
        analyzers.get("unit_separator_pattern"),
        "unit_separator_pattern analyzer should be present");
    assertNotNull(analyzers.get("urn_component"), "urn_component analyzer should be present");
    assertNotNull(
        analyzers.get("query_urn_component"), "query_urn_component analyzer should be present");
    assertNotNull(analyzers.get("word_gram_2"), "word_gram_2 analyzer should be present");
    assertNotNull(analyzers.get("word_gram_3"), "word_gram_3 analyzer should be present");
    assertNotNull(analyzers.get("word_gram_4"), "word_gram_4 analyzer should be present");

    // Check filters
    @SuppressWarnings("unchecked")
    Map<String, Object> filters = (Map<String, Object>) analysis.get("filter");

    assertNotNull(filters.get("alpha_num_space"), "alpha_num_space filter should be present");
    assertNotNull(filters.get("remove_quotes"), "remove_quotes filter should be present");
    // Note: asciifolding is used in filter lists but not as a standalone filter
    assertNotNull(
        filters.get("autocomplete_custom_delimiter"),
        "autocomplete_custom_delimiter filter should be present");
    assertNotNull(
        filters.get("sticky_delimiter_graph"), "sticky_delimiter_graph filter should be present");
    assertNotNull(filters.get("default_syn_graph"), "default_syn_graph filter should be present");
    // Note: flatten_graph, lowercase, partial_urn_component, shingle, snowball, stop, unique,
    // word_delimiter, word_delimiter_graph, trim are used in filter lists but not as standalone
    // filters
    assertNotNull(filters.get("min_length"), "min_length filter should be present");
    assertNotNull(filters.get("multifilter"), "multifilter should be present");
    assertNotNull(filters.get("multifilter_graph"), "multifilter_graph should be present");
    assertNotNull(filters.get("word_gram_2_filter"), "word_gram_2_filter should be present");
    assertNotNull(filters.get("word_gram_3_filter"), "word_gram_3_filter should be present");
    assertNotNull(filters.get("word_gram_4_filter"), "word_gram_4_filter should be present");
    assertNotNull(filters.get("stem_override"), "stem_override filter should be present");
    assertNotNull(filters.get("datahub_stop_words"), "datahub_stop_words filter should be present");

    // Check tokenizers
    @SuppressWarnings("unchecked")
    Map<String, Object> tokenizers = (Map<String, Object>) analysis.get("tokenizer");

    assertNotNull(tokenizers.get("main_tokenizer"), "main_tokenizer should be present");
    assertNotNull(tokenizers.get("slash_tokenizer"), "slash_tokenizer should be present");
    assertNotNull(
        tokenizers.get("unit_separator_path_tokenizer"),
        "unit_separator_path_tokenizer should be present");
    assertNotNull(
        tokenizers.get("unit_separator_tokenizer"), "unit_separator_tokenizer should be present");
    assertNotNull(tokenizers.get("word_gram_tokenizer"), "word_gram_tokenizer should be present");

    // Check normalizers
    @SuppressWarnings("unchecked")
    Map<String, Object> normalizers = (Map<String, Object>) analysis.get("normalizer");

    assertNotNull(normalizers.get("keyword_normalizer"), "keyword_normalizer should be present");
  }

  @Test
  public void testSettingsConsistency() {
    // Test that settings are consistent across multiple calls
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3); // Set required default value
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndex(anyString())).thenReturn(true);
    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);

    Map<String, Object> settings1 = builder.getSettings(indexConfiguration, "test_index");
    Map<String, Object> settings2 = builder.getSettings(indexConfiguration, "test_index");

    assertEquals(settings1, settings2, "Settings should be consistent across multiple calls");
  }

  @Test
  public void testNullIndexConfiguration() {
    // Test that the constructor handles null IndexConfiguration gracefully
    try {
      IndexConvention indexConvention = mock(IndexConvention.class);
      LegacySettingsBuilder builder = new LegacySettingsBuilder(null, indexConvention);
      fail("Constructor should not accept null IndexConfiguration");
    } catch (Exception e) {
      // Expected behavior - constructor should not accept null
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null IndexConfiguration");
    }
  }

  @Test
  public void testEmptyIndexConfiguration() {
    // Test with empty IndexConfiguration
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3); // Set required default value
    // Don't set any specific values, use defaults

    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndex(anyString())).thenReturn(true);
    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);
    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    // Should still build valid settings
    assertNotNull(settings, "Settings should be built even with empty configuration");
    assertNotNull(settings.get("analysis"), "Analysis configuration should be present");
  }

  @Test
  public void testNullMainTokenizer() {
    // Test with null mainTokenizer (simulates when ELASTICSEARCH_MAIN_TOKENIZER is not set)
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3); // Set required default value
    indexConfiguration.setMainTokenizer(null); // Explicitly set to null

    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndex(anyString())).thenReturn(true);
    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);
    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    // Should still build valid settings
    assertNotNull(settings, "Settings should be built even with null mainTokenizer");
    assertNotNull(settings.get("analysis"), "Analysis configuration should be present");

    @SuppressWarnings("unchecked")
    Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");

    assertNotNull(analysis.get("analyzer"), "Analyzers should be present");

    @SuppressWarnings("unchecked")
    Map<String, Object> analyzers = (Map<String, Object>) analysis.get("analyzer");

    // Check that analyzers are still created
    assertTrue(
        analyzers.containsKey("word_delimited"), "word_delimited analyzer should be present");
    assertTrue(
        analyzers.containsKey("query_word_delimited"),
        "query_word_delimited analyzer should be present");

    // Verify that the analyzers use the default MAIN_TOKENIZER when mainTokenizer is null
    @SuppressWarnings("unchecked")
    Map<String, Object> wordDelimitedAnalyzer =
        (Map<String, Object>) analyzers.get("word_delimited");
    assertEquals(
        wordDelimitedAnalyzer.get("tokenizer"),
        LegacySettingsBuilder.MAIN_TOKENIZER,
        "Should use default MAIN_TOKENIZER when mainTokenizer is null");

    @SuppressWarnings("unchecked")
    Map<String, Object> queryWordDelimitedAnalyzer =
        (Map<String, Object>) analyzers.get("query_word_delimited");
    assertEquals(
        queryWordDelimitedAnalyzer.get("tokenizer"),
        LegacySettingsBuilder.MAIN_TOKENIZER,
        "Should use default MAIN_TOKENIZER when mainTokenizer is null");
  }

  @Test
  public void testEmptyStringMainTokenizer() {
    // Test with empty string mainTokenizer (should also fall back to default)
    IndexConfiguration indexConfiguration = new IndexConfiguration();
    indexConfiguration.setMinSearchFilterLength(3); // Set required default value
    indexConfiguration.setMainTokenizer(""); // Set to empty string

    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.isV2EntityIndex(anyString())).thenReturn(true);
    LegacySettingsBuilder builder = new LegacySettingsBuilder(indexConfiguration, indexConvention);
    Map<String, Object> settings = builder.getSettings(indexConfiguration, "test_index");

    // Should still build valid settings
    assertNotNull(settings, "Settings should be built even with empty string mainTokenizer");
    assertNotNull(settings.get("analysis"), "Analysis configuration should be present");

    @SuppressWarnings("unchecked")
    Map<String, Object> analysis = (Map<String, Object>) settings.get("analysis");
    @SuppressWarnings("unchecked")
    Map<String, Object> analyzers = (Map<String, Object>) analysis.get("analyzer");

    // Check that analyzers are still created
    assertTrue(
        analyzers.containsKey("word_delimited"), "word_delimited analyzer should be present");
    assertTrue(
        analyzers.containsKey("query_word_delimited"),
        "query_word_delimited analyzer should be present");

    // Verify that empty string also falls back to MAIN_TOKENIZER
    @SuppressWarnings("unchecked")
    Map<String, Object> wordDelimitedAnalyzer =
        (Map<String, Object>) analyzers.get("word_delimited");
    assertEquals(
        wordDelimitedAnalyzer.get("tokenizer"),
        LegacySettingsBuilder.MAIN_TOKENIZER,
        "Should use default MAIN_TOKENIZER when mainTokenizer is empty string");

    @SuppressWarnings("unchecked")
    Map<String, Object> queryWordDelimitedAnalyzer =
        (Map<String, Object>) analyzers.get("query_word_delimited");
    assertEquals(
        queryWordDelimitedAnalyzer.get("tokenizer"),
        LegacySettingsBuilder.MAIN_TOKENIZER,
        "Should use default MAIN_TOKENIZER when mainTokenizer is empty string");
  }
}
