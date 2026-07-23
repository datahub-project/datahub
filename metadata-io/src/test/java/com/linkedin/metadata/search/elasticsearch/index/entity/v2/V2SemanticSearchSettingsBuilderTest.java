package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/** Tests for V2SemanticSearchSettingsBuilder. */
public class V2SemanticSearchSettingsBuilderTest {

  @Test
  public void testAddsKnnToSemanticIndex() {
    // Setup
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);

    V2LegacySettingsBuilder v2Builder = mock(V2LegacySettingsBuilder.class);

    Map<String, Object> baseSettings = new HashMap<>();
    baseSettings.put("max_ngram_diff", 17);
    baseSettings.put("analysis", Map.of("analyzer", Map.of()));

    when(v2Builder.getSettings(indexConfiguration, "datasetindex_v2")).thenReturn(baseSettings);

    V2SemanticSearchSettingsBuilder semanticBuilder =
        new V2SemanticSearchSettingsBuilder(indexConvention, v2Builder);

    // Execute - use semantic index name
    Map<String, Object> settings =
        semanticBuilder.getSettings(indexConfiguration, "datasetindex_v2_semantic");

    // Verify
    assertNotNull(settings, "Settings should be returned");

    // Verify k-NN is enabled
    assertTrue(settings.containsKey("knn"), "Should have knn key");
    assertEquals(settings.get("knn"), true, "k-NN should be enabled");

    // Verify base settings are preserved
    assertEquals(settings.get("max_ngram_diff"), 17, "Base settings should be preserved");
    assertNotNull(settings.get("analysis"), "Analysis configuration should be preserved");

    // Verify delegation with base index name (without _semantic suffix)
    verify(v2Builder).getSettings(indexConfiguration, "datasetindex_v2");
  }

  @Test
  public void testEs8EngineOmitsIndexLevelKnnSetting() {
    // ES 8 rejects the OpenSearch-only "index.knn" setting because dense_vector handles k-NN
    // at the field level. Verify the builder respects that.
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);

    V2LegacySettingsBuilder v2Builder = mock(V2LegacySettingsBuilder.class);
    Map<String, Object> baseSettings = new HashMap<>();
    baseSettings.put("max_ngram_diff", 17);
    when(v2Builder.getSettings(indexConfiguration, "datasetindex_v2")).thenReturn(baseSettings);

    SearchClientShim<?> es8Shim = mock(SearchClientShim.class);
    when(es8Shim.getEngineType()).thenReturn(SearchEngineType.ELASTICSEARCH_8);

    V2SemanticSearchSettingsBuilder semanticBuilder =
        new V2SemanticSearchSettingsBuilder(indexConvention, v2Builder, es8Shim);

    Map<String, Object> settings =
        semanticBuilder.getSettings(indexConfiguration, "datasetindex_v2_semantic");

    assertFalse(settings.containsKey("knn"), "ES 8 should not emit the index.knn setting");
    assertEquals(settings.get("max_ngram_diff"), 17, "Base settings should be preserved");
  }

  @Test
  public void testOpenSearch2EngineEmitsIndexLevelKnnSetting() {
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);

    V2LegacySettingsBuilder v2Builder = mock(V2LegacySettingsBuilder.class);
    when(v2Builder.getSettings(indexConfiguration, "datasetindex_v2")).thenReturn(new HashMap<>());

    SearchClientShim<?> os2Shim = mock(SearchClientShim.class);
    when(os2Shim.getEngineType()).thenReturn(SearchEngineType.OPENSEARCH_2);

    V2SemanticSearchSettingsBuilder semanticBuilder =
        new V2SemanticSearchSettingsBuilder(indexConvention, v2Builder, os2Shim);

    Map<String, Object> settings =
        semanticBuilder.getSettings(indexConfiguration, "datasetindex_v2_semantic");

    assertEquals(settings.get("knn"), true, "OS 2 must emit index.knn=true to enable the plugin");
  }

  @Test
  public void testNonSemanticIndexReturnsEmptySettings() {
    // Setup
    IndexConfiguration indexConfiguration =
        IndexConfiguration.builder().minSearchFilterLength(3).build();
    EntityIndexConfiguration entityIndexConfiguration = new EntityIndexConfiguration();
    IndexConvention indexConvention = IndexConventionImpl.noPrefix("MD5", entityIndexConfiguration);

    V2LegacySettingsBuilder v2Builder = mock(V2LegacySettingsBuilder.class);

    V2SemanticSearchSettingsBuilder semanticBuilder =
        new V2SemanticSearchSettingsBuilder(indexConvention, v2Builder);

    // Execute - use non-semantic index name
    Map<String, Object> settings =
        semanticBuilder.getSettings(indexConfiguration, "datasetindex_v2");

    // Verify
    assertNotNull(settings, "Settings should not be null");
    assertTrue(settings.isEmpty(), "Non-semantic indices should return empty settings");

    // Verify v2Builder was NOT called for non-semantic index
    verify(v2Builder, never()).getSettings(any(), any());
  }
}
