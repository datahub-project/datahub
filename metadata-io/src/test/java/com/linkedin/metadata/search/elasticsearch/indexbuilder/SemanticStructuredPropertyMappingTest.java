package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SemanticStructuredPropertyMappingTest {
  private static final String INDEX = "documentindex_v2_semantic";
  private static final String MODEL = "test_model";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @DataProvider
  public Object[][] additions() {
    return new Object[][] {{"keyword", false}, {"date", false}, {"keyword", true}, {"date", true}};
  }

  @Test(dataProvider = "additions")
  public void testPropertyAdditionReachesSearchEngine(String propertyType, boolean embeddingDrift)
      throws Exception {
    Map<String, Object> existing = Map.of("existing_property", Map.of("type", "keyword"));
    Map<String, Object> added = new HashMap<>(existing);
    added.put("new_property", Map.of("type", propertyType));
    ReindexConfig state = state(existing, added, embeddingDrift);

    assertTrue(state.hasNewStructuredProperty());
    assertTrue(state.hasSafeStructuredPropertyAddition());
    assertFalse(state.hasRemovedStructuredProperty());
    assertFalse(state.hasStructuredPropertyTypeMismatch());
    assertFalse(state.hasStructuredPropertyCopyToMismatch());
    assertFalse(state.requiresReindex());

    SearchClientShim<?> client = mock(SearchClientShim.class);
    OperationContext context = TestOperationContexts.systemContextNoSearchAuthorization();
    builder(client).applyMappings(context, state, false);

    ArgumentCaptor<PutMappingRequest> request = ArgumentCaptor.forClass(PutMappingRequest.class);
    verify(client).putIndexMapping(any(), request.capture(), any());
    assertEquals(request.getValue().indices(), new String[] {INDEX});
    Map<String, Object> sent =
        MAPPER.readValue(request.getValue().source().utf8ToString(), new TypeReference<>() {});
    Map<String, Object> fields =
        child(child(child(sent, "properties"), "structuredProperties"), "properties");
    assertEquals(fields.get("new_property"), Map.of("type", propertyType));
    if (embeddingDrift) {
      assertEquals(child(sent, "properties").keySet(), java.util.Set.of("structuredProperties"));
      assertFalse(state.isPureStructuredPropertyAddition());
    }
  }

  @Test
  public void testUnrelatedTypeChangeDoesNotTravelWithPropertyAddition() throws Exception {
    Map<String, Object> current = mapping(Map.of(), true);
    Map<String, Object> target =
        MAPPER.convertValue(
            mapping(Map.of("new_property", Map.of("type", "keyword")), false),
            new TypeReference<>() {});
    child(target, "properties").put("urn", Map.of("type", "text"));
    ReindexConfig state =
        ReindexConfig.builder()
            .name(INDEX)
            .exists(true)
            .currentSettings(Settings.builder().put("index.knn", true).build())
            .targetSettings(Map.of("knn", true))
            .currentMappings(current)
            .targetMappings(target)
            .enableIndexMappingsReindex(true)
            .build();
    assertTrue(state.requiresReindex(), "The unrelated type change must still require reindexing");
    assertTrue(state.hasSafeStructuredPropertyAddition());
    assertFalse(state.isPureStructuredPropertyAddition());
    SearchClientShim<?> client = mock(SearchClientShim.class);
    builder(client)
        .applyMappings(TestOperationContexts.systemContextNoSearchAuthorization(), state, false);
    ArgumentCaptor<PutMappingRequest> request = ArgumentCaptor.forClass(PutMappingRequest.class);
    verify(client).putIndexMapping(any(), request.capture(), any());
    Map<String, Object> sent =
        MAPPER.readValue(request.getValue().source().utf8ToString(), new TypeReference<>() {});
    assertEquals(child(sent, "properties").keySet(), java.util.Set.of("structuredProperties"));
  }

  @DataProvider
  public Object[][] conflictingProperties() {
    return new Object[][] {
      {"removed", Map.of()},
      {"type", Map.of("existing_property", Map.of("type", "date"))},
      {
        "copy_to",
        Map.of(
            "existing_property",
            Map.of("type", "keyword", "copy_to", List.of("customFullTextSearchFields")))
      }
    };
  }

  @Test(dataProvider = "conflictingProperties")
  public void testConflictingPropertyChangesRemainBlocked(
      String conflict, Map<String, Object> retained) throws Exception {
    Map<String, Object> target = new HashMap<>(retained);
    target.put("new_property", Map.of("type", "keyword"));
    ReindexConfig state =
        state(Map.of("existing_property", Map.of("type", "keyword")), target, false);
    assertTrue(state.hasNewStructuredProperty());
    assertEquals(state.hasRemovedStructuredProperty(), conflict.equals("removed"));
    assertEquals(state.hasStructuredPropertyTypeMismatch(), conflict.equals("type"));
    assertEquals(state.hasStructuredPropertyCopyToMismatch(), conflict.equals("copy_to"));
    SearchClientShim<?> client = mock(SearchClientShim.class);
    builder(client)
        .applyMappings(TestOperationContexts.systemContextNoSearchAuthorization(), state, false);
    verify(client, never()).putIndexMapping(any(), any(), any());
  }

  private static ESIndexBuilder builder(SearchClientShim<?> client) {
    ElasticSearchConfiguration config = new ElasticSearchConfiguration();
    config.setBuildIndices(new BuildIndicesConfiguration());
    config.setIndex(new IndexConfiguration());
    return new ESIndexBuilder(
        client, config, new StructuredPropertiesConfiguration(), Map.of(), mock(GitVersion.class));
  }

  private static ReindexConfig state(
      Map<String, Object> currentProperties,
      Map<String, Object> targetProperties,
      boolean embeddingDrift) {
    return ReindexConfig.builder()
        .name(INDEX)
        .exists(true)
        .currentSettings(Settings.builder().put("index.knn", true).build())
        .targetSettings(Map.of("knn", true))
        .currentMappings(mapping(currentProperties, embeddingDrift))
        .targetMappings(mapping(targetProperties, false))
        .enableIndexMappingsReindex(false)
        .enableIndexSettingsReindex(false)
        .enableStructuredPropertiesReindex(false)
        .build();
  }

  private static Map<String, Object> mapping(
      Map<String, Object> structuredProperties, boolean embeddingDrift) {
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(4);
    model.setKnnEngine("faiss");
    model.setSpaceType("cosinesimil");
    model.setEfConstruction(128);
    model.setM(16);
    SemanticSearchConfiguration config = new SemanticSearchConfiguration();
    config.setModels(Map.of(MODEL, model));
    Map<String, Object> embeddings =
        MAPPER.convertValue(
            SemanticEmbeddingMappings.buildEmbeddingFieldConfig(config, null),
            new TypeReference<>() {});
    if (embeddingDrift) {
      child(child(embeddings, "properties"), MODEL).remove("dynamic");
      // Dynamic mapping adds metadata emitted alongside chunks by semanticContent writers.
      Map<String, Object> fields =
          child(child(child(embeddings, "properties"), MODEL), "properties");
      Map<String, Object> text =
          Map.of(
              "type",
              "text",
              "fields",
              Map.of("keyword", Map.of("type", "keyword", "ignore_above", 256)));
      fields.put("modelVersion", text);
      fields.put("chunkingStrategy", text);
      fields.put("generatedAt", Map.of("type", "long"));
      fields.put("totalChunks", Map.of("type", "long"));
      fields.put("totalTokens", Map.of("type", "long"));
    }
    return Map.of(
        "properties",
        Map.of(
            "urn",
            Map.of("type", "keyword"),
            "embeddings",
            embeddings,
            "structuredProperties",
            Map.of("type", "object", "dynamic", false, "properties", structuredProperties)));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> child(Map<String, Object> map, String key) {
    return (Map<String, Object>) map.get(key);
  }
}
