package com.linkedin.metadata.search.elasticsearch.client.shim.builder.es8;

import static org.testng.Assert.*;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.Map;
import org.testng.annotations.Test;

public class Es8SemanticIndexSettingsBuilderTest {

  private SemanticIndexSpec spec() {
    return SemanticIndexSpec.builder()
        .indexName("datasetindex_v2_semantic")
        .modelKey("gemini_embedding_001")
        .vectorDimension(3072)
        .build();
  }

  @Test
  public void testNoKnnKey() {
    Map<String, Object> settings = Es8SemanticIndexSettingsBuilder.build(spec());
    assertFalse(
        settings.containsKey("knn"),
        "ES 8 uses dense_vector field mapping, not the OpenSearch-style top-level knn setting");
  }

  @Test
  public void testNoHardcodedShardsOrReplicas() {
    Map<String, Object> settings = Es8SemanticIndexSettingsBuilder.build(spec());
    assertFalse(
        settings.containsKey("number_of_shards"),
        "Shard count should be left to engine defaults / index templates, not hardcoded");
    assertFalse(
        settings.containsKey("number_of_replicas"),
        "Replica count should be left to engine defaults / index templates, not hardcoded");
  }
}
