package com.linkedin.metadata.search.elasticsearch.client.shim.builder.opensearch2;

import static org.testng.Assert.*;

import com.linkedin.metadata.utils.elasticsearch.shim.SemanticIndexSpec;
import java.util.Map;
import org.testng.annotations.Test;

public class OpenSearch2SemanticIndexSettingsBuilderTest {

  private SemanticIndexSpec spec() {
    return SemanticIndexSpec.builder()
        .indexName("datasetindex_v2_semantic")
        .modelKey("text_embedding_3_large")
        .vectorDimension(3072)
        .build();
  }

  @Test
  public void testKnnTruePresent() {
    Map<String, Object> settings = OpenSearch2SemanticIndexSettingsBuilder.build(spec());
    assertTrue(
        settings.containsKey("knn"),
        "OS semantic index must have knn setting to enable knn plugin");
    assertEquals(settings.get("knn"), true, "knn setting must be true");
  }

  @Test
  public void testContainsShardsAndReplicas() {
    Map<String, Object> settings = OpenSearch2SemanticIndexSettingsBuilder.build(spec());
    assertTrue(settings.containsKey("number_of_shards"), "Should include number_of_shards");
    assertTrue(settings.containsKey("number_of_replicas"), "Should include number_of_replicas");
  }
}
