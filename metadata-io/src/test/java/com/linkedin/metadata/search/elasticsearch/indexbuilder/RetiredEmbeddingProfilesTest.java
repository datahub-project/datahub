package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.script.Script;
import org.testng.annotations.Test;

public class RetiredEmbeddingProfilesTest {

  @Test
  public void testAProfileOnlyTheSourceMapsIsRetired() {
    Map<String, Object> source = mappings("model_a", "model_b");
    Map<String, Object> destination = mappings("model_a");

    assertEquals(RetiredEmbeddingProfiles.retiredKeys(source, destination), Set.of("model_b"));
  }

  @Test
  public void testAProfileOnlyTheDestinationMapsIsNotRetired() {
    assertTrue(
        RetiredEmbeddingProfiles.retiredKeys(mappings("model_a"), mappings("model_a", "model_c"))
            .isEmpty());
  }

  @Test
  public void testAnIndexWithoutEmbeddingsRetiresNothing() {
    assertTrue(
        RetiredEmbeddingProfiles.retiredKeys(Map.of("properties", Map.of()), mappings("model_a"))
            .isEmpty());
  }

  @Test
  public void testADestinationWithoutEmbeddingsReportsEverySourceProfile() {
    assertEquals(
        RetiredEmbeddingProfiles.retiredKeys(mappings("model_a", "model_b"), propertiesOnly()),
        Set.of("model_a", "model_b"));
  }

  @Test
  public void testEmbeddingsIsDeclaredEvenWhenItListsNoProfiles() {
    assertTrue(RetiredEmbeddingProfiles.declaresEmbeddings(mappings("model_a")));
    assertTrue(
        RetiredEmbeddingProfiles.declaresEmbeddings(
            Map.of("properties", Map.of("embeddings", Map.of("properties", Map.of())))));
    assertFalse(RetiredEmbeddingProfiles.declaresEmbeddings(propertiesOnly()));
    assertFalse(RetiredEmbeddingProfiles.declaresEmbeddings(Map.of()));
  }

  @Test
  public void testTheRemovalScriptNamesTheRetiredKeys() {
    Script script = RetiredEmbeddingProfiles.removalScript(Set.of("model_b"));

    assertEquals(script.getParams().get("retired"), List.of("model_b"));
  }

  @Test
  public void testNoRetiredKeysBuildNoScript() {
    assertNull(RetiredEmbeddingProfiles.removalScript(Set.of()));
  }

  private static Map<String, Object> propertiesOnly() {
    return Map.of("properties", Map.of("urn", Map.of("type", "keyword")));
  }

  private static Map<String, Object> mappings(String... profileKeys) {
    Map<String, Object> profiles = new java.util.TreeMap<>();
    for (String key : profileKeys) {
      profiles.put(key, Map.of("properties", Map.of()));
    }
    return Map.of("properties", Map.of("embeddings", Map.of("properties", profiles)));
  }
}
