package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SynonymMapLoaderTest {

  @Test
  public void testLoadsDefaultSynonyms() {
    Map<String, Set<String>> synonyms =
        SynonymMapLoader.loadFromClasspath(SynonymMapLoader.DEFAULT_SYNONYMS_RESOURCE);
    // An equivalence line maps every term to the whole group
    assertEquals(synonyms.get("stg"), Set.of("stg", "staging"));
    assertEquals(synonyms.get("staging"), Set.of("stg", "staging"));
    // An explicit mapping expands only its left-hand terms
    assertEquals(synonyms.get("big query"), Set.of("bigquery", "big", "query"));
    assertFalse(synonyms.containsKey("big"));
  }

  @Test
  public void testMissingFileDisablesExpansion() {
    assertTrue(SynonymMapLoader.loadFromClasspath("elasticsearch/synonyms/missing.txt").isEmpty());
  }
}
