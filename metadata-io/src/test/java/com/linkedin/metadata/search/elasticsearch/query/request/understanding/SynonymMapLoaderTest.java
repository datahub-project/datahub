package com.linkedin.metadata.search.elasticsearch.query.request.understanding;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class SynonymMapLoaderTest {

  @Test
  public void testLoadsDefaultSynonyms() {
    Map<String, Set<String>> synonyms = SynonymMapLoader.loadDefault();
    // An equivalence line maps every term to the whole group
    assertEquals(synonyms.get("stg"), Set.of("stg", "staging"));
    assertEquals(synonyms.get("staging"), Set.of("stg", "staging"));
    // Explicit mappings split phrases into tokens; as whole-term synonyms they would give "cac"
    // matches on names such as "customer" or "cost"
    assertFalse(synonyms.containsKey("cac"));
    assertFalse(synonyms.containsKey("big query"));
  }
}
