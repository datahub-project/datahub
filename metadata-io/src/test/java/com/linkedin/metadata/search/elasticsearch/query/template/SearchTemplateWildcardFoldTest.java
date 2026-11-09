package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.testng.annotations.Test;

/** Case-insensitive wildcards are folded before OpenSearch 3 builds the automaton. */
public class SearchTemplateWildcardFoldTest {

  @Test
  public void testObjectValuedWildcardFoldsBothPatterns() throws Exception {
    String filter =
        """
        {
          "bool": {
            "filter": {
              "wildcard": {
                "name": { "value": "Orders*", "case_insensitive": true }
              }
            }
          }
        }
        """;
    String functionScore =
        """
        {
          "function_score": {
            "query": {
              "wildcard": {
                "name": { "value": "Orders*", "case_insensitive": true }
              }
            }
          }
        }
        """;
    assertFoldedPair(filter, "/bool/filter");
    assertFoldedPair(functionScore, "/function_score/query");
  }

  @Test
  public void testLowercaseObjectWildcardDropsTheFlag() throws Exception {
    String json =
        """
        {
          "bool": {
            "must": {
              "wildcard": {
                "name": { "value": "orders*", "case_insensitive": true }
              }
            }
          }
        }
        """;
    JsonNode folded = read(SearchTemplateJson.foldCaseInsensitiveWildcards(json));
    JsonNode must = folded.at("/bool/must");
    assertTrue(must.has("wildcard"), must.toString());
    assertFalse(must.toString().contains("case_insensitive"), must.toString());
    assertEquals(must.at("/wildcard/name/value").asText(), "orders*");
    assertFalse(SearchTemplateJson.parseQuery(json).toString().contains("case_insensitive"));
  }

  @Test
  public void testArrayWildcardStaysASibling() throws Exception {
    String json =
        """
        {
          "bool": {
            "should": [
              {
                "wildcard": {
                  "name": { "value": "Orders*", "case_insensitive": true }
                }
              }
            ]
          }
        }
        """;
    JsonNode folded = read(SearchTemplateJson.foldCaseInsensitiveWildcards(json));
    ArrayNode should = (ArrayNode) folded.at("/bool/should");
    assertEquals(should.size(), 2, should.toString());
    assertEquals(pattern(should.get(0)), "Orders*");
    assertEquals(pattern(should.get(1)), "orders*");
    assertFalse(folded.toString().contains("case_insensitive"), folded.toString());
  }

  @Test
  public void testTermKeepsCaseInsensitive() {
    String json =
        """
        {"bool":{"filter":{"term":{"name":{"value":"Orders","case_insensitive":true}}}}}
        """;
    assertEquals(SearchTemplateJson.foldCaseInsensitiveWildcards(json), json);
  }

  private static void assertFoldedPair(String json, String clausePath) throws Exception {
    JsonNode folded = read(SearchTemplateJson.foldCaseInsensitiveWildcards(json));
    JsonNode clause = folded.at(clausePath);
    ArrayNode should = (ArrayNode) clause.at("/bool/should");
    assertEquals(should.size(), 2, folded.toString());
    assertEquals(clause.at("/bool/minimum_should_match").asInt(), 1);
    assertEquals(pattern(should.get(0)), "Orders*");
    assertEquals(pattern(should.get(1)), "orders*");
    assertFalse(folded.toString().contains("case_insensitive"), folded.toString());
    assertFalse(SearchTemplateJson.parseQuery(json).toString().contains("case_insensitive"));
  }

  private static String pattern(JsonNode clause) {
    JsonNode value = clause.at("/wildcard/name/value");
    assertTrue(value.isTextual(), clause.toString());
    return value.asText();
  }

  private static JsonNode read(String json) throws Exception {
    return SearchTemplateMappers.JSON.readTree(json);
  }
}
