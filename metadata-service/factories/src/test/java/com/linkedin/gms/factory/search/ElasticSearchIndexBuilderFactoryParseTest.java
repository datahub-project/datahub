package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ElasticSearchIndexBuilderFactoryParseTest {

  @Test
  public void testFlatStringSettingsUnchanged() {
    Map<String, Map<String, Object>> parsed =
        ElasticSearchIndexBuilderFactory.parseIndexSettingsMap(
            "{\"my_index\":{\"number_of_shards\":\"3\"}}");
    assertEquals(parsed, Map.of("my_index", Map.of("number_of_shards", "3")));
  }

  @Test
  public void testEmptyAndNull() {
    assertTrue(ElasticSearchIndexBuilderFactory.parseIndexSettingsMap(null).isEmpty());
    assertTrue(ElasticSearchIndexBuilderFactory.parseIndexSettingsMap("").isEmpty());
    assertTrue(ElasticSearchIndexBuilderFactory.parseIndexSettingsMap("null").isEmpty());
  }

  @Test
  public void testNestedAnalysisKeepsScalarsAsJsonText() {
    Map<String, Map<String, Object>> parsed =
        ElasticSearchIndexBuilderFactory.parseIndexSettingsMap(
            "{\"my_index\":{\"analysis\":{"
                + "\"filter\":{\"min_length_2\":{\"type\":\"length\",\"min\":2}},"
                + "\"analyzer\":{\"word_delimited\":{\"filter\":[\"lowercase\",\"min_length_2\"]}}"
                + "}}}");
    Map<String, Object> analysis = (Map<String, Object>) parsed.get("my_index").get("analysis");
    Map<String, Object> filter =
        (Map<String, Object>) ((Map<String, Object>) analysis.get("filter")).get("min_length_2");
    // numbers stay as their JSON text so they compare equal to stored settings ("2", not "2.0")
    assertEquals(filter, Map.of("type", "length", "min", "2"));
    Map<String, Object> analyzer =
        (Map<String, Object>)
            ((Map<String, Object>) analysis.get("analyzer")).get("word_delimited");
    assertEquals(analyzer.get("filter"), List.of("lowercase", "min_length_2"));
  }

  @Test
  public void testJsonNullInsideArrayPreserved() {
    Map<String, Map<String, Object>> parsed =
        ElasticSearchIndexBuilderFactory.parseIndexSettingsMap(
            "{\"my_index\":{\"analysis\":{\"analyzer\":{\"word_delimited\":"
                + "{\"filter\":[\"lowercase\",null,\"min_length_2\"]}}}}}");
    Map<String, Object> analysis = (Map<String, Object>) parsed.get("my_index").get("analysis");
    Map<String, Object> analyzer =
        (Map<String, Object>)
            ((Map<String, Object>) analysis.get("analyzer")).get("word_delimited");
    assertEquals(analyzer.get("filter"), Arrays.asList("lowercase", null, "min_length_2"));
  }

  @Test
  public void testJsonNullEntriesDropped() {
    Map<String, Map<String, Object>> parsed =
        ElasticSearchIndexBuilderFactory.parseIndexSettingsMap(
            "{\"my_index\":{\"refresh_interval\":null,"
                + "\"analysis\":{\"filter\":{\"f\":{\"type\":\"length\",\"min\":null}}}}}");
    assertEquals(
        parsed,
        Map.of(
            "my_index",
            Map.of("analysis", Map.of("filter", Map.of("f", Map.of("type", "length"))))));
  }
}
