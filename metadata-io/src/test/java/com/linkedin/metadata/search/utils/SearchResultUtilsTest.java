package com.linkedin.metadata.search.utils;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.template.DoubleMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.search.features.Features;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class SearchResultUtilsTest {

  @Test
  public void testBuildBaseFeaturesIncludesBackendScoreAndQueryCount() {
    Map<String, Object> source = new HashMap<>();
    source.put("usageCountLast30Days", 42);

    DoubleMap features = SearchResultUtils.buildBaseFeatures(1.23d, source);

    assertEquals(
        features.get(Features.Name.SEARCH_BACKEND_SCORE.toString()), Double.valueOf(1.23d));
    assertEquals(features.get(Features.Name.QUERY_COUNT.toString()), Double.valueOf(42.0d));
  }

  @Test
  public void testBuildBaseFeaturesWithoutUsageOnlyBackendScore() {
    DoubleMap features = SearchResultUtils.buildBaseFeatures(0.5d, null);

    assertEquals(features.get(Features.Name.SEARCH_BACKEND_SCORE.toString()), Double.valueOf(0.5d));
    assertFalse(features.containsKey(Features.Name.QUERY_COUNT.toString()));
  }

  @Test
  public void testToExtraFieldsStringifiesValues() {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> source = new HashMap<>();
    source.put("urn", "urn:li:dataset:(platform,name,PROD)");
    source.put("usageCountLast30Days", 7);

    StringMap extra = SearchResultUtils.toExtraFields(mapper, source);

    // JSON-stringified primitives/strings
    assertEquals(extra.get("usageCountLast30Days"), "7");
    assertEquals(extra.get("urn"), "\"urn:li:dataset:(platform,name,PROD)\"");
  }

  @Test
  public void testToExtraFieldsNullSourceReturnsEmpty() {
    ObjectMapper mapper = new ObjectMapper();
    StringMap extra = SearchResultUtils.toExtraFields(mapper, null);
    assertTrue(extra.isEmpty());
  }
}
