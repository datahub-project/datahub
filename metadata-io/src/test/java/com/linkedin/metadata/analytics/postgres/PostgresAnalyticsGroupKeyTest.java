package com.linkedin.metadata.analytics.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class PostgresAnalyticsGroupKeyTest {

  @Test
  public void emptyAndStable() {
    assertEquals(PostgresAnalyticsGroupKey.of(Map.of()), "");
    String a = PostgresAnalyticsGroupKey.of(Map.of("b", "2", "a", "1"));
    String b = PostgresAnalyticsGroupKey.of(new LinkedHashMap<>(Map.of("a", "1", "b", "2")));
    assertEquals(a, b);
    assertNotEquals(a, PostgresAnalyticsGroupKey.of(Map.of("a", "1")));
  }

  @Test
  public void canonicalize_matchesOfFiltering() {
    Map<String, String> raw = new LinkedHashMap<>();
    raw.put("a", "1");
    raw.put("blank", "  ");
    raw.put("nullVal", null);
    raw.put(null, "x");
    raw.put("b", "2");

    Map<String, String> canon = PostgresAnalyticsGroupKey.canonicalize(raw);
    assertEquals(canon, Map.of("a", "1", "b", "2"));
    assertEquals(PostgresAnalyticsGroupKey.of(raw), PostgresAnalyticsGroupKey.of(canon));
  }
}
