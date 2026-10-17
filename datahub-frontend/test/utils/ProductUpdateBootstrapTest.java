package utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ProductUpdateBootstrapTest {

  @Test
  public void testEnabledUpdateInlinesJsonAndPreloadsImage() {
    String snippet =
        ProductUpdateBootstrap.snippet(
            "{\"enabled\":true,\"id\":\"v2.3\",\"title\":\"October\","
                + "\"image\":\"https://example.com/release.png\"}");

    assertTrue(snippet.contains("window.__DATAHUB_PRODUCT_UPDATE__="));
    assertTrue(snippet.contains("\"id\":\"v2.3\""));
    assertTrue(
        snippet.contains(
            "<link rel=\"preload\" as=\"image\" href=\"https://example.com/release.png\" fetchpriority=\"high\">"));
  }

  @Test
  public void testDisabledOrInvalidJsonInjectsNothing() {
    assertEquals("", ProductUpdateBootstrap.snippet("{\"enabled\":false,\"id\":\"v2.3\"}"));
    assertEquals("", ProductUpdateBootstrap.snippet("not json"));
    assertEquals("", ProductUpdateBootstrap.snippet((String) null));
  }

  @Test
  public void testScriptBreakoutIsEscaped() {
    String snippet =
        ProductUpdateBootstrap.snippet(
            "{\"enabled\":true,\"id\":\"v2.3\",\"title\":\"</script><script>alert(1)</script>\"}");

    assertFalse(snippet.contains("</script><script>"));
    assertTrue(snippet.contains("\\u003c/script"));
  }
}
