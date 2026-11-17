package com.linkedin.metadata.config.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class SearchClusterUriTest {

  @Test
  public void testParsePlainHttpEndpoint() {
    SearchClusterUri uri = SearchClusterUri.parse("primary", "http://search:9200");
    assertEquals(uri.getHost(), "search");
    assertEquals(uri.getPort(), 9200);
    assertFalse(uri.isUseSSL());
    assertEquals(uri.getPathPrefix(), "");
  }

  @Test
  public void testParseHttpsImpliesSsl() {
    SearchClusterUri uri = SearchClusterUri.parse("primary", "https://es.example:443/opensearch");
    assertTrue(uri.isUseSSL());
    assertEquals(uri.getPort(), 443);
    assertEquals(uri.getPathPrefix(), "/opensearch");
  }

  @Test
  public void testPortDefaultsToSchemeDefault() {
    assertEquals(SearchClusterUri.parse("primary", "https://es.example").getPort(), 443);
    assertEquals(SearchClusterUri.parse("primary", "http://es.example").getPort(), 80);
  }

  @Test
  public void testBarePathIsNoPrefix() {
    assertEquals(SearchClusterUri.parse("primary", "http://search:9200/").getPathPrefix(), "");
  }

  @Test
  public void testTrailingSlashStrippedFromPrefix() {
    assertEquals(
        SearchClusterUri.parse("primary", "http://search:9200/opensearch/").getPathPrefix(),
        "/opensearch");
  }

  @Test
  public void testCredentialsInUriRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchClusterUri.parse("primary", "http://user:pass@search:9200"));
  }

  @Test
  public void testQueryOrFragmentRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchClusterUri.parse("primary", "http://search:9200?foo=bar"));
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchClusterUri.parse("primary", "http://search:9200#frag"));
  }

  @Test
  public void testNonHttpSchemeRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SearchClusterUri.parse("primary", "opensearch://search:9200"));
  }

  @Test
  public void testSynthesizeFromLegacySettings() {
    assertEquals(SearchClusterUri.synthesize("search", 9200, false, null), "http://search:9200");
    assertEquals(SearchClusterUri.synthesize("search", 9200, true, null), "https://search:9200");
  }

  @Test
  public void testSynthesizeAddsLeadingSlashToPathPrefix() {
    // ELASTICSEARCH_PATH_PREFIX is commonly set without a leading slash.
    assertEquals(
        SearchClusterUri.synthesize("search", 9200, false, "opensearch"),
        "http://search:9200/opensearch");
    assertEquals(
        SearchClusterUri.synthesize("search", 9200, false, "/opensearch"),
        "http://search:9200/opensearch");
  }

  @Test
  public void testSynthesizeDefaultsHostToLocalhost() {
    assertEquals(SearchClusterUri.synthesize(null, 9200, false, null), "http://localhost:9200");
    assertEquals(SearchClusterUri.synthesize("  ", 9200, false, null), "http://localhost:9200");
  }
}
