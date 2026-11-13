package com.linkedin.metadata.utils.elasticsearch.shim;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class KnnSearchResponseTest {

  @Test
  public void emptyResponse() {
    KnnSearchResponse r = new KnnSearchResponse(List.of());
    assertTrue(r.hits().isEmpty());
    assertTrue(r.isEmpty());
  }

  @Test
  public void carriesHitsInOrder() {
    KnnSearchResponse.Hit h1 = new KnnSearchResponse.Hit("urn:1", 0.9, Map.of("urn", "urn:1"));
    KnnSearchResponse.Hit h2 = new KnnSearchResponse.Hit("urn:2", 0.7, Map.of("urn", "urn:2"));
    KnnSearchResponse r = new KnnSearchResponse(List.of(h1, h2));
    assertEquals(r.hits().size(), 2);
    assertEquals(r.hits().get(0).id(), "urn:1");
    assertEquals(r.hits().get(0).score(), 0.9);
    assertFalse(r.isEmpty());
  }

  @Test
  public void hitAcceptsNullSourceParameter() {
    // The constructor coerces a null source to an empty unmodifiable map; source() must never
    // return null even when called with a literal null argument.
    KnnSearchResponse.Hit hit = new KnnSearchResponse.Hit("urn:1", 0.5, null);
    assertNotNull(hit.source(), "source() must not return null");
    assertTrue(hit.source().isEmpty(), "source() should be empty when constructed with null");
    assertThrows(UnsupportedOperationException.class, () -> hit.source().put("x", "y"));
  }

  @Test
  public void hitSupportsNullSourceFieldValues() {
    // ES/OS responses can include explicit JSON nulls for optional fields.
    // Map.copyOf rejects null values, so the compact constructor must handle this.
    Map<String, Object> source = new HashMap<>();
    source.put("urn", "urn:1");
    source.put("description", null);
    KnnSearchResponse.Hit hit = new KnnSearchResponse.Hit("urn:1", 0.9, source);
    assertEquals(hit.source().get("urn"), "urn:1");
    assertNull(hit.source().get("description"), "null value should be preserved in source map");
  }
}
