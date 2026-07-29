package com.linkedin.metadata.graph.cache.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import org.testng.annotations.Test;

public class EntityGraphCacheKeysTest {

  @Test
  public void testFullGraphKeyIncludesSource() {
    assertEquals(
        EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.PRIMARY), "domain@primary");
    assertEquals(
        EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH), "domain@search");
  }

  @Test
  public void testComponentKeyFormat() {
    String key =
        EntityGraphCacheKeys.componentCacheKey(
            "domain", GraphSnapshotSource.PRIMARY, "abc123def4567890");
    assertEquals(key, "domain@primary:abc123def4567890");
  }

  @Test
  public void testPartialFailureMarkerKeyFormat() {
    assertEquals(
        EntityGraphCacheKeys.partialFailureMarkerKey(
            "domain", GraphSnapshotSource.PRIMARY, "urn:li:domain:root"),
        "domain@primary:marker:urn:li:domain:root");
  }

  @Test
  public void testGraphIdFromCacheKey() {
    assertEquals(EntityGraphCacheKeys.graphIdFromCacheKey("domain@primary"), "domain");
    assertEquals(
        EntityGraphCacheKeys.graphIdFromCacheKey("domain@primary:abc123def4567890"), "domain");
  }

  @Test
  public void testSourceFromCacheKey() {
    assertEquals(
        EntityGraphCacheKeys.sourceFromCacheKey("domain@search"), GraphSnapshotSource.SEARCH);
    assertEquals(
        EntityGraphCacheKeys.sourceFromCacheKey("domain@primary:abc123"),
        GraphSnapshotSource.PRIMARY);
  }

  @Test
  public void testCacheKeyMatchesSource() {
    assertTrue(
        EntityGraphCacheKeys.cacheKeyMatchesSource(
            "domain@primary:fp", "domain", GraphSnapshotSource.PRIMARY));
  }
}
