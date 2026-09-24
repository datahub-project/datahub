package com.linkedin.metadata.graph.cache;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class CacheStatusTest {

  @Test
  public void exposesAllOperationalStates() {
    assertEquals(CacheStatus.values().length, 6);
    assertEquals(CacheStatus.ACTIVE.name(), "ACTIVE");
    assertEquals(CacheStatus.BUILDING.name(), "BUILDING");
  }
}
