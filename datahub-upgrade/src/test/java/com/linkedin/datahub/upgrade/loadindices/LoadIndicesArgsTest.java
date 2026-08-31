package com.linkedin.datahub.upgrade.loadindices;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

import java.util.List;
import org.testng.annotations.Test;

public class LoadIndicesArgsTest {

  @Test
  public void testDefaultValues() {
    LoadIndicesArgs args = new LoadIndicesArgs();

    assertEquals(0, args.batchSize);
    assertEquals(0, args.limit);
    assertEquals(null, args.urnLike);
    assertEquals(null, args.lePitEpochMs);
    assertEquals(null, args.gePitEpochMs);
    assertEquals(null, args.aspectNames);
  }

  @Test
  public void testSettersAndGetters() {
    LoadIndicesArgs args = new LoadIndicesArgs();

    args.batchSize = 1000;
    args.limit = 5000;
    args.urnLike = "urn:li:dataset:*";
    args.lePitEpochMs = 1640995200000L;
    args.gePitEpochMs = 1672531200000L;
    args.aspectNames = List.of("datasetProperties", "ownership");

    assertEquals(1000, args.batchSize);
    assertEquals(5000, args.limit);
    assertEquals("urn:li:dataset:*", args.urnLike);
    assertEquals(Long.valueOf(1640995200000L), args.lePitEpochMs);
    assertEquals(Long.valueOf(1672531200000L), args.gePitEpochMs);
    assertEquals(List.of("datasetProperties", "ownership"), args.aspectNames);
  }

  @Test
  public void testClone() {
    LoadIndicesArgs original = new LoadIndicesArgs();
    original.batchSize = 1000;
    original.limit = 5000;
    original.urnLike = "urn:li:dataset:*";
    original.lePitEpochMs = 1640995200000L;
    original.gePitEpochMs = 1672531200000L;
    original.aspectNames = List.of("datasetProperties", "ownership");

    LoadIndicesArgs cloned = original.clone();

    // Verify it's a different object
    assertNotSame(original, cloned);

    // Verify all values are copied
    assertEquals(original.batchSize, cloned.batchSize);
    assertEquals(original.limit, cloned.limit);
    assertEquals(original.urnLike, cloned.urnLike);
    assertEquals(original.lePitEpochMs, cloned.lePitEpochMs);
    assertEquals(original.gePitEpochMs, cloned.gePitEpochMs);
    assertEquals(original.aspectNames, cloned.aspectNames);

    // Verify modifying clone doesn't affect original
    cloned.batchSize = 2000;
    cloned.urnLike = "urn:li:table:*";

    assertEquals(1000, original.batchSize);
    assertEquals("urn:li:dataset:*", original.urnLike);
    assertEquals(2000, cloned.batchSize);
    assertEquals("urn:li:table:*", cloned.urnLike);
  }

  @Test
  public void testCloneWithNullValues() {
    LoadIndicesArgs original = new LoadIndicesArgs();
    original.batchSize = 1000;
    original.limit = 5000;
    // Leave other fields as null

    LoadIndicesArgs cloned = original.clone();

    assertNotSame(original, cloned);
    assertEquals(1000, cloned.batchSize);
    assertEquals(5000, cloned.limit);
    assertEquals(null, cloned.urnLike);
    assertEquals(null, cloned.lePitEpochMs);
    assertEquals(null, cloned.gePitEpochMs);
    assertEquals(null, cloned.aspectNames);
  }
}
