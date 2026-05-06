package com.linkedin.metadata.timeseries;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/** Direct tests for the atomic read-and-clear that runs server-side on Hazelcast. */
public class GetAndClearUrnsFromReverseIndexProcessorTest {

  private static final String INDEX_KEY = "aspect-index:dataset:datasetProfile";
  private static final String URN_A = "urn:li:dataset:foo";
  private static final String URN_B = "urn:li:dataset:bar";

  @Test
  public void process_emptySlot_returnsEmpty() {
    Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, null);

    Set<String> result = new GetAndClearUrnsFromReverseIndexProcessor().process(entry);

    assertTrue(result.isEmpty());
    assertNull(entry.getValue(), "empty slot should remain unset");
  }

  @Test
  public void process_emptySet_returnsEmpty() {
    Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, new HashSet<>());

    Set<String> result = new GetAndClearUrnsFromReverseIndexProcessor().process(entry);

    assertTrue(result.isEmpty());
  }

  @Test
  public void process_existingSet_returnsCopyAndClearsEntry() {
    Set<String> original = new HashSet<>();
    original.add(URN_A);
    original.add(URN_B);
    Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, original);

    Set<String> result = new GetAndClearUrnsFromReverseIndexProcessor().process(entry);

    assertEquals(result, Set.of(URN_A, URN_B));
    assertNotSame(result, original, "must return a defensive copy, not the live set");
    assertNull(
        entry.getValue(), "entry value should be set to null so the reverse-index slot is cleared");
  }

  @Test
  public void process_nonSetValue_returnsEmpty() {
    // A misshapen value at the index key — defensive: don't throw, just report no URNs.
    Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, "garbage");

    Set<String> result = new GetAndClearUrnsFromReverseIndexProcessor().process(entry);

    assertTrue(result.isEmpty());
  }
}
