package com.linkedin.metadata.timeseries;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/** Direct tests for the atomic URN insert that runs server-side on Hazelcast. */
public class AddUrnToReverseIndexProcessorTest {

  private static final String INDEX_KEY = "aspect-index:dataset:datasetProfile";
  private static final String URN_A = "urn:li:dataset:foo";
  private static final String URN_B = "urn:li:dataset:bar";

  @Test
  public void process_emptySlot_createsSetWithUrn() {
    Map.Entry<String, Set<String>> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, null);

    Boolean added = new AddUrnToReverseIndexProcessor(URN_A).process(entry);

    assertTrue(added);
    Set<String> stored = entry.getValue();
    assertEquals(stored, Set.of(URN_A));
  }

  @Test
  public void process_existingSetMissingUrn_addsAndReturnsTrue() {
    Set<String> existing = new HashSet<>();
    existing.add(URN_A);
    Map.Entry<String, Set<String>> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, existing);

    Boolean added = new AddUrnToReverseIndexProcessor(URN_B).process(entry);

    assertTrue(added, "adding a new URN to an existing set must return true");
    Set<String> stored = entry.getValue();
    assertEquals(stored, Set.of(URN_A, URN_B));
  }

  @Test
  public void process_existingSetWithUrn_noChangeReturnsFalse() {
    Set<String> existing = new HashSet<>();
    existing.add(URN_A);
    Map.Entry<String, Set<String>> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, existing);

    Boolean added = new AddUrnToReverseIndexProcessor(URN_A).process(entry);

    assertFalse(added, "adding an already-present URN must return false");
    Set<String> stored = entry.getValue();
    assertEquals(stored, Set.of(URN_A));
  }
}
