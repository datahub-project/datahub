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
  @SuppressWarnings("unchecked")
  public void process_emptySlot_createsSetWithUrn() {
    Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, null);

    Boolean added = new AddUrnToReverseIndexProcessor(URN_A).process(entry);

    assertTrue(added);
    Set<String> stored = (Set<String>) entry.getValue();
    assertEquals(stored, Set.of(URN_A));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void process_existingSetMissingUrn_addsAndReturnsTrue() {
    Set<String> existing = new HashSet<>();
    existing.add(URN_A);
    Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, existing);

    Boolean added = new AddUrnToReverseIndexProcessor(URN_B).process(entry);

    assertTrue(added, "adding a new URN to an existing set must return true");
    Set<String> stored = (Set<String>) entry.getValue();
    assertEquals(stored, Set.of(URN_A, URN_B));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void process_existingSetWithUrn_noChangeReturnsFalse() {
    Set<String> existing = new HashSet<>();
    existing.add(URN_A);
    Map.Entry<String, Object> entry = new AbstractMap.SimpleEntry<>(INDEX_KEY, existing);

    Boolean added = new AddUrnToReverseIndexProcessor(URN_A).process(entry);

    assertFalse(added, "adding an already-present URN must return false");
    Set<String> stored = (Set<String>) entry.getValue();
    assertEquals(stored, Set.of(URN_A));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void process_nonSetValue_replacedWithFreshSet() {
    // Defensive — keys are namespaced "aspect-index:" vs. "latest:", so a non-Set value at
    // an index key shouldn't happen, but the processor handles it rather than throwing.
    Map.Entry<String, Object> entry =
        new AbstractMap.SimpleEntry<>(INDEX_KEY, "garbage-from-somewhere");

    Boolean added = new AddUrnToReverseIndexProcessor(URN_A).process(entry);

    assertTrue(added);
    Set<String> stored = (Set<String>) entry.getValue();
    assertEquals(stored, Set.of(URN_A));
  }
}
