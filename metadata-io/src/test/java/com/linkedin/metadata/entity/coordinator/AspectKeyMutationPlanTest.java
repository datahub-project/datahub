package com.linkedin.metadata.entity.coordinator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.Constants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.testng.annotations.Test;

public class AspectKeyMutationPlanTest {

  @Test
  public void aspectKeyOrdersByUrnThenAspectThenVersion() {
    // Deliberately out of order; must sort to (urn, aspect, version) — matching the FOR UPDATE
    // lock-acquisition order in EbeanAspectDao.batchGet.
    AspectKey a = new AspectKey("urn:a", "status", 0);
    AspectKey b = new AspectKey("urn:a", "status", 1);
    AspectKey c = new AspectKey("urn:a", "ownership", 0);
    AspectKey d = new AspectKey("urn:b", "status", 0);

    List<AspectKey> sorted = new ArrayList<>(Arrays.asList(d, b, c, a));
    sorted.sort(null);

    assertEquals(sorted, Arrays.asList(c, a, b, d));
  }

  @Test
  public void latestFactoryUsesAspectLatestVersion() {
    AspectKey key = AspectKey.latest("urn:a", "status");
    assertEquals(key.version(), (long) Constants.ASPECT_LATEST_VERSION);
    assertEquals(key.version(), 0L);
  }

  @Test
  public void mergeConflictKeysUnionsWithoutMutatingEither() {
    ConflictKey k1 = ConflictKey.of("SCHEMA_FIELD_LINKAGE", "urn:parent-1");
    ConflictKey k2 = ConflictKey.of("SCHEMA_FIELD_LINKAGE", "urn:parent-2");

    MutationPlan plan1 =
        new MutationPlan("cmd-1", new TreeSet<>(List.of(k1)), Map.of(), new TreeMap<>());
    MutationPlan plan2 =
        new MutationPlan("cmd-2", new TreeSet<>(List.of(k2)), Map.of(), new TreeMap<>());

    MutationPlan merged = plan1.mergeConflictKeys(plan2);

    // Merged plan contains the union.
    assertTrue(merged.conflictKeys().contains(k1));
    assertTrue(merged.conflictKeys().contains(k2));
    assertEquals(merged.conflictKeys().size(), 2);

    // Neither source plan was mutated.
    assertEquals(plan1.conflictKeys().size(), 1);
    assertTrue(plan1.conflictKeys().contains(k1));
    assertFalse(plan1.conflictKeys().contains(k2));
    assertEquals(plan2.conflictKeys().size(), 1);
  }

  @Test
  public void constructorDefensivelyCopiesConflictKeys() {
    TreeSet<ConflictKey> source = new TreeSet<>();
    source.add(ConflictKey.of("D", "1"));
    MutationPlan plan = new MutationPlan("cmd", source, Map.of(), new TreeMap<>());

    // Mutating the caller's set after construction must not leak into the plan.
    source.add(ConflictKey.of("D", "2"));

    assertEquals(plan.conflictKeys().size(), 1);
  }

  @Test
  public void sortedKeysetReturnsMutationKeysInOrder() {
    AspectKey k1 = AspectKey.latest("urn:a", "status");
    AspectKey k2 = AspectKey.latest("urn:a", "ownership");
    TreeMap<AspectKey, PlannedMutation> mutations = new TreeMap<>();
    mutations.put(k1, new PlannedDelete(k1));
    mutations.put(k2, new PlannedDelete(k2));

    MutationPlan plan = new MutationPlan("cmd", new TreeSet<>(), Map.of(), mutations);

    // ownership sorts before status at the same urn/version.
    assertEquals(new ArrayList<>(plan.sortedKeyset()), List.of(k2, k1));
  }
}
