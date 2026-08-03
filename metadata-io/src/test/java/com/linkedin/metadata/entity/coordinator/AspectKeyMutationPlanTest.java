package com.linkedin.metadata.entity.coordinator;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.Constants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  public void constructorDefensivelyCopiesConflictKeys() {
    TreeSet<ConflictKey> source = new TreeSet<>();
    source.add(ConflictKey.of("D", "1"));
    MutationPlan plan = new MutationPlan(source);

    // Mutating the caller's set after construction must not leak into the plan.
    source.add(ConflictKey.of("D", "2"));

    assertEquals(plan.conflictKeys().size(), 1);
  }
}
