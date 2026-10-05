package com.linkedin.metadata.search.elasticsearch.update;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.testng.annotations.Test;

public class WeakIdentityTableTest {

  /** Two distinct keys that are equal by {@code equals}: the table must tell them apart. */
  private static final class Same {
    @Override
    public boolean equals(Object o) {
      return o instanceof Same;
    }

    @Override
    public int hashCode() {
      return 1;
    }
  }

  /** Runs the collector until {@code done} holds; weak references clear on an explicit GC. */
  static void collectUntil(BooleanSupplier done) throws InterruptedException {
    for (int i = 0; i < 100 && !done.getAsBoolean(); i++) {
      System.gc();
      Thread.sleep(10);
    }
    assertTrue(done.getAsBoolean(), "garbage was not collected");
  }

  @Test
  public void rejectsNonPositiveCapacity() {
    expectThrows(IllegalArgumentException.class, () -> new WeakIdentityTable<String>(0, null));
  }

  @Test
  public void keysAreComparedByIdentity() {
    WeakIdentityTable<String> table = new WeakIdentityTable<>(10, null);
    Same a = new Same();
    Same b = new Same();
    table.put(a, "a");
    assertNull(table.get(b), "equal but not identical");
    table.put(b, "b");
    assertEquals(table.size(), 2);
    assertEquals(table.get(a), "a");
    table.put(a, "a2");
    assertEquals(table.size(), 2, "re-putting replaces");
    assertEquals(table.get(a), "a2");
    assertEquals(table.remove(a), "a2");
    assertNull(table.remove(a));
    assertNull(table.get(null));
    assertNull(table.remove(null));
    assertEquals(table.dropped(), 0L, "a removal is not a drop");
  }

  @Test
  public void evictsTheOldestAtCapacity() {
    List<String> dropped = new ArrayList<>();
    WeakIdentityTable<String> table = new WeakIdentityTable<>(2, dropped::add);
    Object k1 = new Object();
    Object k2 = new Object();
    Object k3 = new Object();
    Object k4 = new Object();
    table.put(k1, "1");
    table.put(k2, "2");
    table.put(k1, "1b"); // re-put: k1 is now the newest
    table.put(k3, "3");
    assertEquals(dropped, List.of("2"));
    assertEquals(table.get(k1), "1b");
    assertEquals(table.get(k3), "3");
    table.put(k4, "4");
    assertEquals(dropped, List.of("2", "1b"));
    assertEquals(table.size(), 2);
    assertEquals(table.dropped(), 2L);
  }

  @Test
  public void collectedKeysArePurgedAndReported() throws InterruptedException {
    List<String> dropped = new ArrayList<>();
    WeakIdentityTable<String> table = new WeakIdentityTable<>(100, dropped::add);
    Object kept = new Object();
    table.put(kept, "kept");
    Object removed = new Object();
    table.put(removed, "removed");
    table.remove(removed);
    WeakReference<Object> probe = put(table, "gone");
    removed = null;

    collectUntil(() -> probe.get() == null && table.size() == 1);
    assertEquals(dropped, List.of("gone"), "a removed key is not dropped again when collected");
    assertEquals(table.dropped(), 1L);
    assertEquals(table.get(kept), "kept");

    WeakReference<Object> viaGet = put(table, "viaGet");
    collectUntil(() -> viaGet.get() == null && table.get(kept) != null && dropped.size() == 2);
    WeakReference<Object> viaRemove = put(table, "viaRemove");
    collectUntil(
        () -> viaRemove.get() == null && table.remove(new Object()) == null && dropped.size() == 3);
    WeakReference<Object> viaPut = put(table, "viaPut");
    collectUntil(
        () -> {
          if (viaPut.get() != null) {
            return false;
          }
          table.put(kept, "kept");
          return dropped.size() == 4;
        });
    assertEquals(dropped, List.of("gone", "viaGet", "viaRemove", "viaPut"));
  }

  @Test
  public void purgeWithoutCallbackStillCounts() throws InterruptedException {
    WeakIdentityTable<String> table = new WeakIdentityTable<>(100, null);
    WeakReference<Object> probe = put(table, "x");
    collectUntil(() -> probe.get() == null && table.size() == 0);
    assertEquals(table.dropped(), 1L);
  }

  @Test
  public void clearReturnsValuesWithoutCountingDrops() throws InterruptedException {
    List<String> dropped = new ArrayList<>();
    WeakIdentityTable<String> table = new WeakIdentityTable<>(100, dropped::add);
    Object a = new Object();
    table.put(a, "a");
    WeakReference<Object> probe = put(table, "b");
    collectUntil(() -> probe.get() == null);
    List<String> cleared = table.clear();
    assertTrue(cleared.contains("a"));
    assertEquals(table.size(), 0);
    assertTrue(dropped.isEmpty(), "cleared, not dropped");
    assertEquals(table.dropped(), 0L);
    assertNull(table.get(a));
  }

  @Test
  @SuppressWarnings("EqualsWithItself")
  public void keyEqualityIsReferentIdentity() {
    Object referent = new Object();
    WeakIdentityTable.Key key = new WeakIdentityTable.Key(referent, null);
    assertTrue(key.equals(key));
    assertTrue(key.equals(new WeakIdentityTable.Key(referent, null)));
    assertEquals(key.hashCode(), System.identityHashCode(referent));
    assertFalse(key.equals(referent), "not a key");
    assertFalse(key.equals(new WeakIdentityTable.Key(new Object(), null)));
    WeakIdentityTable.Key cleared = new WeakIdentityTable.Key(referent, null);
    cleared.clear();
    assertFalse(cleared.equals(key), "a collected key equals only itself");
    assertTrue(cleared.equals(cleared));
  }

  /** Puts a value under a key nothing else references; returns a probe for the key. */
  private static WeakReference<Object> put(WeakIdentityTable<String> table, String value) {
    Object key = new Object();
    table.put(key, value);
    return new WeakReference<>(key);
  }
}
