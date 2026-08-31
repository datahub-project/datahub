package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import java.util.Map;
import org.junit.Test;

/** Tests for WriteTrackingAspectCache. */
public class WriteTrackingAspectCacheTest {

  private static final long TTL_MILLIS = 1000L;

  @Test
  public void testPutAndGetAspect() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    Status retrieved = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrieved);
    assertEquals(retrieved, aspect);
  }

  @Test
  public void testGetWithAllowDirtyReturnsDirtyAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.LOCAL, true);

    Status retrieved = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrieved);
    assertEquals(retrieved, aspect);
  }

  @Test
  public void testGetWithServerOnlySkipsDirtyAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.LOCAL, true);

    Status retrieved = cache.get("status", Status.class, ReadMode.SERVER_ONLY);
    assertNull(retrieved);
  }

  @Test
  public void testGetWithServerOnlyReturnsCleanAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    Status retrieved = cache.get("status", Status.class, ReadMode.SERVER_ONLY);
    assertNotNull(retrieved);
    assertEquals(retrieved, aspect);
  }

  @Test
  public void testGetReturnsNullForNonExistentAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);

    Status retrieved = cache.get("nonexistent", Status.class, ReadMode.ALLOW_DIRTY);
    assertNull(retrieved);
  }

  @Test
  public void testGetEnforcesTtlForServerSourcedAspects() throws InterruptedException {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(100L); // 100ms TTL
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    Status retrieved1 = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrieved1);

    Thread.sleep(150);

    Status retrieved2 = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNull(retrieved2);
  }

  @Test
  public void testGetDoesNotEnforceTtlForLocalSourcedAspects() throws InterruptedException {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(100L); // 100ms TTL
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.LOCAL, true);

    Status retrieved1 = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrieved1);

    Thread.sleep(150);

    Status retrieved2 = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrieved2);
  }

  @Test
  public void testGetRemovesExpiredServerAspectsFromCache() throws InterruptedException {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(100L);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);
    Thread.sleep(150);

    Status retrieved = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNull(retrieved);

    Status retrieved2 = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNull(retrieved2);
  }

  @Test(expected = ClassCastException.class)
  public void testGetThrowsClassCastExceptionForWrongAspectClassType() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    cache.get("status", Ownership.class, ReadMode.ALLOW_DIRTY);
  }

  @Test
  public void testMarkDirtyMarksExistingAspectAsDirty() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    Status retrieved1 = cache.get("status", Status.class, ReadMode.SERVER_ONLY);
    assertNotNull(retrieved1);

    cache.markDirty("status");

    Status retrieved2 = cache.get("status", Status.class, ReadMode.SERVER_ONLY);
    assertNull(retrieved2);

    Status retrieved3 = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrieved3);
  }

  @Test
  public void testMarkDirtyIsIdempotent() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    cache.markDirty("status");
    cache.markDirty("status");

    Status retrieved = cache.get("status", Status.class, ReadMode.SERVER_ONLY);
    assertNull(retrieved);
  }

  @Test
  public void testMarkDirtyOnNonExistentAspectDoesNothing() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);

    cache.markDirty("nonexistent");

    Status retrieved = cache.get("nonexistent", Status.class, ReadMode.ALLOW_DIRTY);
    assertNull(retrieved);
  }

  @Test
  public void testGetDirtyAspectsReturnsOnlyDirtyAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status dirtyAspect = new Status().setRemoved(false);
    Status cleanAspect = new Status().setRemoved(true);

    cache.put("dirty", dirtyAspect, AspectSource.LOCAL, true);
    cache.put("clean", cleanAspect, AspectSource.SERVER, false);

    Map<String, RecordTemplate> dirtyAspects = cache.getDirtyAspects();

    assertEquals(dirtyAspects.size(), 1);
    assertTrue(dirtyAspects.containsKey("dirty"));
    assertFalse(dirtyAspects.containsKey("clean"));
    assertEquals(dirtyAspects.get("dirty"), dirtyAspect);
  }

  @Test
  public void testGetDirtyAspectsReturnsEmptyMapWhenNoDirtyAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("clean", aspect, AspectSource.SERVER, false);

    Map<String, RecordTemplate> dirtyAspects = cache.getDirtyAspects();

    assertNotNull(dirtyAspects);
    assertTrue(dirtyAspects.isEmpty());
  }

  @Test
  public void testGetDirtyAspectsReturnsEmptyMapForEmptyCache() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);

    Map<String, RecordTemplate> dirtyAspects = cache.getDirtyAspects();

    assertNotNull(dirtyAspects);
    assertTrue(dirtyAspects.isEmpty());
  }

  @Test
  public void testHasPendingWritesReturnsTrueForDirtyAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.LOCAL, true);

    assertTrue(cache.hasPendingWrites("status"));
  }

  @Test
  public void testHasPendingWritesReturnsFalseForCleanAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    assertFalse(cache.hasPendingWrites("status"));
  }

  @Test
  public void testHasPendingWritesReturnsFalseForNonExistentAspects() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);

    assertFalse(cache.hasPendingWrites("nonexistent"));
  }

  @Test
  public void testMarkCleanClearsDirtyFlag() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.LOCAL, true);
    assertTrue(cache.hasPendingWrites("status"));

    cache.markClean("status");

    assertFalse(cache.hasPendingWrites("status"));
    Status retrieved = cache.get("status", Status.class, ReadMode.SERVER_ONLY);
    assertNotNull(retrieved);
  }

  @Test
  public void testMarkCleanOnNonExistentAspectDoesNothing() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);

    cache.markClean("nonexistent");

    Status retrieved = cache.get("nonexistent", Status.class, ReadMode.ALLOW_DIRTY);
    assertNull(retrieved);
  }

  @Test
  public void testClearDirtyClearsAllDirtyFlags() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect1 = new Status().setRemoved(false);
    Status aspect2 = new Status().setRemoved(true);
    Status aspect3 = new Status().setRemoved(false);

    cache.put("dirty1", aspect1, AspectSource.LOCAL, true);
    cache.put("dirty2", aspect2, AspectSource.LOCAL, true);
    cache.put("clean", aspect3, AspectSource.SERVER, false);

    Map<String, RecordTemplate> dirtyBefore = cache.getDirtyAspects();
    assertEquals(dirtyBefore.size(), 2);

    cache.clearDirty();

    Map<String, RecordTemplate> dirtyAfter = cache.getDirtyAspects();
    assertEquals(dirtyAfter.size(), 0);

    Status retrieved1 = cache.get("dirty1", Status.class, ReadMode.SERVER_ONLY);
    assertNotNull(retrieved1);

    Status retrieved2 = cache.get("dirty2", Status.class, ReadMode.SERVER_ONLY);
    assertNotNull(retrieved2);
  }

  @Test
  public void testMultipleAspectsWithDifferentDirtyStates() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect1 = new Status().setRemoved(false);
    Status aspect2 = new Status().setRemoved(true);
    Status aspect3 = new Status().setRemoved(false);

    cache.put("dirty", aspect1, AspectSource.LOCAL, true);
    cache.put("clean", aspect2, AspectSource.SERVER, false);
    cache.put("marked", aspect3, AspectSource.SERVER, false);
    cache.markDirty("marked");

    Map<String, RecordTemplate> dirtyAspects = cache.getDirtyAspects();
    assertEquals(dirtyAspects.size(), 2);
    assertTrue(dirtyAspects.containsKey("dirty"));
    assertTrue(dirtyAspects.containsKey("marked"));
    assertFalse(dirtyAspects.containsKey("clean"));

    assertTrue(cache.hasPendingWrites("dirty"));
    assertFalse(cache.hasPendingWrites("clean"));
    assertTrue(cache.hasPendingWrites("marked"));
  }

  @Test
  public void testOverwritingExistingAspectWithPut() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect1 = new Status().setRemoved(false);
    Status aspect2 = new Status().setRemoved(true);

    cache.put("status", aspect1, AspectSource.SERVER, false);
    Status retrieved1 = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertEquals(retrieved1, aspect1);

    cache.put("status", aspect2, AspectSource.LOCAL, true);
    Status retrieved2 = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertEquals(retrieved2, aspect2);
    assertTrue(cache.hasPendingWrites("status"));
  }

  @Test
  public void testTtlBoundaryConditionsJustExpired() throws InterruptedException {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(100L);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    Thread.sleep(101);

    Status retrieved = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNull(retrieved);
  }

  @Test
  public void testTtlBoundaryConditionsJustValid() throws InterruptedException {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(100L);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    Thread.sleep(50);

    Status retrieved = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrieved);
  }

  @Test
  public void testMixingServerAndLocalAspectsInSameCache() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status serverAspect = new Status().setRemoved(false);
    Status localAspect = new Status().setRemoved(true);

    cache.put("server", serverAspect, AspectSource.SERVER, false);
    cache.put("local", localAspect, AspectSource.LOCAL, true);

    Status retrievedServer = cache.get("server", Status.class, ReadMode.SERVER_ONLY);
    assertNotNull(retrievedServer);

    Status retrievedLocal = cache.get("local", Status.class, ReadMode.SERVER_ONLY);
    assertNull(retrievedLocal);

    Status retrievedLocalWithDirty = cache.get("local", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrievedLocalWithDirty);
  }

  @Test(expected = ClassCastException.class)
  public void testGetThrowsClassCastExceptionWhenTypeMismatch() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.SERVER, false);

    cache.get("status", Ownership.class, ReadMode.ALLOW_DIRTY);
  }

  @Test
  public void testClearDirtyDoesNotRemoveAspectsFromCache() {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);
    Status aspect = new Status().setRemoved(false);

    cache.put("status", aspect, AspectSource.LOCAL, true);

    cache.clearDirty();

    Status retrieved = cache.get("status", Status.class, ReadMode.ALLOW_DIRTY);
    assertNotNull(retrieved);
    assertEquals(retrieved, aspect);
  }

  @Test
  public void testThreadSafeConcurrentPutAndGet() throws Exception {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);

    Thread writer =
        new Thread(
            () -> {
              for (int i = 0; i < 100; i++) {
                Status aspect = new Status().setRemoved(i % 2 == 0);
                cache.put("status" + i, aspect, AspectSource.SERVER, false);
              }
            });

    Thread reader =
        new Thread(
            () -> {
              for (int i = 0; i < 100; i++) {
                cache.get("status" + i, Status.class, ReadMode.ALLOW_DIRTY);
              }
            });

    writer.start();
    reader.start();

    writer.join();
    reader.join();
  }

  @Test
  public void testThreadSafeMarkDirtyAndGetDirtyAspects() throws Exception {
    WriteTrackingAspectCache cache = new WriteTrackingAspectCache(TTL_MILLIS);

    for (int i = 0; i < 50; i++) {
      Status aspect = new Status().setRemoved(false);
      cache.put("aspect" + i, aspect, AspectSource.SERVER, false);
    }

    Thread marker =
        new Thread(
            () -> {
              for (int i = 0; i < 50; i++) {
                cache.markDirty("aspect" + i);
              }
            });

    Thread getter =
        new Thread(
            () -> {
              for (int i = 0; i < 10; i++) {
                cache.getDirtyAspects();
              }
            });

    marker.start();
    getter.start();

    marker.join();
    getter.join();

    Map<String, RecordTemplate> dirtyAspects = cache.getDirtyAspects();
    assertEquals(dirtyAspects.size(), 50);
  }
}
