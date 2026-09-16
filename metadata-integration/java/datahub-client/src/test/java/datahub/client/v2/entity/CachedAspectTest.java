package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import org.junit.Test;

/** Tests for CachedAspect. */
public class CachedAspectTest {

  @Test
  public void testConstructorInitializesAllFields() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);

    assertNotNull(cached);
    assertNotNull(cached.getAspect());
    assertNotNull(cached.getSource());
    assertEquals(cached.getAspect(), aspect);
    assertEquals(cached.getSource(), AspectSource.SERVER);
    assertFalse(cached.isDirty());
  }

  @Test
  public void testConstructorWithDirtyFlag() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.LOCAL, true);

    assertTrue(cached.isDirty());
    assertEquals(cached.getSource(), AspectSource.LOCAL);
  }

  @Test
  public void testIsDirtyReturnsCorrectInitialState() {
    Status aspect = new Status().setRemoved(false);

    CachedAspect cleanCached = new CachedAspect(aspect, AspectSource.SERVER, false);
    assertFalse(cleanCached.isDirty());

    CachedAspect dirtyCached = new CachedAspect(aspect, AspectSource.LOCAL, true);
    assertTrue(dirtyCached.isDirty());
  }

  @Test
  public void testMarkDirtySetsDirtyFlagToTrue() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);

    assertFalse(cached.isDirty());

    cached.markDirty();

    assertTrue(cached.isDirty());
  }

  @Test
  public void testMarkDirtyIsIdempotent() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);

    cached.markDirty();
    assertTrue(cached.isDirty());

    cached.markDirty();
    assertTrue(cached.isDirty());
  }

  @Test
  public void testMarkCleanSetsDirtyFlagToFalse() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.LOCAL, true);

    assertTrue(cached.isDirty());

    cached.markClean();

    assertFalse(cached.isDirty());
  }

  @Test
  public void testMarkCleanIsIdempotent() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);

    cached.markClean();
    assertFalse(cached.isDirty());

    cached.markClean();
    assertFalse(cached.isDirty());
  }

  @Test
  public void testGetAspectReturnsTheAspect() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);

    RecordTemplate retrieved = cached.getAspect();

    assertNotNull(retrieved);
    assertEquals(retrieved, aspect);
    assertTrue(retrieved instanceof Status);
  }

  @Test
  public void testGetSourceReturnsTheSource() {
    Status aspect = new Status().setRemoved(false);

    CachedAspect serverCached = new CachedAspect(aspect, AspectSource.SERVER, false);
    assertEquals(serverCached.getSource(), AspectSource.SERVER);

    CachedAspect localCached = new CachedAspect(aspect, AspectSource.LOCAL, true);
    assertEquals(localCached.getSource(), AspectSource.LOCAL);
  }

  @Test
  public void testGetTimestampIsSetOnCreation() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);

    long timestamp = cached.getTimestamp();

    assertTrue(timestamp > 0);
  }

  @Test
  public void testGetTimestampDoesNotChange() throws InterruptedException {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);

    long timestamp1 = cached.getTimestamp();
    Thread.sleep(10);
    long timestamp2 = cached.getTimestamp();

    assertEquals(timestamp1, timestamp2);
  }

  @Test
  public void testTimestampIsApproximatelyCurrentTime() {
    long beforeCreation = System.currentTimeMillis();
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);
    long afterCreation = System.currentTimeMillis();

    long timestamp = cached.getTimestamp();

    assertTrue(timestamp >= beforeCreation);
    assertTrue(timestamp <= afterCreation);
  }

  @Test
  public void testDirtyFlagCanBeToggledMultipleTimes() {
    Status aspect = new Status().setRemoved(false);
    CachedAspect cached = new CachedAspect(aspect, AspectSource.SERVER, false);

    assertFalse(cached.isDirty());

    cached.markDirty();
    assertTrue(cached.isDirty());

    cached.markClean();
    assertFalse(cached.isDirty());

    cached.markDirty();
    assertTrue(cached.isDirty());

    cached.markDirty();
    assertTrue(cached.isDirty());

    cached.markClean();
    assertFalse(cached.isDirty());
  }
}
