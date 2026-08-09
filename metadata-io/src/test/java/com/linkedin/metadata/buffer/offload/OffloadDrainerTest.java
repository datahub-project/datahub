package com.linkedin.metadata.buffer.offload;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

/**
 * Behavior tests for the framework {@link OffloadDrainer}'s grouping / context-resolution / retry /
 * DLQ contract, using an in-memory fake buffer (no Hazelcast). The contract under test is the
 * data-loss boundary: permanent failures drop entries; transient failures leave them for
 * at-least-once retry; the {@link DrainAction} owns per-entry removal.
 */
public class OffloadDrainerTest {

  private static final OperationContext SYS =
      TestOperationContexts.systemContextNoSearchAuthorization();

  /** In-memory {@link OffloadBuffer} recording removeIfSame/requeue calls for assertions. */
  static final class FakeBuffer implements OffloadBuffer<TestKey, Long> {
    final LinkedHashMap<TestKey, Long> pending = new LinkedHashMap<>();
    final List<String> removes = new ArrayList<>();
    final List<String> requeues = new ArrayList<>();
    final List<String> enqueues = new ArrayList<>();

    @Override
    public boolean enqueue(@Nonnull TestKey key, @Nonnull Long value) {
      enqueues.add(key.id);
      pending.put(key, value);
      return true;
    }

    @Override
    public long nextSequence() {
      return pending.size() + 1;
    }

    @Override
    @Nonnull
    public List<Map.Entry<TestKey, Long>> drain(int limit) {
      List<Map.Entry<TestKey, Long>> out = new ArrayList<>();
      int n = 0;
      for (Map.Entry<TestKey, Long> e : pending.entrySet()) {
        if (n++ >= limit) {
          break;
        }
        out.add(e);
      }
      return out;
    }

    @Override
    public boolean removeIfSame(@Nonnull TestKey key, @Nonnull Long expected) {
      removes.add(key.id);
      return pending.remove(key, expected);
    }

    @Override
    public void requeue(@Nonnull TestKey key, @Nonnull Long value) {
      requeues.add(key.id);
      pending.put(key, value);
    }

    @Override
    public boolean defersApply() {
      return true;
    }

    @Override
    public int size() {
      return pending.size();
    }

    @Nullable
    @Override
    public Object tryAcquireDrainLock(@Nonnull String lockName, @Nonnull Duration lease) {
      return "token";
    }

    @Override
    public void releaseDrainLock(@Nonnull String lockName, @Nonnull Object token) {}
  }

  /** Resolver that groups by the key's tenant id (TestKey.id prefix before ':'). */
  static final class TenantResolver implements OffloadContextResolver<TestKey> {
    @Override
    @Nonnull
    public String groupKey(@Nonnull TestKey key) {
      int cut = key.id.indexOf(':');
      return cut < 0 ? key.id : key.id.substring(0, cut);
    }

    @Override
    @Nonnull
    public OperationContext resolveOpContext(@Nonnull TestKey key, @Nonnull OperationContext sys) {
      return sys; // tests don't assert on the context object
    }
  }

  static final class TestKey implements Serializable {
    private static final long serialVersionUID = 1L;
    final String id;

    TestKey(String id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof TestKey && ((TestKey) o).id.equals(id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public String toString() {
      return "TestKey[" + id + "]";
    }
  }

  private static OffloadDrainer<TestKey, Long> drainer(
      OffloadBuffer<TestKey, Long> buffer,
      OffloadContextResolver<TestKey> resolver,
      DrainAction<TestKey, Long> action,
      boolean enabled) {
    return new OffloadDrainer<>(buffer, resolver, SYS, action, 10, 60_000L, enabled, "test", null);
  }

  @Test
  public void testDisabledTickIsNoOp() {
    FakeBuffer buffer = new FakeBuffer();
    buffer.enqueue(new TestKey("t1:k"), 1L);
    OffloadDrainer<TestKey, Long> d =
        drainer(buffer, new TenantResolver(), (g, ctx, buf) -> {}, false);
    d.tick();
    assertEquals(buffer.size(), 1); // nothing drained
  }

  @Test
  public void testGroupsByRoutingKeyAndRemovesFinishedEntries() {
    FakeBuffer buffer = new FakeBuffer();
    buffer.enqueue(new TestKey("t1:a"), 1L);
    buffer.enqueue(new TestKey("t2:b"), 2L);
    buffer.enqueue(new TestKey("t1:c"), 3L);

    List<String> seenGroups = new ArrayList<>();
    OffloadDrainer<TestKey, Long> d =
        drainer(
            buffer,
            new TenantResolver(),
            (group, ctx, buf) -> {
              seenGroups.add(group.get(0).getKey().id);
              for (Map.Entry<TestKey, Long> e : group) {
                buf.removeIfSame(e.getKey(), e.getValue());
              }
            },
            true);
    d.tick();

    // Two groups (t1, t2); all entries removed.
    assertEquals(buffer.size(), 0);
    assertEquals(seenGroups.size(), 2);
  }

  @Test
  public void testUnresolvableGroupKeyDropsEntry() {
    FakeBuffer buffer = new FakeBuffer();
    TestKey poison = new TestKey("poison");
    buffer.enqueue(poison, 1L);
    OffloadContextResolver<TestKey> resolver =
        new OffloadContextResolver<TestKey>() {
          @Override
          @Nonnull
          public String groupKey(@Nonnull TestKey key) {
            throw new UnresolvableOffloadKeyException("never resolves: " + key.id);
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull TestKey key, @Nonnull OperationContext sys) {
            return sys;
          }
        };
    OffloadDrainer<TestKey, Long> d = drainer(buffer, resolver, (g, ctx, buf) -> {}, true);
    d.tick();
    assertEquals(buffer.size(), 0); // dropped
    assertTrue(buffer.removes.contains("poison"));
  }

  @Test
  public void testTransientGroupKeyFailureLeavesEntryForRetry() {
    FakeBuffer buffer = new FakeBuffer();
    buffer.enqueue(new TestKey("t1:k"), 1L);
    OffloadContextResolver<TestKey> resolver =
        new OffloadContextResolver<TestKey>() {
          @Override
          @Nonnull
          public String groupKey(@Nonnull TestKey key) {
            throw new RuntimeException("transient blip");
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull TestKey key, @Nonnull OperationContext sys) {
            return sys;
          }
        };
    OffloadDrainer<TestKey, Long> d = drainer(buffer, resolver, (g, ctx, buf) -> {}, true);
    d.tick();
    assertEquals(buffer.size(), 1); // left for retry, NOT dropped
  }

  @Test
  public void testTransientResolveOpContextFailureLeavesEntriesForRetry() {
    FakeBuffer buffer = new FakeBuffer();
    buffer.enqueue(new TestKey("t1:a"), 1L);
    buffer.enqueue(new TestKey("t1:b"), 2L);
    OffloadContextResolver<TestKey> resolver =
        new OffloadContextResolver<TestKey>() {
          @Override
          @Nonnull
          public String groupKey(@Nonnull TestKey key) {
            return "t1";
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull TestKey key, @Nonnull OperationContext sys) {
            throw new RuntimeException("transient resolve blip");
          }
        };
    OffloadDrainer<TestKey, Long> d = drainer(buffer, resolver, (g, ctx, buf) -> {}, true);
    d.tick();
    assertEquals(buffer.size(), 2); // both left for retry
  }

  @Test
  public void testTransientActionFailureLeavesUnremovedEntriesForRetry() {
    FakeBuffer buffer = new FakeBuffer();
    buffer.enqueue(new TestKey("t1:a"), 1L);
    buffer.enqueue(new TestKey("t1:b"), 2L);
    DrainAction<TestKey, Long> action =
        (group, ctx, buf) -> {
          // Remove only the first entry, then throw — simulating partial success.
          buf.removeIfSame(group.get(0).getKey(), group.get(0).getValue());
          throw new RuntimeException("transient action blip");
        };
    OffloadDrainer<TestKey, Long> d = drainer(buffer, new TenantResolver(), action, true);
    d.tick();
    // One removed (the finished one), one left for retry (at-least-once).
    assertEquals(buffer.size(), 1);
  }

  @Test
  public void testUnresolvableActionFailureDropsGroup() {
    FakeBuffer buffer = new FakeBuffer();
    buffer.enqueue(new TestKey("t1:a"), 1L);
    buffer.enqueue(new TestKey("t1:b"), 2L);
    DrainAction<TestKey, Long> action =
        (group, ctx, buf) -> {
          throw new UnresolvableOffloadKeyException("permanent group failure");
        };
    OffloadDrainer<TestKey, Long> d = drainer(buffer, new TenantResolver(), action, true);
    d.tick();
    assertEquals(buffer.size(), 0); // whole group dropped
  }

  @Test
  public void testBackoffRemovesTransientGroupKeyFailureAndRequeuesLater() {
    FakeBuffer buffer = new FakeBuffer();
    buffer.enqueue(new TestKey("t1:k"), 1L);
    OffloadContextResolver<TestKey> resolver =
        new OffloadContextResolver<TestKey>() {
          @Override
          @Nonnull
          public String groupKey(@Nonnull TestKey key) {
            throw new RuntimeException("transient tenant-lookup blip");
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull TestKey key, @Nonnull OperationContext sys) {
            return sys;
          }
        };
    OffloadDrainer<TestKey, Long> d =
        new OffloadDrainer<>(
            buffer, resolver, SYS, (g, ctx, buf) -> {}, 10, 60_000L, true, "test", null, true, 3L);

    // Tick 1: transient failure → key removed from buffer, moved to backoff limbo (size 0).
    d.tick();
    assertEquals(buffer.size(), 0);
    assertEquals(buffer.requeues.size(), 0); // backoff re-merges via enqueue, not requeue

    // Ticks 2-3: still in backoff; buffer stays empty (key NOT re-merged yet).
    d.tick();
    d.tick();
    assertEquals(buffer.size(), 0);

    // Tick 4: backoff (3 ticks) expired → key re-merged via enqueue → drain surfaces it → groupKey
    // still throws transient → backoff again. Buffer ends empty (removed again this tick).
    d.tick();
    assertEquals(buffer.size(), 0);
    // Re-merge happened via enqueue (2nd enqueue: initial setup + 1 re-merge); requeue NOT used.
    assertEquals(buffer.enqueues.size(), 2);
    assertEquals(buffer.requeues.size(), 0);
  }

  @Test
  public void testBackoffDisabledLeavesTransientGroupKeyFailureInBuffer() {
    // backoffEnabled=false (default): transient failure leaves the key in-buffer for next-tick
    // retry
    // (no removeIfSame). This is the hooks behavior — no first-page starvation because hooks never
    // throw transient resolver failures.
    FakeBuffer buffer = new FakeBuffer();
    buffer.enqueue(new TestKey("t1:k"), 1L);
    OffloadContextResolver<TestKey> resolver =
        new OffloadContextResolver<TestKey>() {
          @Override
          @Nonnull
          public String groupKey(@Nonnull TestKey key) {
            throw new RuntimeException("transient blip");
          }

          @Override
          @Nonnull
          public OperationContext resolveOpContext(
              @Nonnull TestKey key, @Nonnull OperationContext sys) {
            return sys;
          }
        };
    OffloadDrainer<TestKey, Long> d = drainer(buffer, resolver, (g, ctx, buf) -> {}, true);
    d.tick();
    assertEquals(buffer.size(), 1); // left in buffer, NOT removed
    assertTrue(buffer.removes.isEmpty());
  }
}
