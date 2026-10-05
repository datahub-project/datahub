package com.linkedin.metadata.search.elasticsearch.update;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A small map keyed by object identity that never keeps its keys alive and never grows past a fixed
 * size. {@link BulkTelemetry} uses it to remember a value per bulk action or per bulk request
 * without retaining the request, its source document or anything it references.
 *
 * <ul>
 *   <li>Keys are held through {@link WeakReference}s and compared with {@code ==} (the JDK has a
 *       weak map and an identity map, but no weak identity map). An entry whose key is garbage
 *       collected without being removed (an action whose add was rejected, a batch whose {@code
 *       afterBulk} never ran) is purged the next time the table is touched.
 *   <li>At {@code capacity}, putting a new key evicts the oldest entry, so the newest keys are
 *       always tracked: a burst of keys that are never removed degrades the oldest ones, it does
 *       not stop tracking.
 *   <li>Values dropped by either path (purged or evicted, not removed) are counted and handed to
 *       the optional {@code onDrop} callback outside the lock.
 * </ul>
 *
 * <p>Thread-safe; every method takes the table's monitor briefly.
 */
final class WeakIdentityTable<V> {

  private final int capacity;
  @Nullable private final Consumer<? super V> onDrop;
  private final ReferenceQueue<Object> collected = new ReferenceQueue<>();
  private final LinkedHashMap<Key, V> entries = new LinkedHashMap<>();
  private long dropped;

  WeakIdentityTable(int capacity, @Nullable Consumer<? super V> onDrop) {
    if (capacity < 1) {
      throw new IllegalArgumentException("capacity must be positive: " + capacity);
    }
    this.capacity = capacity;
    this.onDrop = onDrop;
  }

  /** Maps {@code key} to {@code value} as the newest entry, evicting the oldest at capacity. */
  void put(@Nonnull Object key, @Nonnull V value) {
    List<V> drops;
    synchronized (this) {
      drops = purge();
      Key k = new Key(key, collected);
      entries.remove(k); // re-putting a key makes it the newest, not a candidate for eviction
      entries.put(k, value);
      if (entries.size() > capacity) {
        Iterator<V> oldest = entries.values().iterator();
        if (drops == null) {
          drops = new ArrayList<>(1);
        }
        while (entries.size() > capacity) {
          drops.add(oldest.next());
          oldest.remove();
          dropped++;
        }
      }
    }
    notifyDropped(drops);
  }

  /** The value for {@code key}, or null. */
  @Nullable
  V get(@Nullable Object key) {
    if (key == null) {
      return null;
    }
    List<V> drops;
    V value;
    synchronized (this) {
      drops = purge();
      value = entries.get(new Key(key, null));
    }
    notifyDropped(drops);
    return value;
  }

  /** Removes and returns the value for {@code key}, or null. A removal is not a drop. */
  @Nullable
  V remove(@Nullable Object key) {
    if (key == null) {
      return null;
    }
    List<V> drops;
    V value;
    synchronized (this) {
      drops = purge();
      value = entries.remove(new Key(key, null));
    }
    notifyDropped(drops);
    return value;
  }

  /** Live entries, after purging collected keys. */
  int size() {
    List<V> drops;
    int size;
    synchronized (this) {
      drops = purge();
      size = entries.size();
    }
    notifyDropped(drops);
    return size;
  }

  /** Entries dropped so far without being removed: evicted at capacity or key collected. */
  synchronized long dropped() {
    return dropped;
  }

  /** Empties the table and returns the values it held; not counted as drops. */
  @Nonnull
  synchronized List<V> clear() {
    List<V> values = new ArrayList<>(entries.values());
    entries.clear();
    while (collected.poll() != null) {
      // every key is gone; nothing left to purge
    }
    return values;
  }

  /** Drops entries whose key was collected; returns them, or null when none (no allocation). */
  @Nullable
  private List<V> purge() {
    List<V> drops = null;
    for (Reference<?> ref; (ref = collected.poll()) != null; ) {
      // A key removed earlier is also enqueued when collected; it finds nothing here.
      V value = entries.remove(ref);
      if (value != null) {
        if (drops == null) {
          drops = new ArrayList<>(1);
        }
        drops.add(value);
        dropped++;
      }
    }
    return drops;
  }

  private void notifyDropped(@Nullable List<V> drops) {
    if (drops != null && onDrop != null) {
      for (V value : drops) {
        onDrop.accept(value);
      }
    }
  }

  /**
   * Weak identity key. Its hash is the referent's identity hash, fixed at creation; it equals
   * itself, or another key whose referent is the same live object. Once collected it equals only
   * itself, which is how {@link #purge} finds the entry.
   */
  static final class Key extends WeakReference<Object> {
    private final int hash;

    Key(@Nonnull Object referent, @Nullable ReferenceQueue<Object> queue) {
      super(referent, queue);
      this.hash = System.identityHashCode(referent);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Key)) {
        return false;
      }
      Key that = (Key) other;
      Object referent = get();
      return that.hash == hash && referent != null && referent == that.get();
    }
  }
}
