package datahub.client.v2.entity;

import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;

/**
 * Represents a cached aspect with metadata about its source and write status.
 *
 * <p>This class wraps an aspect along with:
 *
 * <ul>
 *   <li>Source - whether it came from the server or was created locally
 *   <li>Dirty flag - whether it needs to be written to the server
 *   <li>Timestamp - when it was cached (for TTL enforcement)
 * </ul>
 */
public class CachedAspect {
  private final RecordTemplate aspect;
  private final AspectSource source;
  private boolean dirty;
  private final long timestamp;

  /**
   * Creates a new cached aspect.
   *
   * @param aspect the aspect data
   * @param source where this aspect came from
   * @param dirty whether this aspect needs to be written to the server
   */
  public CachedAspect(@Nonnull RecordTemplate aspect, @Nonnull AspectSource source, boolean dirty) {
    this.aspect = aspect;
    this.source = source;
    this.dirty = dirty;
    this.timestamp = System.currentTimeMillis();
  }

  /** Returns whether this aspect has unsaved local modifications. */
  public boolean isDirty() {
    return dirty;
  }

  /** Marks this aspect as having unsaved local modifications. */
  public void markDirty() {
    this.dirty = true;
  }

  /** Marks this aspect as clean (in sync with server). */
  public void markClean() {
    this.dirty = false;
  }

  /** Returns the cached aspect data. */
  @Nonnull
  public RecordTemplate getAspect() {
    return aspect;
  }

  /** Returns the source of this aspect. */
  @Nonnull
  public AspectSource getSource() {
    return source;
  }

  /** Returns when this aspect was cached (milliseconds since epoch). */
  public long getTimestamp() {
    return timestamp;
  }
}
