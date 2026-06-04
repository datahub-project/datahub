package com.linkedin.metadata.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Contiguous {@code enqueue_seq} watermark advancement for pgQueue consumer offsets. */
public final class PgQueueContiguousOffset {

  private PgQueueContiguousOffset() {}

  /**
   * Advances {@code currentOffset} only across a contiguous run of acked sequences starting at
   * {@code currentOffset + 1}. Sequences at or below {@code currentOffset} are ignored (idempotent
   * re-ack). Stops at the first gap in {@code ackedEnqueueSeqs}.
   */
  public static long advanceWatermark(long currentOffset, @Nonnull List<Long> ackedEnqueueSeqs) {
    if (ackedEnqueueSeqs.isEmpty()) {
      return currentOffset;
    }
    List<Long> sorted = new ArrayList<>(ackedEnqueueSeqs);
    Collections.sort(sorted);
    long watermark = currentOffset;
    for (long seq : sorted) {
      if (seq <= watermark) {
        continue;
      }
      if (seq == watermark + 1) {
        watermark = seq;
      } else {
        break;
      }
    }
    return watermark;
  }
}
