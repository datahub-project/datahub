package com.linkedin.metadata.queue;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PgQueueContiguousOffsetTest {

  @Test
  public void advanceWatermark_emptyAcks_returnsCurrent() {
    Assert.assertEquals(PgQueueContiguousOffset.advanceWatermark(5L, List.of()), 5L);
  }

  @Test
  public void advanceWatermark_contiguousRun() {
    Assert.assertEquals(PgQueueContiguousOffset.advanceWatermark(0L, List.of(1L, 2L, 3L)), 3L);
  }

  @Test
  public void advanceWatermark_stopsAtGap() {
    Assert.assertEquals(PgQueueContiguousOffset.advanceWatermark(0L, List.of(2L, 3L)), 0L);
    Assert.assertEquals(PgQueueContiguousOffset.advanceWatermark(0L, List.of(1L, 3L)), 1L);
  }

  @Test
  public void advanceWatermark_outOfOrderAcks() {
    Assert.assertEquals(PgQueueContiguousOffset.advanceWatermark(0L, List.of(3L, 1L, 2L)), 3L);
  }

  @Test
  public void advanceWatermark_idempotentReAck() {
    Assert.assertEquals(PgQueueContiguousOffset.advanceWatermark(5L, List.of(3L, 5L, 6L)), 6L);
  }
}
