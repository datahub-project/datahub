package com.linkedin.metadata.utils.progress;

import static org.testng.Assert.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class ProgressTrackerTest {

  @Test
  public void testSnapshotWithoutTotal_noEtaOrPercent() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("test")
            .reportIntervalMs(60_000)
            .warmupMs(0L)
            .clock(clock)
            .build();

    tracker.record(100);
    clock.advance(10_000);

    ProgressSnapshot snap = tracker.snapshot();
    assertEquals(snap.getProcessed(), 100);
    assertNull(snap.getTotal());
    assertNull(snap.getPercentComplete());
    assertNull(snap.getEtaSeconds());
    assertTrue(snap.getMessage().contains("100 processed"));
    assertFalse(snap.getMessage().contains("ETA"));
  }

  @Test
  public void testSnapshotWithTotal_computesPercentAndEta() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("scan")
            .total(1000L)
            .reportIntervalMs(60_000)
            .warmupMs(0L)
            .clock(clock)
            .build();

    tracker.record(250);
    clock.advance(10_000); // 25/sec

    ProgressSnapshot snap = tracker.snapshot();
    assertEquals(snap.getProcessed(), 250);
    assertEquals(snap.getTotal().longValue(), 1000L);
    assertEquals(snap.getPercentComplete().intValue(), 25);
    assertNotNull(snap.getEtaSeconds());
    assertEquals(snap.getEtaSeconds().longValue(), 30L); // 750 remaining / 25/sec
    assertEquals(snap.getEtaHuman(), "30s");
    assertTrue(snap.getMessage().contains("250/1000"));
    assertTrue(snap.getMessage().contains("est. ETA 30s"));
  }

  @Test
  public void testPercentCappedAt99UntilFinished() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder().label("scan").total(100L).warmupMs(0L).clock(clock).build();

    tracker.record(99);
    clock.advance(1000);
    assertEquals(tracker.snapshot().getPercentComplete().intValue(), 99);

    tracker.record(1);
    ProgressSnapshot done = tracker.snapshot();
    assertTrue(done.isFinished());
    assertEquals(done.getPercentComplete().intValue(), 100);
    assertNull(done.getEtaSeconds());
  }

  @Test
  public void testWarmupSuppressesEtaAndFirstReport() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("scan")
            .total(1000L)
            .warmupMs(30_000L)
            .reportIntervalMs(60_000)
            .clock(clock)
            .build();

    tracker.record(100);
    clock.advance(10_000); // still in warmup

    ProgressSnapshot duringWarmup = tracker.snapshot();
    assertNull(duringWarmup.getEtaSeconds());

    List<ProgressSnapshot> reports = new ArrayList<>();
    assertFalse(tracker.maybeReport(reports::add));
    assertTrue(reports.isEmpty());

    clock.advance(25_000); // past warmup
    assertTrue(tracker.maybeReport(reports::add));
    assertEquals(reports.size(), 1);
    assertNotNull(reports.get(0).getEtaSeconds());
  }

  @Test
  public void testThrottleSuppressesRapidReports() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("scan")
            .total(10_000L)
            .warmupMs(0L)
            .reportIntervalMs(60_000)
            .clock(clock)
            .build();

    tracker.record(100);
    clock.advance(1000);

    AtomicReference<Integer> count = new AtomicReference<>(0);
    assertTrue(tracker.maybeReport(s -> count.updateAndGet(c -> c + 1)));
    assertEquals(count.get().intValue(), 1);

    clock.advance(30_000);
    tracker.record(100);
    assertFalse(tracker.maybeReport(s -> count.updateAndGet(c -> c + 1)));
    assertEquals(count.get().intValue(), 1);

    clock.advance(30_000);
    tracker.record(100);
    assertTrue(tracker.maybeReport(s -> count.updateAndGet(c -> c + 1)));
    assertEquals(count.get().intValue(), 2);
  }

  @Test
  public void testResumeInitialProcessed_rateUsesCurrentRunOnly() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("scan")
            .total(1000L)
            .initialProcessed(400)
            .warmupMs(0L)
            .clock(clock)
            .build();

    tracker.record(100);
    clock.advance(10_000);
    assertEquals(tracker.getProcessed(), 500);
    ProgressSnapshot snap = tracker.snapshot();
    assertEquals(snap.getPercentComplete().intValue(), 50);
    assertEquals(snap.getRatePerSecond(), 10.0, 0.01);
    assertEquals(snap.getEtaSeconds().longValue(), 50L);
  }

  @Test
  public void testZeroTotal_markedFinished() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder().label("scan").total(0L).warmupMs(0L).clock(clock).build();

    ProgressSnapshot snap = tracker.snapshot();
    assertEquals(snap.getTotal().longValue(), 0L);
    assertTrue(snap.isFinished());
    assertEquals(snap.getPercentComplete().intValue(), 100);
    assertNull(snap.getEtaSeconds());
  }

  @Test
  public void testBuilderDefaultWarmup() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("scan")
            .total(1000L)
            .reportIntervalMs(60_000L)
            .clock(clock)
            .build();

    tracker.record(100);
    clock.advance(10_000);

    assertNull(tracker.snapshot().getEtaSeconds());
    assertFalse(tracker.maybeReport(s -> {}));

    clock.advance(25_000);
    assertNotNull(tracker.snapshot().getEtaSeconds());
  }

  @Test
  public void testForceReportBypassesThrottle() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("scan")
            .total(100L)
            .warmupMs(0L)
            .reportIntervalMs(60_000)
            .clock(clock)
            .build();

    tracker.record(10);
    clock.advance(1000);
    List<ProgressSnapshot> reports = new ArrayList<>();
    tracker.forceReport(reports::add);
    tracker.forceReport(reports::add);
    assertEquals(reports.size(), 2);
  }

  @Test
  public void testNegativeTotal_treatedAsUnknown() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder().label("scan").total(-5L).warmupMs(0L).clock(clock).build();

    ProgressSnapshot snap = tracker.snapshot();
    assertNull(snap.getTotal());
    assertNull(snap.getPercentComplete());
    assertFalse(snap.isFinished());
  }

  @Test
  public void testNegativeWarmupUsesDefault() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("scan")
            .total(1000L)
            .warmupMs(-1L)
            .reportIntervalMs(60_000L)
            .clock(clock)
            .build();

    tracker.record(100);
    clock.advance(10_000);
    assertNull(tracker.snapshot().getEtaSeconds());
    assertFalse(tracker.maybeReport(s -> {}));
  }

  @Test
  public void testFormatDuration() {
    assertEquals(ProgressTracker.formatDuration(45), "45s");
    assertEquals(ProgressTracker.formatDuration(125), "2m 5s");
    assertEquals(ProgressTracker.formatDuration(3725), "1h 2m");
  }

  @Test
  public void testEtaHumanOnLongRemaining() {
    MutableClock clock = new MutableClock(1_000_000L);
    ProgressTracker tracker =
        ProgressTracker.builder().label("scan").total(100_000L).warmupMs(0L).clock(clock).build();

    tracker.record(100);
    clock.advance(10_000); // 10/sec → ~9990s remaining ≈ 2h 46m
    ProgressSnapshot snap = tracker.snapshot();
    assertEquals(snap.getEtaHuman(), "2h 46m");
    assertTrue(snap.getMessage().contains("est. ETA 2h 46m"));
  }

  /** Simple mutable clock for deterministic tests. */
  private static final class MutableClock extends Clock {
    private long millis;

    MutableClock(long millis) {
      this.millis = millis;
    }

    void advance(long deltaMs) {
      millis += deltaMs;
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(millis);
    }

    @Override
    public long millis() {
      return millis;
    }
  }
}
