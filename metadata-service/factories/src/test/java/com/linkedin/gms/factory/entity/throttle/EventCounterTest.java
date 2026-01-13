package com.linkedin.gms.factory.entity.throttle;

import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EventCounterTest {

  private EventCounter eventCounter;

  @BeforeMethod
  public void setup() {
    eventCounter = new EventCounter();
  }

  @Test
  public void testRecordEvents_SingleEvent() {
    eventCounter.recordEvents(10);

    assertEquals(
        eventCounter.numEventsInActivationInterval.get(),
        10,
        "Should record 10 events in activation interval");
    assertEquals(eventCounter.getTotalCount(60), 10, "Should have 10 total events");
  }

  @Test
  public void testRecordEvents_MultipleEvents() {
    eventCounter.recordEvents(5);
    eventCounter.recordEvents(10);
    eventCounter.recordEvents(15);

    assertEquals(
        eventCounter.numEventsInActivationInterval.get(),
        30,
        "Should accumulate events in activation interval");
    assertEquals(eventCounter.getTotalCount(60), 30, "Should have 30 total events");
  }

  @Test
  public void testGetTotalCount_PrunesOldEvents() throws InterruptedException {
    eventCounter.recordEvents(10);
    Thread.sleep(1100);
    eventCounter.recordEvents(20);

    assertEquals(eventCounter.getTotalCount(1), 20, "Should only count events within 1 second");
  }

  @Test
  public void testGetTotalCount_IncludesAllRecentEvents() throws InterruptedException {
    eventCounter.recordEvents(10);
    Thread.sleep(100);
    eventCounter.recordEvents(20);

    assertEquals(
        eventCounter.getTotalCount(60),
        30,
        "Should count all events within 60 seconds (both batches)");
  }

  @Test
  public void testGetTotalCount_AllEventsPruned() throws InterruptedException {
    eventCounter.recordEvents(10);
    Thread.sleep(1100);

    assertEquals(
        eventCounter.getTotalCount(1), 0, "Should return 0 when all events are older than window");
  }

  @Test
  public void testGetTotalCount_EmptyQueue() {
    assertEquals(eventCounter.getTotalCount(60), 0, "Should return 0 when no events recorded");
  }

  @Test
  public void testGetMsUntilNumEventsElapsed_SingleBatch() {
    eventCounter.recordEvents(100);

    long msElapsed = eventCounter.getMsUntilNumEventsElapsed(50);

    assertTrue(msElapsed >= 0, "Time elapsed should be non-negative");
    assertTrue(msElapsed < 100, "Time elapsed should be recent (less than 100ms)");
  }

  @Test
  public void testGetMsUntilNumEventsElapsed_MultipleBatches() throws InterruptedException {
    eventCounter.recordEvents(30);
    Thread.sleep(50);
    eventCounter.recordEvents(40);
    Thread.sleep(50);
    eventCounter.recordEvents(30);

    long msElapsed = eventCounter.getMsUntilNumEventsElapsed(50);

    assertTrue(
        msElapsed >= 50, "Time should be at least 50ms since we need to go back to second batch");
    assertTrue(msElapsed < 150, "Time should be less than 150ms (50 + 50 + some processing time)");
  }

  @Test
  public void testGetMsUntilNumEventsElapsed_ExceedsAllEvents() throws InterruptedException {
    eventCounter.recordEvents(20);
    Thread.sleep(100);
    eventCounter.recordEvents(30);

    long msElapsed = eventCounter.getMsUntilNumEventsElapsed(100);

    assertTrue(
        msElapsed >= 0, "Should return time from oldest event when requested events exceed total");
  }

  @Test
  public void testGetMsUntilNumEventsElapsed_ZeroEvents() {
    eventCounter.recordEvents(50);

    long msElapsed = eventCounter.getMsUntilNumEventsElapsed(0);

    assertTrue(msElapsed >= 0, "Should handle zero events gracefully");
  }

  @Test
  public void testNumEventsInActivationInterval_IndependentOfPruning() throws InterruptedException {
    eventCounter.recordEvents(100);

    assertEquals(
        eventCounter.numEventsInActivationInterval.get(),
        100,
        "Activation interval count should be 100");

    Thread.sleep(100);
    eventCounter.getTotalCount(1);

    assertEquals(
        eventCounter.numEventsInActivationInterval.get(),
        100,
        "Activation interval count should remain 100 even after pruning");
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    Thread thread1 =
        new Thread(
            () -> {
              for (int i = 0; i < 100; i++) {
                eventCounter.recordEvents(1);
              }
            });

    Thread thread2 =
        new Thread(
            () -> {
              for (int i = 0; i < 100; i++) {
                eventCounter.recordEvents(1);
              }
            });

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    assertEquals(
        eventCounter.numEventsInActivationInterval.get(),
        200,
        "Should handle concurrent event recording");
    assertEquals(eventCounter.getTotalCount(60), 200, "Total count should be 200");
  }

  @Test
  public void testGetTotalCount_MultipleCalls_ConsistentPruning() throws InterruptedException {
    eventCounter.recordEvents(50);
    Thread.sleep(1100);
    eventCounter.recordEvents(30);

    int count1 = eventCounter.getTotalCount(1);
    int count2 = eventCounter.getTotalCount(1);

    assertEquals(count1, 30, "First call should return only recent events");
    assertEquals(count2, 30, "Second call should return same count (pruning already happened)");
  }
}
