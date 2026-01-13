package com.linkedin.gms.factory.entity.throttle;

import com.linkedin.util.Pair;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;

class EventCounter {
  final AtomicLong numEventsInActivationInterval = new AtomicLong(0);
  private final Deque<Pair<Instant, Integer>> actions = new ArrayDeque<>();
  private int totalCount;

  synchronized int getTotalCount(int rateLimitIntervalSeconds) {
    Instant now = Instant.now();
    pruneEvents(now.minusSeconds(rateLimitIntervalSeconds));
    return totalCount;
  }

  synchronized long getMsUntilNumEventsElapsed(int numEvents) {
    Instant now = Instant.now();
    for (Pair<Instant, Integer> action : actions) {
      numEvents -= action.getValue();
      if (numEvents <= 0) {
        return Duration.between(action.getKey(), now).toMillis();
      }
    }
    return Duration.between(actions.getLast().getKey(), now).toMillis();
  }

  synchronized void recordEvents(int count) {
    Instant now = Instant.now();
    numEventsInActivationInterval.addAndGet(count);
    actions.addLast(new Pair<>(now, count));
    totalCount += count;
  }

  private void pruneEvents(Instant cutoff) {
    // Remove expired entries from the front
    while (!actions.isEmpty() && actions.peekFirst().getKey().isBefore(cutoff)) {
      Pair<Instant, Integer> removed = actions.removeFirst();
      totalCount -= removed.getValue();
    }
  }
}
