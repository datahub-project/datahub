package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs each of a process's upgrade-row polls from startup, once a minute, for as long as the
 * process runs: the mirror-live marker of every family it migrates, and its report as a live
 * writer. The copy step waits for the markers before it reads the source, and nothing else
 * guarantees a process writes a given family soon after it starts (GMS never writes usage events,
 * and a standalone GMS writes timeseries rows only for UI edits), so they are recorded on a clock
 * rather than on a first write; the same poll keeps the delete log in step with the copy's status
 * and restores markers a reset row has lost. The writer report is what tells the copy that the
 * previous rollout's pods have stopped writing.
 */
@Slf4j
public final class MirrorLiveMarkerScheduler implements AutoCloseable {
  private final List<UpgradeRowPoll> polls;
  private final ScheduledExecutorService executor;

  public MirrorLiveMarkerScheduler(@Nonnull final List<UpgradeRowPoll> polls) {
    this.polls = List.copyOf(polls);
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              final Thread thread = new Thread(runnable, "cluster-migration-mirror-markers");
              thread.setDaemon(true);
              return thread;
            });
  }

  @Nonnull
  public List<UpgradeRowPoll> getPolls() {
    return polls;
  }

  /** Starts the periodic polls; returns at once. */
  public void start() {
    if (polls.isEmpty()) {
      return;
    }
    executor.scheduleWithFixedDelay(
        this::pollAll, 0, UpgradeRowMirrorLiveMarker.POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  void pollAll() {
    for (UpgradeRowPoll poll : polls) {
      try {
        poll.poll();
      } catch (RuntimeException e) {
        log.warn("Upgrade-row poll for {} failed", poll.getUpgradeUrn(), e);
      }
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
