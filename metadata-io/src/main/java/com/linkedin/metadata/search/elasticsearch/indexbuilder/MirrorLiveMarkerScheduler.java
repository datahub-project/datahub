package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs {@link UpgradeRowMirrorLiveMarker#poll()} for each of a process's markers from startup, once
 * a minute, for as long as the process runs. The copy step waits for the markers before it reads
 * the source, and nothing else guarantees a process writes a given family soon after it starts (GMS
 * never writes usage events, and a standalone GMS writes timeseries rows only for UI edits), so
 * they are recorded on a clock rather than on a first write; the same poll keeps the delete log in
 * step with the copy's status and restores markers a reset row has lost.
 */
@Slf4j
public final class MirrorLiveMarkerScheduler implements AutoCloseable {
  private final List<UpgradeRowMirrorLiveMarker> markers;
  private final ScheduledExecutorService executor;

  public MirrorLiveMarkerScheduler(@Nonnull final List<UpgradeRowMirrorLiveMarker> markers) {
    this.markers = List.copyOf(markers);
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              final Thread thread = new Thread(runnable, "cluster-migration-mirror-markers");
              thread.setDaemon(true);
              return thread;
            });
  }

  @Nonnull
  public List<UpgradeRowMirrorLiveMarker> getMarkers() {
    return markers;
  }

  /** Starts the periodic polls; returns at once. */
  public void start() {
    if (markers.isEmpty()) {
      return;
    }
    executor.scheduleWithFixedDelay(
        this::pollAll, 0, UpgradeRowMirrorLiveMarker.POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  void pollAll() {
    for (UpgradeRowMirrorLiveMarker marker : markers) {
      try {
        marker.poll();
      } catch (RuntimeException e) {
        log.warn("Mirror-live marker poll for {} failed", marker.getUpgradeUrn(), e);
      }
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
