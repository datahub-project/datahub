package com.datahub.telemetry;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.mixpanel.mixpanelapi.ClientDelivery;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import com.mixpanel.mixpanelapi.MixpanelMessageException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

/**
 * Delivers Mixpanel messages from a single background thread so that no caller ever waits on the
 * Mixpanel endpoint.
 *
 * <p>The endpoint DataHub reports to has gone down repeatedly, and when it does it drops packets
 * rather than refusing connections. {@link MixpanelAPI} sends synchronously over {@code
 * HttpURLConnection} with a 2s connect and 10s read timeout, so a direct call from a request thread
 * would stall that thread for the full timeout on every event. Here {@link #submit} only enqueues:
 * it never blocks, and it drops the message when the queue is full. Telemetry is best effort, so
 * losing events during an outage is acceptable; slowing GMS down is not.
 *
 * <p>Delivery outcomes are counted under {@value #MESSAGES_METRIC} with a {@value #RESULT_TAG} tag
 * of {@value #RESULT_SENT}, {@value #RESULT_FAILED} or {@value #RESULT_DROPPED}, so an outage shows
 * up as a failure rate rather than as unexplained latency.
 */
@Slf4j
public class MixpanelEventSender implements AutoCloseable {

  public static final String MESSAGES_METRIC = "datahub.telemetry.mixpanel.messages";
  public static final String RESULT_TAG = "result";
  public static final String RESULT_SENT = "sent";
  public static final String RESULT_FAILED = "failed";
  public static final String RESULT_DROPPED = "dropped";

  /**
   * Bounds memory while the endpoint is unreachable. A busy instance emits a few UI events per
   * second and the worker clears a healthy queue far faster than that, so this only fills during an
   * outage, where dropping is the intended behavior.
   */
  private static final int DEFAULT_QUEUE_CAPACITY = 1000;

  /** Mixpanel accepts at most 50 messages per request. */
  private static final int MAX_BATCH_SIZE = 50;

  private static final long SHUTDOWN_JOIN_MILLIS = 1000L;

  private final String name;
  private final MixpanelAPI mixpanelAPI;
  @Nullable private final MetricUtils metricUtils;
  private final BlockingQueue<JSONObject> queue;
  private final Thread worker;
  private volatile boolean closed = false;

  // Only touched by the worker thread. Failures are logged on the transition into and out of a
  // failing state, not on every batch, so a multi-day outage does not flood the log.
  private boolean deliveryFailing = false;

  /**
   * @param name identifies this sender in log lines and its thread name. GMS runs more than one (UI
   *     event tracking and the daily report), and each reports its own delivery state.
   */
  public MixpanelEventSender(
      @Nonnull final String name,
      @Nonnull final MixpanelAPI mixpanelAPI,
      @Nullable final MetricUtils metricUtils) {
    this(name, mixpanelAPI, metricUtils, DEFAULT_QUEUE_CAPACITY);
  }

  @VisibleForTesting
  MixpanelEventSender(
      @Nonnull final String name,
      @Nonnull final MixpanelAPI mixpanelAPI,
      @Nullable final MetricUtils metricUtils,
      final int queueCapacity) {
    this.name = name;
    this.mixpanelAPI = mixpanelAPI;
    this.metricUtils = metricUtils;
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
    this.worker = new Thread(this::runLoop, "mixpanel-telemetry-sender-" + name);
    this.worker.setDaemon(true);
    this.worker.start();
  }

  /**
   * Queue a message built by {@link com.mixpanel.mixpanelapi.MessageBuilder} for delivery. Never
   * blocks.
   *
   * @return true if the message was queued, false if it was dropped because the queue is full or
   *     the sender is closed
   */
  public boolean submit(@Nonnull final JSONObject message) {
    if (!closed && queue.offer(message)) {
      return true;
    }
    count(RESULT_DROPPED, 1);
    log.debug("Mixpanel sender {}: queue full or closed, dropping message", name);
    return false;
  }

  @VisibleForTesting
  int queuedCount() {
    return queue.size();
  }

  private void runLoop() {
    while (!closed) {
      final JSONObject first;
      try {
        first = queue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      final List<JSONObject> batch = new ArrayList<>(MAX_BATCH_SIZE);
      batch.add(first);
      queue.drainTo(batch, MAX_BATCH_SIZE - 1);
      deliver(batch);
    }
  }

  private void deliver(@Nonnull final List<JSONObject> batch) {
    final ClientDelivery delivery = new ClientDelivery();
    int accepted = 0;
    for (final JSONObject message : batch) {
      try {
        delivery.addMessage(message);
        accepted++;
      } catch (MixpanelMessageException e) {
        count(RESULT_FAILED, 1);
        log.debug("Skipping malformed Mixpanel message", e);
      }
    }
    if (accepted == 0) {
      return;
    }
    try {
      mixpanelAPI.deliver(delivery);
      count(RESULT_SENT, accepted);
      if (deliveryFailing) {
        deliveryFailing = false;
        log.info("Mixpanel sender {}: delivery recovered", name);
      }
    } catch (Throwable e) {
      // Catch everything, Errors included: this thread is the queue's only consumer, so if it died
      // every later event would be dropped without a trace. Telemetry must never take a thread
      // down with it.
      count(RESULT_FAILED, accepted);
      if (!deliveryFailing) {
        deliveryFailing = true;
        log.warn(
            "Mixpanel sender {}: delivery failing; events will be dropped until it recovers: {}",
            name,
            e.getMessage());
      } else {
        log.debug("Mixpanel sender {}: delivery still failing", name, e);
      }
    }
  }

  private void count(@Nonnull final String result, final int amount) {
    if (metricUtils != null) {
      metricUtils.incrementMicrometer(MESSAGES_METRIC, amount, RESULT_TAG, result);
    }
  }

  /**
   * Stops the worker. Anything still queued, or a send in progress against an unresponsive
   * endpoint, is abandoned after a short wait: the thread is a daemon, so it cannot hold the JVM
   * open.
   */
  @Override
  public void close() {
    closed = true;
    worker.interrupt();
    try {
      worker.join(SHUTDOWN_JOIN_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
