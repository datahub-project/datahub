package com.datahub.telemetry;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.mixpanel.mixpanelapi.ClientDelivery;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.mockito.InOrder;
import org.testng.annotations.Test;

public class MixpanelEventSenderTest {

  private static final long WAIT_MILLIS = 5000;
  private final MessageBuilder messageBuilder = new MessageBuilder("test-token");

  private JSONObject event(String name) {
    return messageBuilder.event("client-id", name, new JSONObject());
  }

  @Test
  public void testSubmitDeliversOnBackgroundThread() throws Exception {
    MixpanelAPI api = mock(MixpanelAPI.class);
    AtomicReference<Thread> deliveryThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              deliveryThread.set(Thread.currentThread());
              return null;
            })
        .when(api)
        .deliver(any(ClientDelivery.class));
    MetricUtils metricUtils = mock(MetricUtils.class);
    MixpanelEventSender sender = new MixpanelEventSender("test", api, metricUtils);
    try {
      assertTrue(sender.submit(event("one")));
      verify(api, timeout(WAIT_MILLIS)).deliver(any(ClientDelivery.class));
      assertNotNull(deliveryThread.get());
      assertNotEquals(deliveryThread.get(), Thread.currentThread());
      verify(metricUtils, timeout(WAIT_MILLIS))
          .incrementMicrometer(
              eq(MixpanelEventSender.MESSAGES_METRIC),
              eq(1.0),
              eq(MixpanelEventSender.RESULT_TAG),
              eq(MixpanelEventSender.RESULT_SENT));
    } finally {
      sender.close();
    }
  }

  // The timeout is the non-blocking assertion: a submit that waited on the stuck worker would
  // hang until the delivery is released, which only happens in the finally block.
  @Test(timeOut = WAIT_MILLIS)
  public void testSubmitNeverBlocksAndDropsWhenQueueIsFull() throws Exception {
    // Simulate an endpoint that accepts the connection but never answers: the worker is stuck in
    // deliver() for the whole test.
    CountDownLatch deliveryStarted = new CountDownLatch(1);
    CountDownLatch releaseDelivery = new CountDownLatch(1);
    MixpanelAPI api = mock(MixpanelAPI.class);
    doAnswer(
            invocation -> {
              deliveryStarted.countDown();
              releaseDelivery.await(WAIT_MILLIS, TimeUnit.MILLISECONDS);
              return null;
            })
        .when(api)
        .deliver(any(ClientDelivery.class));
    MetricUtils metricUtils = mock(MetricUtils.class);
    MixpanelEventSender sender = new MixpanelEventSender("test", api, metricUtils, 1);
    try {
      assertTrue(sender.submit(event("in-flight")));
      assertTrue(deliveryStarted.await(WAIT_MILLIS, TimeUnit.MILLISECONDS));

      // One slot in the queue, then everything else is shed without waiting.
      assertTrue(sender.submit(event("queued")));
      assertFalse(sender.submit(event("dropped")));
      verify(metricUtils)
          .incrementMicrometer(
              eq(MixpanelEventSender.MESSAGES_METRIC),
              eq(1.0),
              eq(MixpanelEventSender.RESULT_TAG),
              eq(MixpanelEventSender.RESULT_DROPPED));
    } finally {
      releaseDelivery.countDown();
      sender.close();
    }
  }

  @Test
  public void testDeliveryFailureIsCountedAndDoesNotStopTheWorker() throws Exception {
    MixpanelAPI api = mock(MixpanelAPI.class);
    doThrow(new IOException("connect timed out")).when(api).deliver(any(ClientDelivery.class));
    MetricUtils metricUtils = mock(MetricUtils.class);
    MixpanelEventSender sender = new MixpanelEventSender("test", api, metricUtils);
    try {
      assertTrue(sender.submit(event("one")));
      verify(metricUtils, timeout(WAIT_MILLIS))
          .incrementMicrometer(
              eq(MixpanelEventSender.MESSAGES_METRIC),
              eq(1.0),
              eq(MixpanelEventSender.RESULT_TAG),
              eq(MixpanelEventSender.RESULT_FAILED));

      // The worker survives the failure and keeps draining.
      assertTrue(sender.submit(event("two")));
      verify(metricUtils, timeout(WAIT_MILLIS).times(2))
          .incrementMicrometer(
              eq(MixpanelEventSender.MESSAGES_METRIC),
              eq(1.0),
              eq(MixpanelEventSender.RESULT_TAG),
              eq(MixpanelEventSender.RESULT_FAILED));
    } finally {
      sender.close();
    }
  }

  @Test
  public void testRecoveryAfterFailureCountsSent() throws Exception {
    MixpanelAPI api = mock(MixpanelAPI.class);
    doThrow(new IOException("connect timed out"))
        .doNothing()
        .when(api)
        .deliver(any(ClientDelivery.class));
    MetricUtils metricUtils = mock(MetricUtils.class);
    MixpanelEventSender sender = new MixpanelEventSender("test", api, metricUtils);
    try {
      assertTrue(sender.submit(event("fails")));
      verify(metricUtils, timeout(WAIT_MILLIS))
          .incrementMicrometer(
              eq(MixpanelEventSender.MESSAGES_METRIC),
              eq(1.0),
              eq(MixpanelEventSender.RESULT_TAG),
              eq(MixpanelEventSender.RESULT_FAILED));

      assertTrue(sender.submit(event("recovers")));
      verify(metricUtils, timeout(WAIT_MILLIS))
          .incrementMicrometer(
              eq(MixpanelEventSender.MESSAGES_METRIC),
              eq(1.0),
              eq(MixpanelEventSender.RESULT_TAG),
              eq(MixpanelEventSender.RESULT_SENT));
      InOrder inOrder = inOrder(metricUtils);
      inOrder
          .verify(metricUtils)
          .incrementMicrometer(
              eq(MixpanelEventSender.MESSAGES_METRIC),
              eq(1.0),
              eq(MixpanelEventSender.RESULT_TAG),
              eq(MixpanelEventSender.RESULT_FAILED));
      inOrder
          .verify(metricUtils)
          .incrementMicrometer(
              eq(MixpanelEventSender.MESSAGES_METRIC),
              eq(1.0),
              eq(MixpanelEventSender.RESULT_TAG),
              eq(MixpanelEventSender.RESULT_SENT));
    } finally {
      sender.close();
    }
  }

  @Test
  public void testSubmitAfterCloseIsDropped() {
    MixpanelAPI api = mock(MixpanelAPI.class);
    MixpanelEventSender sender = new MixpanelEventSender("test", api, null);
    sender.close();
    assertFalse(sender.submit(event("late")));
  }
}
