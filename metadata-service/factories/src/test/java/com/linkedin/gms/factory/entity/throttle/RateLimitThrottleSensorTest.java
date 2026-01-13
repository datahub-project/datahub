package com.linkedin.gms.factory.entity.throttle;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.linkedin.gms.factory.common.CacheConfig;
import com.linkedin.metadata.dao.throttle.ThrottleControl;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RateLimitThrottleSensorTest {

  private RateLimitThrottleSensor sensor;
  private HazelcastInstance mockHazelcast;
  private IMap mockThrottleMap;

  @BeforeMethod
  public void setup() {
    mockHazelcast = mock(HazelcastInstance.class);
    mockThrottleMap = mock(IMap.class);
    when(mockHazelcast.getMap(CacheConfig.IS_RATE_LIMIT_THROTTLED)).thenReturn(mockThrottleMap);
  }

  @Test
  public void testCheckThrottle_BelowActivationQuota_NoThrottle() throws InterruptedException {
    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .build();

    AtomicBoolean callbackInvoked = new AtomicBoolean(false);
    sensor.addCallback(
        event -> {
          callbackInvoked.set(true);
          return ThrottleControl.NONE;
        });

    sensor.eventRecorder().accept(50);

    sensor.checkThrottle();

    assertFalse(
        callbackInvoked.get(), "Callback should not be invoked when below activation quota");
  }

  @Test
  public void testCheckThrottle_AboveActivationQuota_BelowRateLimit_NoThrottle()
      throws InterruptedException {
    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(150)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .build();

    AtomicBoolean callbackInvoked = new AtomicBoolean(false);
    sensor.addCallback(
        event -> {
          callbackInvoked.set(true);
          return ThrottleControl.NONE;
        });

    sensor.eventRecorder().accept(120);

    sensor.checkThrottle();

    assertFalse(
        callbackInvoked.get(),
        "Callback should not be invoked when above activation but below rate limit");
  }

  @Test
  public void testCheckThrottle_AboveBothThresholds_ShouldThrottle() throws InterruptedException {
    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .hazelcastInstance(mockHazelcast)
            .build();

    AtomicInteger callbackInvocationCount = new AtomicInteger(0);
    sensor.addCallback(
        event -> {
          callbackInvocationCount.incrementAndGet();
          return ThrottleControl.NONE;
        });

    sensor.eventRecorder().accept(120);

    sensor.checkThrottle();

    assertEquals(
        callbackInvocationCount.get(), 1, "Callback should be invoked once when throttling");
    verify(mockThrottleMap).put("status", true);
  }

  @Test
  public void testCheckThrottle_AlreadyChecking_ShouldSkip() throws InterruptedException {
    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .build();

    AtomicInteger callbackInvocationCount = new AtomicInteger(0);
    sensor.addCallback(
        event -> {
          callbackInvocationCount.incrementAndGet();
          try {
            sensor.checkThrottle();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return ThrottleControl.NONE;
        });

    sensor.eventRecorder().accept(120);

    sensor.checkThrottle();

    assertEquals(
        callbackInvocationCount.get(),
        1,
        "Nested checkThrottle should be skipped when already checking");
  }

  @Test
  public void testSetIsThrottled_WithHazelcast() throws InterruptedException {
    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .hazelcastInstance(mockHazelcast)
            .build();

    sensor.addCallback(event -> ThrottleControl.NONE);

    sensor.eventRecorder().accept(120);

    sensor.checkThrottle();

    verify(mockThrottleMap).put("status", true);
  }

  @Test
  public void testSetIsThrottled_WithoutHazelcast_LocalFallback() {
    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .build();

    sensor.setIsThrottled(true);

    assertTrue(sensor.isLocallyThrottled.get(), "Should set local throttle flag when no Hazelcast");
  }

  @Test
  public void testSetIsThrottled_HazelcastError_FallbackToLocal() {
    when(mockHazelcast.getMap(CacheConfig.IS_RATE_LIMIT_THROTTLED))
        .thenThrow(new RuntimeException("Hazelcast error"));

    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .hazelcastInstance(mockHazelcast)
            .build();

    sensor.setIsThrottled(true);

    assertTrue(
        sensor.isLocallyThrottled.get(),
        "Should fall back to local throttle flag on Hazelcast error");
  }

  @Test
  public void testCheckThrottle_MultipleCallbacks() throws InterruptedException {
    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .build();

    AtomicInteger callback1Count = new AtomicInteger(0);
    AtomicInteger callback2Count = new AtomicInteger(0);

    sensor.addCallback(
        event -> {
          callback1Count.incrementAndGet();
          return ThrottleControl.NONE;
        });
    sensor.addCallback(
        event -> {
          callback2Count.incrementAndGet();
          return ThrottleControl.NONE;
        });

    sensor.eventRecorder().accept(120);

    sensor.checkThrottle();

    assertEquals(callback1Count.get(), 1, "First callback should be invoked");
    assertEquals(callback2Count.get(), 1, "Second callback should be invoked");
  }

  @Test
  public void testEventRecorder_ReturnsConsumer() {
    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .build();

    assertNotNull(sensor.eventRecorder(), "Event recorder should not be null");
  }

  @Test
  public void testGetIsThrottled() throws InterruptedException {
    when(mockThrottleMap.get("status")).thenReturn(null).thenReturn(true).thenReturn(false);

    sensor =
        RateLimitThrottleSensor.builder()
            .activationQuota(100)
            .activationIntervalMinutes(5)
            .rateLimitIntervalSeconds(60)
            .limitPerInterval(50)
            .updateIntervalMs(1000)
            .jitterRatio(0.1)
            .hazelcastInstance(mockHazelcast)
            .build();

    assertFalse(sensor.getIsThrottled(), "Should return false when status is null");

    sensor.addCallback(event -> ThrottleControl.NONE);
    sensor.eventRecorder().accept(120);
    sensor.checkThrottle();

    assertTrue(sensor.getIsThrottled(), "Should return true after throttling is activated");

    when(mockThrottleMap.get("status")).thenReturn(false);
    assertFalse(sensor.getIsThrottled(), "Should return false when status is explicitly false");
  }
}
