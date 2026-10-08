package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UnknownDataGuardTest {

  private EntityRegistry registry;
  private MetricUtils metricUtils;
  private Logger log;
  private AtomicLong clock;
  private UnknownDataGuard guard;

  @BeforeMethod
  public void setUp() {
    UnknownDataGuard.resetLogWindows();
    EntitySpec dataset = mock(EntitySpec.class);
    when(dataset.getAspectSpec("status")).thenReturn(mock(AspectSpec.class));
    registry = mock(EntityRegistry.class);
    when(registry.findEntitySpec(anyString())).thenReturn(Optional.empty());
    when(registry.findEntitySpec("dataset")).thenReturn(Optional.of(dataset));
    metricUtils = mock(MetricUtils.class);
    log = mock(Logger.class);
    clock = new AtomicLong(1_000L);
    guard = UnknownDataGuard.forSite(UnknownDataGuardTest.class, "MCL", log, clock::get);
  }

  @Test
  public void testKnownAndMalformedDataIsAdmittedWithoutReporting() {
    assertTrue(guard.admit(registry, Optional.of(metricUtils), "dataset", "status", "urn"));
    assertTrue(guard.admit(registry, Optional.of(metricUtils), "dataset", null, "urn"));
    assertTrue(guard.admit(registry, Optional.of(metricUtils), null, "status", "urn"));
    assertTrue(guard.admitUrn(registry, Optional.of(metricUtils), "not-a-urn", "status"));

    verify(metricUtils, never()).increment(any(Class.class), anyString(), anyDouble());
    verifyNoMoreInteractions(log);
  }

  @Test
  public void testEverySkipIsCountedButLoggedOncePerKeyPerInterval() {
    for (int i = 0; i < 5; i++) {
      assertFalse(
          guard.admit(registry, Optional.of(metricUtils), "dataset", "aspectFromNewerBuild", i));
    }
    assertFalse(
        guard.admitUrn(registry, Optional.of(metricUtils), "urn:li:entityFromNewerBuild:x", null));

    verify(metricUtils, times(6))
        .increment(UnknownDataGuardTest.class, UnknownDataGuard.SKIPPED_METRIC, 1);
    // One WARN per key within the interval; the other four unknown-aspect skips are only counted.
    verify(log, times(2)).warn(anyString(), any(), any(), any());
    verifyNoMoreInteractions(log);
  }

  @Test
  public void testNextReportAfterTheIntervalCarriesTheSuppressedCount() {
    for (int i = 0; i < 3; i++) {
      guard.admit(registry, Optional.empty(), "dataset", "aspectFromNewerBuild", i);
    }
    clock.addAndGet(TimeUnit.MINUTES.toNanos(1));
    guard.admit(registry, Optional.empty(), "dataset", "aspectFromNewerBuild", 3);

    // The first skip, then one report with the two skips suppressed in between.
    verify(log).warn(anyString(), any(), any(), any());
    verify(log).warn(anyString(), any(), any(), any(), any());
    verifyNoMoreInteractions(log);
  }

  @Test
  public void testDifferentKindsOfDataAtOneSiteHaveSeparateWindows() {
    UnknownDataGuard other =
        UnknownDataGuard.forSite(UnknownDataGuardTest.class, "rollback row", log, clock::get);

    guard.admit(registry, Optional.empty(), "dataset", "aspectFromNewerBuild", "a");
    other.admit(registry, Optional.empty(), "dataset", "aspectFromNewerBuild", "a");

    verify(log, times(2)).warn(anyString(), any(), any(), any());
  }
}
