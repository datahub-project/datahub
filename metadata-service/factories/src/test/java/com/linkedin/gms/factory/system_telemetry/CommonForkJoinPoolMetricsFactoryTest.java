package com.linkedin.gms.factory.system_telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testng.annotations.Test;

public class CommonForkJoinPoolMetricsFactoryTest {

  private static final String POOL_NAME = "fork-join-common";

  @Test
  public void testCommonForkJoinPoolMetricsRegistered() {
    MeterRegistry registry = new SimpleMeterRegistry();

    new CommonForkJoinPoolMetricsFactory(registry);

    // executor.queued is the ForkJoinPool analog of the ThreadPoolExecutor's executor.queued.tasks
    assertThat(registry.find("executor.queued").tag("name", POOL_NAME).gauge()).isNotNull();
    // executor.steals is unique to ForkJoinPool instrumentation, confirming the FJP branch ran
    assertThat(registry.find("executor.steals").tag("name", POOL_NAME).functionCounter())
        .isNotNull();
  }
}
