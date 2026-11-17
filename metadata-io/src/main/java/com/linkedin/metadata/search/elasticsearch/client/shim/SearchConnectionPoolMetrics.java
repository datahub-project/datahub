package com.linkedin.metadata.search.elasticsearch.client.shim;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.function.ToDoubleFunction;
import javax.annotation.Nonnull;

/**
 * Metrics for the search client's HTTP connection pool. Every GMS caller (graph walks, search,
 * timeseries, bulk writes) shares this pool, so {@code waiting > 0} means requests are queued in
 * the client for a connection. The gauges are sampled at scrape time and miss short waits; the
 * lease-wait timer records every lease.
 */
public final class SearchConnectionPoolMetrics {

  public static final String CONNECTIONS_METRIC = "datahub.elasticsearch.client.connections";
  public static final String LEASE_WAIT_METRIC = "datahub.elasticsearch.client.connection.wait";

  private SearchConnectionPoolMetrics() {}

  public static void register(
      @Nonnull MeterRegistry registry,
      @Nonnull String clusterName,
      @Nonnull WaitTrackingConnectionManager connectionManager) {
    gauge(registry, clusterName, "leased", connectionManager, cm -> cm.getTotalStats().getLeased());
    gauge(
        registry,
        clusterName,
        "waiting",
        connectionManager,
        WaitTrackingConnectionManager::getWaiting);
    connectionManager.setLeaseWaitTimer(
        Timer.builder(LEASE_WAIT_METRIC)
            .tag("cluster", clusterName)
            .description("Time a request waits to lease a search client connection")
            .register(registry));
  }

  private static void gauge(
      MeterRegistry registry,
      String clusterName,
      String state,
      WaitTrackingConnectionManager connectionManager,
      ToDoubleFunction<WaitTrackingConnectionManager> value) {
    Gauge.builder(CONNECTIONS_METRIC, connectionManager, value)
        .tag("cluster", clusterName)
        .tag("state", state)
        .description("Search client HTTP connection pool, by state")
        .register(registry);
  }
}
