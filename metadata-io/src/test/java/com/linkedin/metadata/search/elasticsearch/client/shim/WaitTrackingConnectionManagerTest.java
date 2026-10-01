package com.linkedin.metadata.search.elasticsearch.client.shim;

import static org.testng.Assert.assertEquals;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.testng.annotations.Test;

public class WaitTrackingConnectionManagerTest {

  @Test
  public void countsRequestsUntilTheLeaseEnds() throws Exception {
    // The reactor is never started, so lease requests stay queued until cancelled.
    DefaultConnectingIOReactor ioReactor =
        new DefaultConnectingIOReactor(IOReactorConfig.custom().setIoThreadCount(1).build());
    WaitTrackingConnectionManager manager =
        new WaitTrackingConnectionManager(
            ioReactor,
            RegistryBuilder.<SchemeIOSessionStrategy>create()
                .register("http", NoopIOSessionStrategy.INSTANCE)
                .build());
    HttpRoute route = new HttpRoute(new HttpHost("localhost", 9200));
    try {
      Future<NHttpClientConnection> first =
          manager.requestConnection(route, null, 1000, 0, TimeUnit.MILLISECONDS, null);
      Future<NHttpClientConnection> second =
          manager.requestConnection(route, null, 1000, 0, TimeUnit.MILLISECONDS, null);
      assertEquals(manager.getWaiting(), 2);

      first.cancel(true);
      assertEquals(manager.getWaiting(), 1);
      first.cancel(true); // a second cancel must not double-decrement
      assertEquals(manager.getWaiting(), 1);

      second.cancel(true);
      assertEquals(manager.getWaiting(), 0);
    } finally {
      manager.shutdown();
    }
  }
}
