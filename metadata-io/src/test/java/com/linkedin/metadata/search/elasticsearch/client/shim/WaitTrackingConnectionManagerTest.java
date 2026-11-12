package com.linkedin.metadata.search.elasticsearch.client.shim;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.testng.annotations.Test;

public class WaitTrackingConnectionManagerTest {

  private static WaitTrackingConnectionManager newManager() throws Exception {
    return new WaitTrackingConnectionManager(
        new DefaultConnectingIOReactor(IOReactorConfig.custom().setIoThreadCount(1).build()),
        RegistryBuilder.<SchemeIOSessionStrategy>create()
            .register("http", NoopIOSessionStrategy.INSTANCE)
            .build());
  }

  private static Timer attachTimer(WaitTrackingConnectionManager manager) {
    Timer timer = Timer.builder("lease.wait").register(new SimpleMeterRegistry());
    manager.setLeaseWaitTimer(timer);
    return timer;
  }

  private static HttpRoute loopbackRoute(int port) {
    return new HttpRoute(new HttpHost(InetAddress.getLoopbackAddress(), port, "http"));
  }

  // The future completes before its callback runs, so the counter can lag get() briefly.
  private static void awaitWaiting(WaitTrackingConnectionManager manager, int expected)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (manager.getWaiting() != expected && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertEquals(manager.getWaiting(), expected);
  }

  @Test
  public void countsRequestsUntilTheLeaseEnds() throws Exception {
    // The reactor is never started, so lease requests stay queued until cancelled.
    WaitTrackingConnectionManager manager = newManager();
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

  @Test(timeOut = 10000)
  public void stopsCountingOnceALeaseIsGranted() throws Exception {
    WaitTrackingConnectionManager manager = newManager();
    manager.setDefaultMaxPerRoute(1);
    Timer leaseWait = attachTimer(manager);
    try (ServerSocket server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        CloseableHttpAsyncClient client =
            HttpAsyncClients.custom().setConnectionManager(manager).build()) {
      client.start(); // runs the IO reactor
      HttpRoute route = loopbackRoute(server.getLocalPort());

      Future<NHttpClientConnection> first =
          manager.requestConnection(route, null, 5000, 0, TimeUnit.MILLISECONDS, null);
      NHttpClientConnection conn = first.get(5, TimeUnit.SECONDS);
      assertNotNull(conn);
      awaitWaiting(manager, 0);

      // The only connection is leased, so the next request has to wait for it.
      Future<NHttpClientConnection> second =
          manager.requestConnection(route, null, 5000, 0, TimeUnit.MILLISECONDS, null);
      assertEquals(manager.getWaiting(), 1);

      Thread.sleep(50);
      manager.releaseConnection(conn, null, 1, TimeUnit.MINUTES);
      assertNotNull(second.get(5, TimeUnit.SECONDS));
      awaitWaiting(manager, 0);

      // Both leases are timed, and the second one includes the wait for the release.
      assertEquals(leaseWait.count(), 2);
      assertTrue(leaseWait.max(TimeUnit.MILLISECONDS) >= 50);
    }
  }

  @Test(timeOut = 10000)
  public void stopsCountingWhenTheConnectFails() throws Exception {
    int closedPort;
    try (ServerSocket probe = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      closedPort = probe.getLocalPort();
    }
    WaitTrackingConnectionManager manager = newManager();
    Timer leaseWait = attachTimer(manager);
    try (CloseableHttpAsyncClient client =
        HttpAsyncClients.custom().setConnectionManager(manager).build()) {
      client.start();
      Future<NHttpClientConnection> lease =
          manager.requestConnection(
              loopbackRoute(closedPort), null, 5000, 0, TimeUnit.MILLISECONDS, null);
      expectThrows(ExecutionException.class, () -> lease.get(5, TimeUnit.SECONDS));
      awaitWaiting(manager, 0);
      assertEquals(leaseWait.count(), 1);
    }
  }
}
