package com.linkedin.metadata.search.elasticsearch.client.shim;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.Registry;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.nio.NHttpClientConnection;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;

/**
 * Connection manager that counts requests waiting to lease a connection. The NIO pool keeps those
 * waiters in a private list; {@code getTotalStats().getPending()} counts connections being opened,
 * not requests queued for one.
 */
public class WaitTrackingConnectionManager extends PoolingNHttpClientConnectionManager {

  private final AtomicInteger waiting = new AtomicInteger();

  public WaitTrackingConnectionManager(
      ConnectingIOReactor ioReactor, Registry<SchemeIOSessionStrategy> registry) {
    super(ioReactor, registry);
  }

  /** Requests that asked for a connection and have not yet been given one (or failed). */
  public int getWaiting() {
    return waiting.get();
  }

  @Override
  public Future<NHttpClientConnection> requestConnection(
      HttpRoute route,
      Object state,
      long connectTimeout,
      long leaseTimeout,
      TimeUnit timeUnit,
      @Nullable FutureCallback<NHttpClientConnection> callback) {
    waiting.incrementAndGet();
    AtomicBoolean done = new AtomicBoolean();
    Runnable leaseEnded =
        () -> {
          if (done.compareAndSet(false, true)) {
            waiting.decrementAndGet();
          }
        };
    try {
      return super.requestConnection(
          route,
          state,
          connectTimeout,
          leaseTimeout,
          timeUnit,
          new FutureCallback<>() {
            @Override
            public void completed(NHttpClientConnection result) {
              leaseEnded.run();
              if (callback != null) {
                callback.completed(result);
              }
            }

            @Override
            public void failed(Exception ex) {
              leaseEnded.run();
              if (callback != null) {
                callback.failed(ex);
              }
            }

            @Override
            public void cancelled() {
              leaseEnded.run();
              if (callback != null) {
                callback.cancelled();
              }
            }
          });
    } catch (RuntimeException e) {
      leaseEnded.run();
      throw e;
    }
  }
}
