package com.linkedin.gms.factory.clientwrite;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Reports a tracking failure at WARN, at most once per interval, with the number of failures
 * suppressed since the last report. A failure that repeats on every write (a meter name collision,
 * say) stays visible at the default log level without logging once per request.
 */
@Slf4j
public class ClientWriteTrackingErrors {
  static final long REPORT_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

  @Nonnull private final String source;
  @Nonnull private final LongSupplier clockNanos;
  private final AtomicLong nextReportNanos;
  private final AtomicLong suppressed = new AtomicLong();

  public ClientWriteTrackingErrors(@Nonnull String source) {
    this(source, System::nanoTime);
  }

  ClientWriteTrackingErrors(@Nonnull String source, @Nonnull LongSupplier clockNanos) {
    this.source = source;
    this.clockNanos = clockNanos;
    this.nextReportNanos = new AtomicLong(clockNanos.getAsLong());
  }

  /** Logs the failure, or counts it when one was already logged within the interval. */
  public void report(@Nonnull RuntimeException e) {
    long now = clockNanos.getAsLong();
    long next = nextReportNanos.get();
    if (now - next < 0 || !nextReportNanos.compareAndSet(next, now + REPORT_INTERVAL_NANOS)) {
      suppressed.incrementAndGet();
      return;
    }
    log.warn(
        "Client write tracking failed in {}; the request continues unaffected ({} more failures"
            + " since the last report, logged at most once per minute)",
        source,
        suppressed.getAndSet(0),
        e);
  }
}
