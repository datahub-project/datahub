package utils;

import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One in-flight refresh of the product-update HTML snippet. Callers that arrive while a refresh is
 * running get the previous snippet immediately instead of waiting on the network.
 */
public final class ProductUpdateSnippetCache {
  /**
   * Returns the snippet to store. Null keeps the previous snippet, which is how a failed fetch
   * behaves. An empty string is a real result and clears the snippet.
   */
  @FunctionalInterface
  public interface Fetcher {
    @Nullable
    String fetch();
  }

  private final Fetcher fetcher;
  private final long ttlMs;
  private final Supplier<Long> clock;
  private final Object lock = new Object();
  private volatile String snippet = "";
  private volatile long fetchedAtMs;
  private boolean refreshInFlight;

  public ProductUpdateSnippetCache(@Nonnull Fetcher fetcher, long ttlMs) {
    this(fetcher, ttlMs, System::currentTimeMillis);
  }

  ProductUpdateSnippetCache(@Nonnull Fetcher fetcher, long ttlMs, @Nonnull Supplier<Long> clock) {
    this.fetcher = fetcher;
    this.ttlMs = ttlMs;
    this.clock = clock;
  }

  @Nonnull
  public String current() {
    if (isFresh(clock.get())) {
      return snippet;
    }
    boolean fetchNow = false;
    synchronized (lock) {
      if (isFresh(clock.get())) {
        return snippet;
      }
      if (!refreshInFlight) {
        refreshInFlight = true;
        fetchNow = true;
      }
    }
    if (!fetchNow) {
      return snippet;
    }
    try {
      String fetched = fetcher.fetch();
      if (fetched != null) {
        snippet = fetched;
      }
    } finally {
      synchronized (lock) {
        fetchedAtMs = clock.get();
        refreshInFlight = false;
      }
    }
    return snippet;
  }

  private boolean isFresh(long now) {
    return fetchedAtMs != 0 && now - fetchedAtMs < ttlMs;
  }
}
