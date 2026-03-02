package io.datahubproject.test.search;

import java.util.Objects;
import javax.annotation.Nullable;
import org.opensearch.OpenSearchException;

/**
 * Immutable spec for fault injection in {@link FaultInjectingSearchClientShim}. Describes how many
 * times to fail {@code count()} and/or {@code createIndex()} before delegating to the real shim.
 */
public final class FaultSpec {

  public enum CountExceptionType {
    SOCKET_TIMEOUT,
    OPENSEARCH
  }

  private final int countFailures;
  private final CountExceptionType countExceptionType;
  private final int createIndexFailures;

  private FaultSpec(
      int countFailures, CountExceptionType countExceptionType, int createIndexFailures) {
    this.countFailures = Math.max(0, countFailures);
    this.countExceptionType = Objects.requireNonNull(countExceptionType);
    this.createIndexFailures = Math.max(0, createIndexFailures);
  }

  public int getCountFailures() {
    return countFailures;
  }

  public CountExceptionType getCountExceptionType() {
    return countExceptionType;
  }

  public int getCreateIndexFailures() {
    return createIndexFailures;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private int countFailures = 0;
    private CountExceptionType countExceptionType = CountExceptionType.SOCKET_TIMEOUT;
    private int createIndexFailures = 0;

    public Builder countFailures(int countFailures) {
      this.countFailures = countFailures;
      return this;
    }

    public Builder countExceptionType(CountExceptionType countExceptionType) {
      this.countExceptionType = countExceptionType;
      return this;
    }

    public Builder createIndexFailures(int createIndexFailures) {
      this.createIndexFailures = createIndexFailures;
      return this;
    }

    public FaultSpec build() {
      return new FaultSpec(countFailures, countExceptionType, createIndexFailures);
    }
  }

  /** ThreadLocal holder for the current test's fault spec. Tests set this in @BeforeMethod. */
  public static final class Holder {
    private static final ThreadLocal<FaultSpec> CURRENT = new ThreadLocal<>();

    @Nullable
    public static FaultSpec get() {
      return CURRENT.get();
    }

    public static void set(@Nullable FaultSpec spec) {
      if (spec == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(spec);
      }
    }

    public static void clear() {
      CURRENT.remove();
    }

    private Holder() {}
  }

  /**
   * Create an exception for count() failures per this spec. Caller is responsible for using the
   * right failure index (e.g. after incrementing a counter).
   */
  public Exception createCountException() {
    switch (countExceptionType) {
      case SOCKET_TIMEOUT:
        return new java.net.SocketTimeoutException("simulated timeout for test");
      case OPENSEARCH:
        return new OpenSearchException("simulated OpenSearch failure for test");
      default:
        return new java.net.SocketTimeoutException("simulated timeout for test");
    }
  }
}
