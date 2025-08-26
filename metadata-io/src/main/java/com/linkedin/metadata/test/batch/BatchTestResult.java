package com.linkedin.metadata.test.batch;

import com.linkedin.common.urn.Urn;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.Getter;

/**
 * Stores the results of running a batch Metadata Test by Metadata Urn. Used for reporting purposes.
 */
@Data
public class BatchTestResult {
  @Getter @Nonnull private final Urn urn;
  private final AtomicLong passCount;
  private final AtomicLong failCount;

  public BatchTestResult(@Nonnull final Urn urn) {
    this.urn = Objects.requireNonNull(urn);
    this.passCount = new AtomicLong(0);
    this.failCount = new AtomicLong(0);
  }

  public void incrementPass(long count) {
    passCount.addAndGet(count);
  }

  public void incrementFail(long count) {
    failCount.addAndGet(count);
  }

  public long getPassCount() {
    return passCount.get();
  }

  public long getFailCount() {
    return failCount.get();
  }
}
