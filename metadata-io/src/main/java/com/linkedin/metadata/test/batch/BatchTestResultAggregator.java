package com.linkedin.metadata.test.batch;

import com.linkedin.common.urn.Urn;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.Data;

/** Thread-safe object for storing the results of running a batch Metadata Test. */
@Data
public class BatchTestResultAggregator {
  @Nonnull private final ConcurrentHashMap<Urn, BatchTestResult> tests;

  public BatchTestResultAggregator() {
    this.tests = new ConcurrentHashMap<>();
  }

  public void incrementPass(@Nonnull final Urn urn, long count) {
    tests.computeIfAbsent(urn, BatchTestResult::new).incrementPass(count);
  }

  public void incrementFail(@Nonnull final Urn urn, long count) {
    tests.computeIfAbsent(urn, BatchTestResult::new).incrementFail(count);
  }

  @Nonnull
  public Map<Urn, BatchTestResult> getTestResultSummaries() {
    return tests;
  }
}
