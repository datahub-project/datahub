package com.linkedin.datahub.upgrade.test;

import com.linkedin.common.urn.Urn;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Stores the results of running a batch Metadata Test by Metadata Urn.
 * Used for reporting purposes.
 */
class BatchTestResult {

  private final Urn urn;
  private int passCount;
  private int failCount;

  public BatchTestResult(@Nonnull final Urn urn) {
    this.urn = Objects.requireNonNull(urn);
    this.passCount = 0;
    this.failCount = 0;
  }

  public synchronized void incrementPass() {
    passCount++;
  }

  public synchronized void incrementFail() {
    failCount++;
  }

  public Urn getUrn() {
    return urn;
  }

  public int getPassCount() {
    return passCount;
  }

  public int getFailCount() {
    return failCount;
  }

}

/**
 * Thread-safe object for storing the results of running a batch Metadata Test.
 */
class BatchTestResultAggregator {
  private Map<Urn, BatchTestResult> tests;

  public BatchTestResultAggregator() {
    this.tests = new HashMap<>();
  }

  public synchronized void addTestResultSummary(@Nonnull final Urn urn) {
    if (!tests.containsKey(urn)) {
      tests.put(urn, new BatchTestResult(urn));
    }
  }

  public void incrementPass(@Nonnull final Urn urn) {
    synchronized (this) {
      if (!tests.containsKey(urn)) {
        addTestResultSummary(urn);
      }
    }
    tests.get(urn).incrementPass();
  }

  public void incrementFail(@Nonnull final Urn urn) {
    synchronized (this) {
      if (!tests.containsKey(urn)) {
        addTestResultSummary(urn);
      }
    }
    tests.get(urn).incrementFail();
  }

  @Nonnull
  public Map<Urn, BatchTestResult> getTestResultSummaries() {
    synchronized (this) {
      return new HashMap<>(tests);
    }
  }
}