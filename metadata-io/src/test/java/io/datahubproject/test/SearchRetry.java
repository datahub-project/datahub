package io.datahubproject.test;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;

/** Used to retry search fixture tests 1 time after 5 seconds */
public class SearchRetry implements IRetryAnalyzer {

  private int retryCount = 0;
  private static final int maxRetryCount = 1;
  private static final int delayMs = 5000;

  @Override
  public boolean retry(ITestResult result) {
    if (retryCount < maxRetryCount) {
      retryCount++;
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return true;
    }
    return false;
  }
}
