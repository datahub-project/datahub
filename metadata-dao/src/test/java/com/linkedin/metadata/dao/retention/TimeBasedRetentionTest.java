package com.linkedin.metadata.dao.retention;

import org.testng.annotations.Test;


public class TimeBasedRetentionTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNegativeMaxAgeToRetain() {
    new TimeBasedRetention(-1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testZeroMaxAgeToRetain() {
    new TimeBasedRetention(0);
  }
}
