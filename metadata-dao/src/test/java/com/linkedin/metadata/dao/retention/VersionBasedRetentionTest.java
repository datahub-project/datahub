package com.linkedin.metadata.dao.retention;

import org.testng.annotations.Test;


public class VersionBasedRetentionTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNegativeMaxVersionsToRetain() {
    new VersionBasedRetention(-1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testZeroMaxVersionsToRetain() {
    new VersionBasedRetention(0);
  }

}
