package com.linkedin.metadata.config.search;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class SearchConfigurationTest {

  @Test
  public void testDefaultMaxAggValuesDefaultsToTwenty() {
    assertEquals(new SearchConfiguration().getDefaultMaxAggValues(), 20);
    assertEquals(SearchConfiguration.builder().build().getDefaultMaxAggValues(), 20);
  }

  @Test
  public void testDefaultMaxAggValuesWithinCeilingIsNotFlagged() {
    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(60);
    config.setDefaultMaxAggValues(20);

    assertFalse(config.isDefaultMaxAggValuesAboveMaxTermBucketSize());
    config.validate(); // should not throw
  }

  @Test
  public void testDefaultMaxAggValuesEqualToCeilingIsNotFlagged() {
    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(60);
    config.setDefaultMaxAggValues(60);

    assertFalse(config.isDefaultMaxAggValuesAboveMaxTermBucketSize());
  }

  @Test
  public void testDefaultMaxAggValuesAboveCeilingIsFlagged() {
    SearchConfiguration config = new SearchConfiguration();
    config.setMaxTermBucketSize(60);
    config.setDefaultMaxAggValues(100);

    assertTrue(config.isDefaultMaxAggValuesAboveMaxTermBucketSize());
    config.validate(); // should log a warning, not throw
  }
}
