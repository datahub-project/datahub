package com.linkedin.metadata.config.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class SearchConfigurationTest {

  @Test
  public void testDefaultMaxAggValuesDefaultsToTwenty() {
    assertEquals(new SearchConfiguration().getDefaultMaxAggValues(), 20);
    assertEquals(SearchConfiguration.builder().build().getDefaultMaxAggValues(), 20);
  }

  @Test
  public void testDefaultMaxAggValuesWithinCeilingIsNotFlagged() {
    SearchConfiguration config =
        new SearchConfiguration().setMaxTermBucketSize(60).setDefaultMaxAggValues(60);

    assertFalse(config.isDefaultMaxAggValuesAboveMaxTermBucketSize());
    config.validate();
  }

  @Test
  public void testDefaultMaxAggValuesAboveCeilingIsFlagged() {
    SearchConfiguration config =
        new SearchConfiguration().setMaxTermBucketSize(60).setDefaultMaxAggValues(100);

    assertTrue(config.isDefaultMaxAggValuesAboveMaxTermBucketSize());
    config.validate();
  }
}
