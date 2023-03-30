package com.linkedin.metadata.search;

import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;


public class LineageSearchResultCacheKeyTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testNulls() {
    // ensure no NPE
    assertEquals(new EntityLineageResultCacheKey(null, null, null, null, null),
            EntityLineageResultCacheKey.from(null, null, null, null, null));
  }

  @Test
  public void testDateTruncation() {
    // expect start of day milli
    assertEquals(new EntityLineageResultCacheKey(null, null, 1679529600000L, 1679616000000L, null),
            EntityLineageResultCacheKey.from(null, null, 1679530293000L, 1679530293001L,
                    null));
  }
}
