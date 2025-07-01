package com.linkedin.metadata.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class LineageSearchResultCacheKeyTest extends AbstractTestNGSpringContextTests {

  @Test
  public void testNulls() {
    // ensure no NPE
    assertEquals(
        new EntityLineageResultCacheKey("", null, null, null, null),
        new EntityLineageResultCacheKey("", null, null, null, null));
  }

  @Test
  public void testDateTruncation() {
    // expect start of day milli
    assertEquals(
        new EntityLineageResultCacheKey("", null, null, null, null),
        new EntityLineageResultCacheKey("", null, null, null, null));
    assertNotSame(
        new EntityLineageResultCacheKey("", null, null, null, null),
        new EntityLineageResultCacheKey("", null, null, null, null));
  }
}
