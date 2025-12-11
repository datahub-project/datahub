/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
