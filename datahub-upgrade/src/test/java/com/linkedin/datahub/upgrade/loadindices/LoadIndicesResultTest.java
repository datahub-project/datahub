package com.linkedin.datahub.upgrade.loadindices;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class LoadIndicesResultTest {

  @Test
  public void testDefaultValues() {
    LoadIndicesResult result = new LoadIndicesResult();

    assertEquals(0, result.rowsProcessed);
    assertEquals(0, result.ignored);
    assertEquals(0, result.timeSqlQueryMs);
    assertEquals(0, result.timeElasticsearchWriteMs);
  }

  @Test
  public void testSettersAndGetters() {
    LoadIndicesResult result = new LoadIndicesResult();

    result.rowsProcessed = 1000;
    result.ignored = 5;
    result.timeSqlQueryMs = 1500L;
    result.timeElasticsearchWriteMs = 8000L;

    assertEquals(1000, result.rowsProcessed);
    assertEquals(5, result.ignored);
    assertEquals(1500L, result.timeSqlQueryMs);
    assertEquals(8000L, result.timeElasticsearchWriteMs);
  }

  @Test
  public void testToString() {
    LoadIndicesResult result = new LoadIndicesResult();
    result.rowsProcessed = 1000;
    result.ignored = 5;
    result.timeSqlQueryMs = 1500L;
    result.timeElasticsearchWriteMs = 8000L;

    String resultString = result.toString();

    assertTrue(resultString.contains("LoadIndicesResult"));
    assertTrue(resultString.contains("rowsProcessed=1000"));
    assertTrue(resultString.contains("ignored=5"));
    assertTrue(resultString.contains("timeSqlQueryMs=1500"));
    assertTrue(resultString.contains("timeElasticsearchWriteMs=8000"));
  }

  @Test
  public void testToStringWithZeroValues() {
    LoadIndicesResult result = new LoadIndicesResult();

    String resultString = result.toString();

    assertTrue(resultString.contains("LoadIndicesResult"));
    assertTrue(resultString.contains("rowsProcessed=0"));
    assertTrue(resultString.contains("ignored=0"));
    assertTrue(resultString.contains("timeSqlQueryMs=0"));
    assertTrue(resultString.contains("timeElasticsearchWriteMs=0"));
  }
}
