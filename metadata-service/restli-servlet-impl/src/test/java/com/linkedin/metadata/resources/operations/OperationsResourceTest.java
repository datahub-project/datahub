package com.linkedin.metadata.resources.operations;

import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import junit.framework.TestCase;
import mock.MockTimeseriesAspectService;
import org.testng.annotations.Test;


public class OperationsResourceTest extends TestCase {
  private static final String TASK_ID = "taskId123";

  TimeseriesAspectService mockTimeseriesAspectService = new MockTimeseriesAspectService();

  @Test
  public void testDryRun() {
    String entityType = "dataset";
    String aspectName = "datasetusagestatistics";
    long endTimeMillis = 3000;
    OperationsResource testResource = new OperationsResource(mockTimeseriesAspectService);
    String output = testResource.executeTruncateTimeseriesAspect(entityType, aspectName, endTimeMillis, true, null,
        null);
    assertTrue(output.contains("This was a dry run"));
    output = testResource.executeTruncateTimeseriesAspect(entityType, aspectName, endTimeMillis, false, null,
        null);
    assertEquals(TASK_ID, output);
  }

  @Test
  public void testIsTaskIdValid() {
    assertFalse(OperationsResource.isTaskIdValid("hello"));
    assertTrue(OperationsResource.isTaskIdValid("aB1cdEf2GHIJKLMnoPQr3S:123456"));
    assertFalse(OperationsResource.isTaskIdValid("123456:aB1cdEf2GHIJKLMnoPQr3S"));
    assertFalse(OperationsResource.isTaskIdValid(":123"));
  }
}