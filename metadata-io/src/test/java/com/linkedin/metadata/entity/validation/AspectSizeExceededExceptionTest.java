package com.linkedin.metadata.entity.validation;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class AspectSizeExceededExceptionTest {

  private static final String URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)";
  private static final String ASPECT_NAME = "schemaMetadata";

  @Test
  public void testExceptionConstruction() {
    AspectSizeExceededException exception =
        new AspectSizeExceededException(
            ValidationPoint.PRE_DB_PATCH, 16000000L, 15728640L, URN, ASPECT_NAME);

    assertNotNull(exception);
    assertEquals(exception.getValidationPoint(), ValidationPoint.PRE_DB_PATCH);
    assertEquals(exception.getActualSize(), 16000000L);
    assertEquals(exception.getThreshold(), 15728640L);
    assertEquals(exception.getUrn(), URN);
    assertEquals(exception.getAspectName(), ASPECT_NAME);
  }

  @Test
  public void testExceptionMessage() {
    AspectSizeExceededException exception =
        new AspectSizeExceededException(
            ValidationPoint.POST_DB_PATCH, 20000000L, 15728640L, URN, ASPECT_NAME);

    String expectedMessage =
        "Size validation failed at postPatch: 20000000 bytes exceeds threshold of 15728640 bytes for urn="
            + URN
            + ", aspect="
            + ASPECT_NAME;
    assertEquals(exception.getMessage(), expectedMessage);
  }

  @Test
  public void testGetters() {
    long actualSize = 17000000L;
    long threshold = 16000000L;

    AspectSizeExceededException exception =
        new AspectSizeExceededException(
            ValidationPoint.PRE_DB_PATCH, actualSize, threshold, URN, ASPECT_NAME);

    assertEquals(exception.getValidationPoint(), ValidationPoint.PRE_DB_PATCH);
    assertEquals(exception.getActualSize(), actualSize);
    assertEquals(exception.getThreshold(), threshold);
    assertEquals(exception.getUrn(), URN);
    assertEquals(exception.getAspectName(), ASPECT_NAME);
  }
}
