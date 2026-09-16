package com.linkedin.metadata.models;

import static org.testng.Assert.*;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.annotation.TimeseriesFieldAnnotation;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class TimerseriesFieldAnnotationTest {
  private static final String TEST_FIELD_NAME = "testField";

  @Test
  public void testBasicAnnotation() throws Exception {
    Object annotationObject = new HashMap<>();
    TimeseriesFieldAnnotation annotation =
        TimeseriesFieldAnnotation.fromPegasusAnnotationObject(
            annotationObject, TEST_FIELD_NAME, DataSchema.Type.INT, "");
    TimeseriesFieldAnnotation expectedAnnotation =
        new TimeseriesFieldAnnotation(
            TEST_FIELD_NAME,
            TimeseriesFieldAnnotation.AggregationType.LATEST,
            TimeseriesFieldAnnotation.FieldType.INT);
    assertEquals(annotation, expectedAnnotation);
  }

  @Test
  public void testCustomName() throws Exception {
    Map<String, String> annotationObject = new HashMap<>();
    annotationObject.put("name", "newName");
    TimeseriesFieldAnnotation annotation =
        TimeseriesFieldAnnotation.fromPegasusAnnotationObject(
            annotationObject, TEST_FIELD_NAME, DataSchema.Type.FLOAT, "");
    TimeseriesFieldAnnotation expectedAnnotation =
        new TimeseriesFieldAnnotation(
            "newName",
            TimeseriesFieldAnnotation.AggregationType.LATEST,
            TimeseriesFieldAnnotation.FieldType.FLOAT);
    assertEquals(annotation, expectedAnnotation);
  }

  @Test
  public void testCustomAggregationType() throws Exception {
    Map<String, String> annotationObject = new HashMap<>();
    annotationObject.put("aggregationType", "SUM");
    TimeseriesFieldAnnotation annotation =
        TimeseriesFieldAnnotation.fromPegasusAnnotationObject(
            annotationObject, TEST_FIELD_NAME, DataSchema.Type.FLOAT, "");
    TimeseriesFieldAnnotation expectedAnnotation =
        new TimeseriesFieldAnnotation(
            TEST_FIELD_NAME,
            TimeseriesFieldAnnotation.AggregationType.SUM,
            TimeseriesFieldAnnotation.FieldType.FLOAT);
    assertEquals(annotation, expectedAnnotation);
  }

  @Test
  public void testInvalidCustomAggregationTypeThrows() throws Exception {
    Map<String, String> annotationObject = new HashMap<>();
    annotationObject.put("aggregationType", "invalid");
    assertThrows(
        () ->
            TimeseriesFieldAnnotation.fromPegasusAnnotationObject(
                annotationObject, TEST_FIELD_NAME, DataSchema.Type.FLOAT, ""));
  }

  @Test
  public void testCustomFieldType() throws Exception {
    Map<String, String> annotationObject = new HashMap<>();
    annotationObject.put("fieldType", "DOUBLE");
    TimeseriesFieldAnnotation annotation =
        TimeseriesFieldAnnotation.fromPegasusAnnotationObject(
            annotationObject, TEST_FIELD_NAME, DataSchema.Type.INT, "");
    TimeseriesFieldAnnotation expectedAnnotation =
        new TimeseriesFieldAnnotation(
            TEST_FIELD_NAME,
            TimeseriesFieldAnnotation.AggregationType.LATEST,
            TimeseriesFieldAnnotation.FieldType.DOUBLE);
    assertEquals(annotation, expectedAnnotation);
  }

  @Test
  public void testCustomFieldTypeDatetime() throws Exception {
    Map<String, String> annotationObject = new HashMap<>();
    annotationObject.put("fieldType", "DATETIME");
    TimeseriesFieldAnnotation annotation =
        TimeseriesFieldAnnotation.fromPegasusAnnotationObject(
            annotationObject, TEST_FIELD_NAME, DataSchema.Type.LONG, "");
    TimeseriesFieldAnnotation expectedAnnotation =
        new TimeseriesFieldAnnotation(
            TEST_FIELD_NAME,
            TimeseriesFieldAnnotation.AggregationType.LATEST,
            TimeseriesFieldAnnotation.FieldType.DATETIME);
    assertEquals(annotation, expectedAnnotation);
  }

  @Test
  public void testInvalidCustomFieldTypeThrows() throws Exception {
    Map<String, String> annotationObject = new HashMap<>();
    annotationObject.put("fieldType", "invalid");
    assertThrows(
        () ->
            TimeseriesFieldAnnotation.fromPegasusAnnotationObject(
                annotationObject, TEST_FIELD_NAME, DataSchema.Type.LONG, ""));
  }
}
