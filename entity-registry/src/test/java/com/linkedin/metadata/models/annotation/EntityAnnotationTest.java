package com.linkedin.metadata.models.annotation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/** Tests for {@link EntityAnnotation} */
public class EntityAnnotationTest {

  @Test
  public void testEntityAnnotationWithSearchIndexGroup() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "primary");

    EntityAnnotation annotation =
        EntityAnnotation.fromSchemaProperty(annotationMap, "test-context");

    assertEquals(annotation.getName(), "testEntity");
    assertEquals(annotation.getKeyAspect(), "testKey");
    assertEquals(annotation.getSearchGroup(), "primary");
  }

  @Test
  public void testEntityAnnotationWithoutSearchIndexGroup() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");

    EntityAnnotation annotation =
        EntityAnnotation.fromSchemaProperty(annotationMap, "test-context");

    assertEquals(annotation.getName(), "testEntity");
    assertEquals(annotation.getKeyAspect(), "testKey");
    assertEquals(annotation.getSearchGroup(), EntityAnnotation.DEFAULT_SEARCH_GROUP);
  }

  @Test
  public void testEntityAnnotationWithTimeseriesGroup() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "timeseries");

    EntityAnnotation annotation =
        EntityAnnotation.fromSchemaProperty(annotationMap, "test-context");

    assertEquals(annotation.getName(), "testEntity");
    assertEquals(annotation.getKeyAspect(), "testKey");
    assertEquals(annotation.getSearchGroup(), "timeseries");
  }

  @Test
  public void testEntityAnnotationWithCustomGroup() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "customgroup");

    EntityAnnotation annotation =
        EntityAnnotation.fromSchemaProperty(annotationMap, "test-context");

    assertEquals(annotation.getName(), "testEntity");
    assertEquals(annotation.getKeyAspect(), "testKey");
    assertEquals(annotation.getSearchGroup(), "customgroup");
  }

  @Test
  public void testEntityAnnotationConstructorWithSearchIndexGroup() {
    EntityAnnotation annotation = new EntityAnnotation("testEntity", "testKey", "primary");

    assertEquals(annotation.getName(), "testEntity");
    assertEquals(annotation.getKeyAspect(), "testKey");
    assertEquals(annotation.getSearchGroup(), "primary");
  }

  @Test
  public void testEntityAnnotationConstructorWithoutSearchIndexGroup() {
    EntityAnnotation annotation = new EntityAnnotation("testEntity", "testKey");

    assertEquals(annotation.getName(), "testEntity");
    assertEquals(annotation.getKeyAspect(), "testKey");
    assertEquals(annotation.getSearchGroup(), EntityAnnotation.DEFAULT_SEARCH_GROUP);
  }

  @Test
  public void testEntityAnnotationMissingName() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "primary");

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }

  @Test
  public void testEntityAnnotationMissingKeyAspect() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("searchGroup", "primary");

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }

  @Test
  public void testEntityAnnotationInvalidType() {
    Object invalidAnnotation = "not-a-map";

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(invalidAnnotation, "test-context"));
  }

  @Test
  public void testConstants() {
    assertEquals(EntityAnnotation.DEFAULT_SEARCH_GROUP, "default");
    assertEquals(EntityAnnotation.ANNOTATION_NAME, "Entity");
  }

  @Test
  public void testValidSearchIndexGroupNames() {
    // Test various valid index names
    String[] validNames = {
      "primary",
      "timeseries",
      "default",
      "custom-group",
      "custom_group",
      "group123",
      "a",
      "test.group",
      "my-custom-index"
    };

    for (String validName : validNames) {
      Map<String, Object> annotationMap = new HashMap<>();
      annotationMap.put("name", "testEntity");
      annotationMap.put("keyAspect", "testKey");
      annotationMap.put("searchGroup", validName);

      // Should not throw exception
      EntityAnnotation annotation =
          EntityAnnotation.fromSchemaProperty(annotationMap, "test-context");
      assertEquals(annotation.getSearchGroup(), validName);
    }
  }

  @Test
  public void testInvalidSearchIndexGroupWithSpaces() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "invalid group");

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }

  @Test
  public void testInvalidSearchIndexGroupWithUppercase() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "InvalidGroup");

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }

  @Test
  public void testInvalidSearchIndexGroupStartingWithUnderscore() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "_invalidgroup");

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }

  @Test
  public void testInvalidSearchIndexGroupStartingWithHyphen() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "-invalidgroup");

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }

  @Test
  public void testInvalidSearchIndexGroupStartingWithPlus() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "+invalidgroup");

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }

  @Test
  public void testInvalidSearchIndexGroupReservedNames() {
    String[] reservedNames = {".", ".."};

    for (String reservedName : reservedNames) {
      Map<String, Object> annotationMap = new HashMap<>();
      annotationMap.put("name", "testEntity");
      annotationMap.put("keyAspect", "testKey");
      annotationMap.put("searchGroup", reservedName);

      assertThrows(
          ModelValidationException.class,
          () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
    }
  }

  @Test
  public void testInvalidSearchIndexGroupWithInvalidCharacters() {
    String[] invalidChars = {"\\", "/", "*", "?", "\"", "<", ">", "|", ",", "#"};

    for (String invalidChar : invalidChars) {
      Map<String, Object> annotationMap = new HashMap<>();
      annotationMap.put("name", "testEntity");
      annotationMap.put("keyAspect", "testKey");
      annotationMap.put("searchGroup", "invalid" + invalidChar + "group");

      assertThrows(
          ModelValidationException.class,
          () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
    }
  }

  @Test
  public void testInvalidSearchIndexGroupEmpty() {
    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", "");

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }

  @Test
  public void testInvalidSearchIndexGroupTooLong() {
    // Create a string longer than 255 characters
    StringBuilder longName = new StringBuilder();
    for (int i = 0; i < 256; i++) {
      longName.append("a");
    }

    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("name", "testEntity");
    annotationMap.put("keyAspect", "testKey");
    annotationMap.put("searchGroup", longName.toString());

    assertThrows(
        ModelValidationException.class,
        () -> EntityAnnotation.fromSchemaProperty(annotationMap, "test-context"));
  }
}
