package com.linkedin.metadata.entity.validation;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class ValidationPointTest {

  @Test
  public void testEnumValues() {
    ValidationPoint[] values = ValidationPoint.values();
    assertEquals(values.length, 2);
    assertEquals(values[0], ValidationPoint.PRE_DB_PATCH);
    assertEquals(values[1], ValidationPoint.POST_DB_PATCH);
  }

  @Test
  public void testValueOf() {
    assertEquals(ValidationPoint.valueOf("PRE_DB_PATCH"), ValidationPoint.PRE_DB_PATCH);
    assertEquals(ValidationPoint.valueOf("POST_DB_PATCH"), ValidationPoint.POST_DB_PATCH);
  }

  @Test
  public void testToString() {
    assertEquals(ValidationPoint.PRE_DB_PATCH.toString(), "prePatch");
    assertEquals(ValidationPoint.POST_DB_PATCH.toString(), "postPatch");
  }
}
