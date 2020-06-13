package com.linkedin.metadata.generator;

import org.testng.annotations.Test;

import static com.linkedin.metadata.generator.SchemaGeneratorUtil.*;
import static org.testng.Assert.*;


public class TestSchemaGeneratorUtil {

  private static final String TEST_NAME = "BarUrn";

  @Test
  public void testDeCapitalize() {
    assertEquals(deCapitalize(TEST_NAME), "barUrn");
  }

  @Test
  public void testGetEntityName() {
    assertEquals(getEntityName(TEST_NAME), "Bar");
  }

  @Test
  public void testStripNamespace() {
    assertEquals(stripNamespace(TEST_NAME), "BarUrn");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testStripIllegalNamespace() {
    stripNamespace(TEST_NAME + ".");
  }
}