package com.linkedin.common;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class FabricTypeNamesTest {

  @Test
  public void testEveryBuiltInIsWellFormed() {
    for (FabricType type : FabricType.values()) {
      if (type != FabricType.$UNKNOWN) {
        assertTrue(FabricTypeNames.isBuiltIn(type.name()), type.name());
        assertTrue(FabricTypeNames.isWellFormed(type.name()), type.name());
      }
    }
  }

  @Test
  public void testCustomNames() {
    assertTrue(FabricTypeNames.isWellFormed("MY_ENV"));
    assertTrue(FabricTypeNames.isWellFormed("PERF2"));
    assertFalse(FabricTypeNames.isBuiltIn("MY_ENV"));

    assertFalse(FabricTypeNames.isWellFormed("prod"));
    assertFalse(FabricTypeNames.isWellFormed("MY-ENV"));
    assertFalse(FabricTypeNames.isWellFormed("2ENV"));
    assertFalse(FabricTypeNames.isWellFormed("A".repeat(65)));
    assertFalse(FabricTypeNames.isWellFormed(""));
  }

  @Test
  public void testCollidesWithBuiltInIgnoringCase() {
    assertTrue(FabricTypeNames.collidesWithBuiltIn("PROD"));
    assertTrue(FabricTypeNames.collidesWithBuiltIn("prod"));
    assertTrue(FabricTypeNames.collidesWithBuiltIn("Sandbox"));
    assertFalse(FabricTypeNames.collidesWithBuiltIn("MY_ENV"));
  }
}
