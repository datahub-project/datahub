package com.linkedin.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Optional;
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
  public void testCanonicalConfigValues() {
    assertEquals(FabricTypeNames.canonical("prod"), Optional.of("PROD"));
    assertEquals(FabricTypeNames.canonical(" Dev "), Optional.of("DEV"));
    assertEquals(FabricTypeNames.canonical("my_env"), Optional.of("MY_ENV"));
    assertEquals(FabricTypeNames.canonical("my-env"), Optional.empty());
    assertEquals(FabricTypeNames.canonical(""), Optional.empty());
    assertEquals(FabricTypeNames.canonical(null), Optional.empty());
  }

  @Test
  public void testCollidesWithBuiltInIgnoringCase() {
    assertTrue(FabricTypeNames.collidesWithBuiltIn("PROD"));
    assertTrue(FabricTypeNames.collidesWithBuiltIn("prod"));
    assertTrue(FabricTypeNames.collidesWithBuiltIn("Sandbox"));
    assertFalse(FabricTypeNames.collidesWithBuiltIn("MY_ENV"));
  }
}
