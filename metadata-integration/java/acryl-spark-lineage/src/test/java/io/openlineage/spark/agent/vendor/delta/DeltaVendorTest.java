package io.openlineage.spark.agent.vendor.delta;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

public class DeltaVendorTest {

  @Test
  void testHasDeltaClassesReturnsFalseInTestEnvironment() {
    // Test environment won't have Delta classes in classpath
    assertFalse(DeltaVendor.hasDeltaClasses());
  }
}
