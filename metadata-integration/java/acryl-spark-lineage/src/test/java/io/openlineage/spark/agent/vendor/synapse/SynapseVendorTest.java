package io.openlineage.spark.agent.vendor.synapse;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SynapseVendorTest {

  @Test
  void testHasSynapseClasses() {
    // Instead of mocking Thread, use reflection to test the class loading logic
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread()
          .setContextClassLoader(new MockClassLoader(Constants.SYNAPSE_CLASS_NAME));
      assertTrue(SynapseVendor.hasSynapseClasses());
    } finally {
      // Ensure we restore the original class loader
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @Test
  void testHasSynapseClassesReturnsFalseWhenNotPresent() {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(new MockClassLoader("some.other.class"));
      assertFalse(SynapseVendor.hasSynapseClasses());
    } finally {
      // Ensure we restore the original class loader
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  static class MockClassLoader extends ClassLoader {
    private final String availableClassName;

    public MockClassLoader(String availableClassName) {
      this.availableClassName = availableClassName;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      if (name.equals(availableClassName)) {
        return Object.class;
      }
      throw new ClassNotFoundException(name);
    }
  }
}
