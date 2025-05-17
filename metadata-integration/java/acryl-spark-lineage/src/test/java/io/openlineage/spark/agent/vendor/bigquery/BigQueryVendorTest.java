package io.openlineage.spark.agent.vendor.bigquery;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BigQueryVendorTest {

  @Test
  void testHasBigQueryClasses() {
    // Instead of mocking Thread, use reflection to test the class loading logic
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread()
          .setContextClassLoader(new MockClassLoader(Constants.BIGQUERY_CLASS_NAME));
      assertTrue(BigQueryVendor.hasBigQueryClasses());
    } finally {
      // Ensure we restore the original class loader
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @Test
  void testHasBigQueryClassesReturnsFalseWhenNotPresent() {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(new MockClassLoader("some.other.class"));
      assertFalse(BigQueryVendor.hasBigQueryClasses());
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
