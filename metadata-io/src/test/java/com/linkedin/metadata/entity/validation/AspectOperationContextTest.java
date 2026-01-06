package com.linkedin.metadata.entity.validation;

import static org.testng.Assert.*;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectOperationContextTest {

  @BeforeMethod
  public void setup() {
    AspectOperationContext.clear();
  }

  @AfterMethod
  public void cleanup() {
    AspectOperationContext.clear();
  }

  @Test
  public void testSetAndGet() {
    AspectOperationContext.set("testKey", "testValue");

    Object value = AspectOperationContext.get("testKey");
    assertEquals(value, "testValue");
  }

  @Test
  public void testSetAndGetMap() {
    AspectOperationContext.set("key1", "value1");
    AspectOperationContext.set("key2", 42);
    AspectOperationContext.set("key3", true);

    Map<String, Object> context = AspectOperationContext.get();
    assertEquals(context.size(), 3);
    assertEquals(context.get("key1"), "value1");
    assertEquals(context.get("key2"), 42);
    assertEquals(context.get("key3"), true);
  }

  @Test
  public void testGetNonExistentKey() {
    Object value = AspectOperationContext.get("nonExistent");
    assertNull(value);
  }

  @Test
  public void testGetEmptyContext() {
    Map<String, Object> context = AspectOperationContext.get();
    assertNotNull(context);
    assertTrue(context.isEmpty());
  }

  @Test
  public void testClear() {
    AspectOperationContext.set("key1", "value1");
    AspectOperationContext.set("key2", "value2");

    Map<String, Object> contextBefore = AspectOperationContext.get();
    assertEquals(contextBefore.size(), 2);

    AspectOperationContext.clear();

    Map<String, Object> contextAfter = AspectOperationContext.get();
    assertTrue(contextAfter.isEmpty());
  }

  @Test
  public void testGetReturnsUnmodifiableMap() {
    AspectOperationContext.set("key1", "value1");

    Map<String, Object> context = AspectOperationContext.get();

    // Attempting to modify should throw
    try {
      context.put("key2", "value2");
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test
  public void testThreadIsolation() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    AtomicReference<Map<String, Object>> thread1Context = new AtomicReference<>();
    AtomicReference<Map<String, Object>> thread2Context = new AtomicReference<>();

    // Thread 1 sets its own context
    Thread t1 =
        new Thread(
            () -> {
              try {
                AspectOperationContext.set("threadId", "thread1");
                AspectOperationContext.set("value", 100);
                thread1Context.set(AspectOperationContext.get());
                latch.countDown();
              } catch (Exception e) {
                fail("Thread 1 failed: " + e.getMessage());
              }
            });

    // Thread 2 sets its own context
    Thread t2 =
        new Thread(
            () -> {
              try {
                AspectOperationContext.set("threadId", "thread2");
                AspectOperationContext.set("value", 200);
                thread2Context.set(AspectOperationContext.get());
                latch.countDown();
              } catch (Exception e) {
                fail("Thread 2 failed: " + e.getMessage());
              }
            });

    t1.start();
    t2.start();
    latch.await();

    // Each thread should only see its own context
    assertEquals(thread1Context.get().get("threadId"), "thread1");
    assertEquals(thread1Context.get().get("value"), 100);

    assertEquals(thread2Context.get().get("threadId"), "thread2");
    assertEquals(thread2Context.get().get("value"), 200);

    // Main thread should see empty context
    assertTrue(AspectOperationContext.get().isEmpty());
  }

  @Test
  public void testOverwriteValue() {
    AspectOperationContext.set("key", "value1");
    assertEquals(AspectOperationContext.get("key"), "value1");

    AspectOperationContext.set("key", "value2");
    assertEquals(AspectOperationContext.get("key"), "value2");
  }

  @Test
  public void testRemediationDeletionFlag() {
    // Test the specific use case for remediation deletion
    AspectOperationContext.set("isRemediationDeletion", true);

    Object value = AspectOperationContext.get("isRemediationDeletion");
    assertTrue(value instanceof Boolean);
    assertTrue((Boolean) value);

    Map<String, Object> context = AspectOperationContext.get();
    assertTrue((Boolean) context.get("isRemediationDeletion"));
  }

  @Test
  public void testMixedTypes() {
    AspectOperationContext.set("stringKey", "stringValue");
    AspectOperationContext.set("intKey", 42);
    AspectOperationContext.set("boolKey", true);
    AspectOperationContext.set("longKey", 123456789L);
    AspectOperationContext.set("doubleKey", 3.14);

    assertEquals(AspectOperationContext.get("stringKey"), "stringValue");
    assertEquals(AspectOperationContext.get("intKey"), 42);
    assertEquals(AspectOperationContext.get("boolKey"), true);
    assertEquals(AspectOperationContext.get("longKey"), 123456789L);
    assertEquals(AspectOperationContext.get("doubleKey"), 3.14);
  }
}
