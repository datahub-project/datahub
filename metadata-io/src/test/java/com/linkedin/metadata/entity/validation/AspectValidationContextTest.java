package com.linkedin.metadata.entity.validation;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectValidationContextTest {

  @BeforeMethod
  public void setup() {
    AspectValidationContext.clearPendingDeletions();
  }

  @AfterMethod
  public void cleanup() {
    AspectValidationContext.clearPendingDeletions();
  }

  @Test
  public void testAddAndGetPendingDeletions() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");

    AspectDeletionRequest request =
        AspectDeletionRequest.builder()
            .urn(urn)
            .aspectName("status")
            .validationPoint(ValidationPoint.POST_DB_PATCH)
            .aspectSize(20000000L)
            .threshold(15000000L)
            .build();

    AspectValidationContext.addPendingDeletion(request);

    List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
    assertEquals(deletions.size(), 1);
    assertEquals(deletions.get(0).getUrn(), urn);
    assertEquals(deletions.get(0).getAspectName(), "status");
    assertEquals(deletions.get(0).getValidationPoint(), ValidationPoint.POST_DB_PATCH);
    assertEquals(deletions.get(0).getAspectSize(), 20000000L);
    assertEquals(deletions.get(0).getThreshold(), 15000000L);
  }

  @Test
  public void testAddMultipleDeletions() throws Exception {
    Urn urn1 = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test1,PROD)");
    Urn urn2 = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test2,PROD)");

    AspectDeletionRequest request1 =
        AspectDeletionRequest.builder()
            .urn(urn1)
            .aspectName("status")
            .validationPoint(ValidationPoint.PRE_DB_PATCH)
            .aspectSize(10000000L)
            .threshold(5000000L)
            .build();

    AspectDeletionRequest request2 =
        AspectDeletionRequest.builder()
            .urn(urn2)
            .aspectName("ownership")
            .validationPoint(ValidationPoint.POST_DB_PATCH)
            .aspectSize(20000000L)
            .threshold(15000000L)
            .build();

    AspectValidationContext.addPendingDeletion(request1);
    AspectValidationContext.addPendingDeletion(request2);

    List<AspectDeletionRequest> deletions = AspectValidationContext.getPendingDeletions();
    assertEquals(deletions.size(), 2);
    assertEquals(deletions.get(0).getUrn(), urn1);
    assertEquals(deletions.get(1).getUrn(), urn2);
  }

  @Test
  public void testClearPendingDeletions() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");

    AspectDeletionRequest request =
        AspectDeletionRequest.builder()
            .urn(urn)
            .aspectName("status")
            .validationPoint(ValidationPoint.POST_DB_PATCH)
            .aspectSize(20000000L)
            .threshold(15000000L)
            .build();

    AspectValidationContext.addPendingDeletion(request);
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 1);

    AspectValidationContext.clearPendingDeletions();
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testThreadIsolation() throws Exception {
    Urn urn1 = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,thread1,PROD)");
    Urn urn2 = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,thread2,PROD)");

    CountDownLatch latch = new CountDownLatch(2);
    AtomicReference<List<AspectDeletionRequest>> thread1Deletions = new AtomicReference<>();
    AtomicReference<List<AspectDeletionRequest>> thread2Deletions = new AtomicReference<>();

    // Thread 1 adds request with urn1
    Thread t1 =
        new Thread(
            () -> {
              try {
                AspectDeletionRequest request =
                    AspectDeletionRequest.builder()
                        .urn(urn1)
                        .aspectName("status")
                        .validationPoint(ValidationPoint.POST_DB_PATCH)
                        .aspectSize(10000000L)
                        .threshold(5000000L)
                        .build();
                AspectValidationContext.addPendingDeletion(request);
                thread1Deletions.set(AspectValidationContext.getPendingDeletions());
                latch.countDown();
              } catch (Exception e) {
                fail("Thread 1 failed: " + e.getMessage());
              }
            });

    // Thread 2 adds request with urn2
    Thread t2 =
        new Thread(
            () -> {
              try {
                AspectDeletionRequest request =
                    AspectDeletionRequest.builder()
                        .urn(urn2)
                        .aspectName("ownership")
                        .validationPoint(ValidationPoint.PRE_DB_PATCH)
                        .aspectSize(20000000L)
                        .threshold(15000000L)
                        .build();
                AspectValidationContext.addPendingDeletion(request);
                thread2Deletions.set(AspectValidationContext.getPendingDeletions());
                latch.countDown();
              } catch (Exception e) {
                fail("Thread 2 failed: " + e.getMessage());
              }
            });

    t1.start();
    t2.start();
    latch.await();

    // Each thread should only see its own deletion request
    assertEquals(thread1Deletions.get().size(), 1);
    assertEquals(thread1Deletions.get().get(0).getUrn(), urn1);

    assertEquals(thread2Deletions.get().size(), 1);
    assertEquals(thread2Deletions.get().get(0).getUrn(), urn2);

    // Main thread should see neither
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 0);
  }

  @Test
  public void testGetReturnsDefensiveCopy() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");

    AspectDeletionRequest request =
        AspectDeletionRequest.builder()
            .urn(urn)
            .aspectName("status")
            .validationPoint(ValidationPoint.POST_DB_PATCH)
            .aspectSize(20000000L)
            .threshold(15000000L)
            .build();

    AspectValidationContext.addPendingDeletion(request);

    List<AspectDeletionRequest> deletions1 = AspectValidationContext.getPendingDeletions();
    List<AspectDeletionRequest> deletions2 = AspectValidationContext.getPendingDeletions();

    // Should return defensive copies
    assertNotSame(deletions1, deletions2);
    assertEquals(deletions1, deletions2);

    // Modifying returned list should not affect ThreadLocal
    deletions1.clear();
    assertEquals(AspectValidationContext.getPendingDeletions().size(), 1);
  }
}
