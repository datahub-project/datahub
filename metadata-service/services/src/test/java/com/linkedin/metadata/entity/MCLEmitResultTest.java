package com.linkedin.metadata.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.mxe.MetadataChangeLog;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.testng.annotations.Test;

public class MCLEmitResultTest {

  @Test
  void testFactoryMethodsAndGetters() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture(null);

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, true);

    assertEquals(result.getMetadataChangeLog(), mcl);
    assertEquals(result.getMclFuture(), mclFuture);
    assertTrue(result.isProcessedMCL());
    assertTrue(result.isEmitted());
  }

  @Test
  void testIsProduced_SuccessfulFuture() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture("success");

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, false);

    assertTrue(result.isProduced());
  }

  @Test
  void testIsProduced_NotEmitted() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);

    MCLEmitResult result = MCLEmitResult.notEmitted(mcl, false);

    assertFalse(result.isProduced());
    assertFalse(result.isEmitted());
  }

  @Test
  void testIsProduced_FutureWithInterruptedException() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = mock(Future.class);

    try {
      when(mclFuture.get()).thenThrow(new InterruptedException("Test interruption"));
    } catch (InterruptedException | ExecutionException e) {
      // This should not happen in test setup
    }

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, false);

    assertFalse(result.isProduced());
  }

  @Test
  void testIsProduced_FutureWithExecutionException() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = mock(Future.class);

    try {
      when(mclFuture.get())
          .thenThrow(new ExecutionException("Test execution error", new RuntimeException()));
    } catch (InterruptedException | ExecutionException e) {
      // This should not happen in test setup
    }

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, false);

    assertFalse(result.isProduced());
  }

  @Test
  void testIsProduced_FailedFuture() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    CompletableFuture<?> mclFuture = new CompletableFuture<>();
    mclFuture.completeExceptionally(new RuntimeException("Test failure"));

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, false);

    assertFalse(result.isProduced());
  }

  @Test
  void testNotEmittedDefaults() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);

    MCLEmitResult result = MCLEmitResult.notEmitted(mcl, false);

    assertEquals(result.getMetadataChangeLog(), mcl);
    assertNull(result.getMclFuture());
    assertFalse(result.isProcessedMCL());
    assertFalse(result.isEmitted());
    assertFalse(result.isProduced());
  }

  @Test
  void testEqualsAndHashCode() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture(null);

    MCLEmitResult result1 = MCLEmitResult.emitted(mcl, mclFuture, true);

    MCLEmitResult result2 = MCLEmitResult.emitted(mcl, mclFuture, true);

    assertEquals(result1, result2);
    assertEquals(result1.hashCode(), result2.hashCode());
  }

  @Test
  void testToString() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture(null);

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, true);

    String toString = result.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("MCLEmitResult"));
    assertTrue(toString.contains("processedMCL=true"));
    assertTrue(toString.contains("emitted=true"));
  }

  @Test
  void testAllPossibleStates() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture(null);

    // Test all combinations of processedMCL and emitted flags
    for (boolean processed : new boolean[] {true, false}) {
      // Test not emitted state
      MCLEmitResult notEmittedResult = MCLEmitResult.notEmitted(mcl, processed);
      assertEquals(notEmittedResult.isProcessedMCL(), processed);
      assertFalse(notEmittedResult.isEmitted());
      assertFalse(notEmittedResult.isProduced());

      // Test emitted state
      MCLEmitResult emittedResult = MCLEmitResult.emitted(mcl, mclFuture, processed);
      assertEquals(emittedResult.isProcessedMCL(), processed);
      assertTrue(emittedResult.isEmitted());
      assertTrue(emittedResult.isProduced()); // Future completed successfully
    }
  }

  @Test
  void testInvariantValidation_EmittedWithNullFuture() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);

    try {
      MCLEmitResult.emitted(mcl, null, false);
      fail("Should throw IllegalArgumentException when mclFuture is null");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("mclFuture cannot be null"));
    }
  }

  @Test
  void testProductionResult_Success() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture("success");

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, false);
    MCLEmitResult.ProductionResult productionResult = result.getProductionResult();

    assertTrue(productionResult.isSuccess());
    assertFalse(productionResult.isFailure());
    assertFalse(productionResult.wasNotEmitted());
    assertFalse(productionResult.getError().isPresent());
  }

  @Test
  void testProductionResult_Failure() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    CompletableFuture<?> mclFuture = new CompletableFuture<>();
    RuntimeException testException = new RuntimeException("Test failure");
    mclFuture.completeExceptionally(testException);

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, false);
    MCLEmitResult.ProductionResult productionResult = result.getProductionResult();

    assertFalse(productionResult.isSuccess());
    assertTrue(productionResult.isFailure());
    assertFalse(productionResult.wasNotEmitted());
    assertTrue(productionResult.getError().isPresent());
    assertEquals(productionResult.getError().get(), testException);
  }

  @Test
  void testProductionResult_NotEmitted() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);

    MCLEmitResult result = MCLEmitResult.notEmitted(mcl, false);
    MCLEmitResult.ProductionResult productionResult = result.getProductionResult();

    assertFalse(productionResult.isSuccess());
    assertFalse(productionResult.isFailure());
    assertTrue(productionResult.wasNotEmitted());
    assertFalse(productionResult.getError().isPresent());
  }

  @Test
  void testProductionResult_ExecutionExceptionUnwrapping() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = mock(Future.class);
    RuntimeException rootCause = new RuntimeException("Root cause");

    try {
      when(mclFuture.get()).thenThrow(new ExecutionException("Wrapper", rootCause));
    } catch (InterruptedException | ExecutionException e) {
      // This should not happen in test setup
    }

    MCLEmitResult result = MCLEmitResult.emitted(mcl, mclFuture, false);
    MCLEmitResult.ProductionResult productionResult = result.getProductionResult();

    assertTrue(productionResult.isFailure());
    assertTrue(productionResult.getError().isPresent());
    // Verify the root cause was unwrapped, not the ExecutionException wrapper
    assertEquals(productionResult.getError().get(), rootCause);
  }
}
