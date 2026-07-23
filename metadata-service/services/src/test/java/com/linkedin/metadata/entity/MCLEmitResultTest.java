package com.linkedin.metadata.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.mxe.MetadataChangeLog;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.testng.annotations.Test;

public class MCLEmitResultTest {

  @Test
  void testBuilderAndGetters() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture(null);

    MCLEmitResult result =
        MCLEmitResult.builder()
            .metadataChangeLog(mcl)
            .mclFuture(mclFuture)
            .processedMCL(true)
            .emitted(true)
            .build();

    assertEquals(result.getMetadataChangeLog(), mcl);
    assertEquals(result.getMclFuture(), mclFuture);
    assertTrue(result.isProcessedMCL());
    assertTrue(result.isEmitted());
  }

  @Test
  void testIsProduced_SuccessfulFuture() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture("success");

    MCLEmitResult result =
        MCLEmitResult.builder().metadataChangeLog(mcl).mclFuture(mclFuture).build();

    assertTrue(result.isProduced());
  }

  @Test
  void testIsProduced_NullFuture() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);

    MCLEmitResult result = MCLEmitResult.builder().metadataChangeLog(mcl).mclFuture(null).build();

    assertFalse(result.isProduced());
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

    MCLEmitResult result =
        MCLEmitResult.builder().metadataChangeLog(mcl).mclFuture(mclFuture).build();

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

    MCLEmitResult result =
        MCLEmitResult.builder().metadataChangeLog(mcl).mclFuture(mclFuture).build();

    assertFalse(result.isProduced());
  }

  @Test
  void testIsProduced_FailedFuture() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    CompletableFuture<?> mclFuture = new CompletableFuture<>();
    mclFuture.completeExceptionally(new RuntimeException("Test failure"));

    MCLEmitResult result =
        MCLEmitResult.builder().metadataChangeLog(mcl).mclFuture(mclFuture).build();

    assertFalse(result.isProduced());
  }

  @Test
  void testDefaultValues() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);

    MCLEmitResult result = MCLEmitResult.builder().metadataChangeLog(mcl).build();

    assertEquals(result.getMetadataChangeLog(), mcl);
    assertNull(result.getMclFuture());
    assertFalse(result.isProcessedMCL());
    assertFalse(result.isEmitted());
    assertFalse(result.isProduced());
  }

  @Test
  void testToBuilder() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture(null);

    MCLEmitResult original =
        MCLEmitResult.builder()
            .metadataChangeLog(mcl)
            .mclFuture(mclFuture)
            .processedMCL(true)
            .emitted(false)
            .build();

    MCLEmitResult modified = original.toBuilder().emitted(true).build();

    assertEquals(modified.getMetadataChangeLog(), mcl);
    assertEquals(modified.getMclFuture(), mclFuture);
    assertTrue(modified.isProcessedMCL());
    assertTrue(modified.isEmitted());
    assertTrue(modified.isProduced());
  }

  @Test
  void testEqualsAndHashCode() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture(null);

    MCLEmitResult result1 =
        MCLEmitResult.builder()
            .metadataChangeLog(mcl)
            .mclFuture(mclFuture)
            .processedMCL(true)
            .emitted(true)
            .build();

    MCLEmitResult result2 =
        MCLEmitResult.builder()
            .metadataChangeLog(mcl)
            .mclFuture(mclFuture)
            .processedMCL(true)
            .emitted(true)
            .build();

    assertEquals(result1, result2);
    assertEquals(result1.hashCode(), result2.hashCode());
  }

  @Test
  void testToString() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    Future<?> mclFuture = CompletableFuture.completedFuture(null);

    MCLEmitResult result =
        MCLEmitResult.builder()
            .metadataChangeLog(mcl)
            .mclFuture(mclFuture)
            .processedMCL(true)
            .emitted(true)
            .build();

    String toString = result.toString();
    assertNotNull(toString);
    assertTrue(toString.contains("MCLEmitResult"));
    assertTrue(toString.contains("processedMCL=true"));
    assertTrue(toString.contains("emitted=true"));
  }

  @Test
  void testAllPossibleStates() {
    MetadataChangeLog mcl = mock(MetadataChangeLog.class);

    // Test all combinations of processedMCL and emitted flags
    for (boolean processed : new boolean[] {true, false}) {
      for (boolean emitted : new boolean[] {true, false}) {
        MCLEmitResult result =
            MCLEmitResult.builder()
                .metadataChangeLog(mcl)
                .processedMCL(processed)
                .emitted(emitted)
                .build();

        assertEquals(result.isProcessedMCL(), processed);
        assertEquals(result.isEmitted(), emitted);
        assertFalse(result.isProduced()); // No future set, so should always be false
      }
    }
  }
}
