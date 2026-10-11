package com.linkedin.metadata.search.embedding;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class EmbeddingProviderExceptionTest {

  @Test
  public void rejectedCredentialOrModelIsSystemic() {
    assertTrue(EmbeddingProviderException.isSystemicHttpStatus(401));
    assertTrue(EmbeddingProviderException.isSystemicHttpStatus(403));
    assertTrue(EmbeddingProviderException.isSystemicHttpStatus(404));
    // 400 is document-specific (e.g. an oversized input); 429 and 5xx are transient.
    assertFalse(EmbeddingProviderException.isSystemicHttpStatus(400));
    assertFalse(EmbeddingProviderException.isSystemicHttpStatus(429));
    assertFalse(EmbeddingProviderException.isSystemicHttpStatus(500));
  }

  @Test
  public void failuresAreItemScopedUnlessMarkedSystemic() {
    RuntimeException cause = new RuntimeException("provider error");

    assertFalse(new EmbeddingProviderException("oversized input", false).isSystemic());
    EmbeddingProviderException transientFailure =
        new EmbeddingProviderException("throttled", cause, true);
    assertTrue(transientFailure.isRetryable());
    assertFalse(transientFailure.isSystemic());

    assertTrue(new EmbeddingProviderException("unknown model", false, true).isSystemic());
    EmbeddingProviderException rejectedKey =
        new EmbeddingProviderException("rejected key", cause, false, true);
    assertFalse(rejectedKey.isRetryable());
    assertTrue(rejectedKey.isSystemic());
  }
}
