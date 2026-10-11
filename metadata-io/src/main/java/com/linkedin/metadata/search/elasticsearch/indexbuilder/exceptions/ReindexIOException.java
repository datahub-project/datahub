package com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;
import java.io.IOException;

/**
 * Thrown when IO errors occur during reindex operations.
 *
 * <p>This indicates network or communication failures such as: - Socket timeouts - Connection
 * errors - Request/response marshalling failures
 */
public class ReindexIOException extends BaseReindexException {
  public ReindexIOException(String message) {
    super(message);
  }

  public ReindexIOException(String message, Exception cause) {
    super(message, cause);
  }

  public ReindexIOException(IOException cause) {
    super(cause);
  }

  public ReindexResult getFailureResult() {
    return ReindexResult.FAILED_SUBMISSION_IO;
  }
}
