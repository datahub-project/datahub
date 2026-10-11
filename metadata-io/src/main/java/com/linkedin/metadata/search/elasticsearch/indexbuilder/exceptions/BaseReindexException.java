package com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;

public class BaseReindexException extends RuntimeException {
  public BaseReindexException(String message) {
    super(message);
  }

  public BaseReindexException(String message, Throwable e) {
    super(message, e);
  }

  public BaseReindexException(Throwable e) {
    super(e);
  }

  public ReindexResult getFailureResult() {
    return ReindexResult.FAILED_SUBMISSION;
  }
}
