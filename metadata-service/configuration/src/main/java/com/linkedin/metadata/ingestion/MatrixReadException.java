package com.linkedin.metadata.ingestion;

/**
 * A matrix read that failed with a cause the reader could already classify — an HTTP status, an S3
 * or GCS service error, a filesystem permission denial.
 *
 * <p>Carrying the classification on the exception is what lets {@link
 * PollingIngestionCliVersionMatrixSource} log an operator-actionable token without knowing any
 * backend's exception types, and keeps the AWS/GCP-aware classification in the module that already
 * depends on those SDKs.
 */
public class MatrixReadException extends RuntimeException {

  private final MatrixRefreshFailure failure;

  public MatrixReadException(MatrixRefreshFailure failure, String message) {
    this(failure, message, null);
  }

  public MatrixReadException(MatrixRefreshFailure failure, String message, Throwable cause) {
    super(message, cause);
    this.failure = failure;
  }

  public MatrixRefreshFailure failure() {
    return failure;
  }
}
