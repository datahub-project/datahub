package com.linkedin.gms.factory.ingestion;

import com.google.cloud.BaseServiceException;
import com.linkedin.metadata.ingestion.MatrixDocumentReader;
import com.linkedin.metadata.ingestion.MatrixReadException;
import com.linkedin.metadata.ingestion.MatrixRefreshFailure;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import software.amazon.awssdk.core.exception.SdkServiceException;

/**
 * Reads the matrix document through an {@link ObjectStorageClient} — S3, GCS, or the local
 * filesystem, selected by the scheme of {@code ingestion.cliVersionMatrix.uri}. Cloud reads use
 * GMS's ambient credentials, so a private bucket needs no separate secret distributed to GMS.
 *
 * <p>Lives in the factory module rather than alongside the other readers in {@code configuration}
 * because that is where the storage clients and their credential resolution already live, keeping
 * the lightweight {@code configuration} module free of AWS/GCP dependencies.
 */
public class ObjectStorageMatrixDocumentReader implements MatrixDocumentReader {

  /** Bound on the cause-chain walk in {@link #classify}, so a cyclic chain cannot spin. */
  private static final int MAX_CAUSE_DEPTH = 10;

  private final ObjectStorageClient client;
  private final String objectKey;

  /** The configured URI, carried purely so log lines name the location an operator can go fix. */
  private final String displayUri;

  public ObjectStorageMatrixDocumentReader(
      final ObjectStorageClient client, final String objectKey, final String displayUri) {
    this.client = client;
    this.objectKey = objectKey;
    this.displayUri = displayUri;
  }

  @Override
  public String displayUri() {
    return displayUri;
  }

  @Override
  public String read() {
    try {
      return client.getObjectAsString(objectKey);
    } catch (RuntimeException e) {
      // Classify here, where the AWS/GCP exception types are on the classpath, and hand the polling
      // source a verdict it can log without knowing any of them.
      throw new MatrixReadException(classify(e), "Failed to read " + displayUri, e);
    }
  }

  /**
   * Maps a storage failure onto an operator-actionable class. The clients wrap provider exceptions
   * in a {@link RuntimeException}, so the cause chain is walked rather than the top-level type
   * inspected. AWS and GCS both expose an HTTP-shaped status code, which is what distinguishes
   * "denied" from "missing"; the local backend surfaces the equivalent as {@code java.nio.file}
   * exceptions.
   *
   * <p>Package-private for direct unit testing — classification drives what an operator is told to
   * fix, so it is behaviour worth asserting rather than an implementation detail.
   */
  static MatrixRefreshFailure classify(Throwable t) {
    // Bounded walk: a self-referential or cyclic cause chain must not spin here.
    Throwable cause = t;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (cause instanceof AccessDeniedException) {
        return MatrixRefreshFailure.PERMISSION;
      }
      if (cause instanceof NoSuchFileException) {
        return MatrixRefreshFailure.NOT_FOUND;
      }
      if (cause instanceof SdkServiceException awsError) {
        return MatrixRefreshFailure.forHttpStatus(awsError.statusCode());
      }
      if (cause instanceof BaseServiceException gcpError) {
        return MatrixRefreshFailure.forHttpStatus(gcpError.getCode());
      }
      cause = cause.getCause();
    }
    return MatrixRefreshFailure.TRANSPORT;
  }
}
