package com.linkedin.gms.factory.ingestion;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.google.cloud.storage.StorageException;
import com.linkedin.metadata.ingestion.MatrixReadException;
import com.linkedin.metadata.ingestion.MatrixRefreshFailure;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Unit tests for {@link ObjectStorageMatrixDocumentReader}. The read is mocked at the {@link
 * ObjectStorageClient} boundary, so this one suite covers every backend the client abstracts (S3,
 * GCS, local) — the provider-specific read implementations are exercised in metadata-utils, and the
 * refresh/cache contract in {@code PollingIngestionCliVersionMatrixSourceTest}.
 */
public class ObjectStorageMatrixDocumentReaderTest {

  private static final String KEY = "matrix.json";
  private static final String URI = "s3://cli-version-matrix/matrix.json";

  @Test
  public void readReturnsTheObjectBody() {
    ObjectStorageClient client = mock(ObjectStorageClient.class);
    when(client.getObjectAsString(KEY)).thenReturn("{}");

    assertEquals(new ObjectStorageMatrixDocumentReader(client, KEY, URI).read(), "{}");
  }

  @Test
  public void readWrapsFailuresWithAClassificationAndKeepsTheCause() {
    // The polling source logs the attached verdict and needs the original cause for the stack
    // trace.
    S3Exception denied =
        (S3Exception) S3Exception.builder().statusCode(403).message("Access Denied").build();
    ObjectStorageClient client = mock(ObjectStorageClient.class);
    when(client.getObjectAsString(KEY)).thenThrow(wrapped(denied));

    try {
      new ObjectStorageMatrixDocumentReader(client, KEY, URI).read();
      throw new AssertionError("Expected MatrixReadException");
    } catch (MatrixReadException thrown) {
      assertEquals(thrown.failure(), MatrixRefreshFailure.PERMISSION);
      assertSame(thrown.getCause().getCause(), denied, "the provider error must not be swallowed");
    }
  }

  // ---------------------------------------------------------------------------
  // Failure classification — decides which fix an operator is pointed at
  // ---------------------------------------------------------------------------

  @Test
  public void classifiesAccessAndExistenceFailuresPerBackend() {
    // The storage clients wrap provider exceptions in a RuntimeException, so classification has to
    // walk the cause chain rather than inspect the top-level type.
    assertEquals(
        ObjectStorageMatrixDocumentReader.classify(
            wrapped(S3Exception.builder().statusCode(403).message("Access Denied").build())),
        MatrixRefreshFailure.PERMISSION,
        "an S3 403 is an access problem, not a transient one");
    assertEquals(
        ObjectStorageMatrixDocumentReader.classify(
            wrapped(S3Exception.builder().statusCode(404).message("NoSuchKey").build())),
        MatrixRefreshFailure.NOT_FOUND);
    assertEquals(
        ObjectStorageMatrixDocumentReader.classify(wrapped(new StorageException(403, "Forbidden"))),
        MatrixRefreshFailure.PERMISSION,
        "GCS reports authorization failures with the same status shape as S3");
    assertEquals(
        ObjectStorageMatrixDocumentReader.classify(
            wrapped(new AccessDeniedException("/matrix.json"))),
        MatrixRefreshFailure.PERMISSION,
        "the local backend surfaces access failures as java.nio.file exceptions");
    assertEquals(
        ObjectStorageMatrixDocumentReader.classify(
            wrapped(new NoSuchFileException("/matrix.json"))),
        MatrixRefreshFailure.NOT_FOUND);
  }

  @Test
  public void classifiesServerErrorsAndUnknownFailuresAsTransport() {
    // A 5xx or an unrecognised error is retried on the next tick, so it must not be reported as
    // something the operator has to go fix.
    assertEquals(
        ObjectStorageMatrixDocumentReader.classify(
            wrapped(S3Exception.builder().statusCode(503).message("SlowDown").build())),
        MatrixRefreshFailure.TRANSPORT);
    assertEquals(
        ObjectStorageMatrixDocumentReader.classify(new RuntimeException("connection reset")),
        MatrixRefreshFailure.TRANSPORT);
  }

  /** Mirrors how the storage clients surface provider errors: wrapped in a RuntimeException. */
  private static RuntimeException wrapped(Throwable providerError) {
    return new RuntimeException("Failed to read " + URI + ": " + providerError, providerError);
  }
}
