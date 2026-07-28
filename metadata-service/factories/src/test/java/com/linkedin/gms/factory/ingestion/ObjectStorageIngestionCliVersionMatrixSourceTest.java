package com.linkedin.gms.factory.ingestion;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.google.cloud.storage.StorageException;
import com.linkedin.metadata.ingestion.IngestionCliVersionMatrix;
import com.linkedin.metadata.ingestion.MatrixRefreshFailure;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Unit tests for {@link ObjectStorageIngestionCliVersionMatrixSource}. The storage read is mocked
 * at the {@link ObjectStorageClient} boundary, so this one suite covers every backend the client
 * abstracts (S3, GCS, local) — the provider-specific read implementations are exercised in
 * metadata-utils. JSON parsing and validation are covered by {@code
 * IngestionCliVersionMatrixParserTest}, so these tests focus on the source's wiring: fetch → parse
 * → cache → timestamp, and last-known-good retention on failure.
 *
 * <p>{@code refresh()} is driven directly rather than waiting for the background scheduler so the
 * assertions are deterministic.
 */
public class ObjectStorageIngestionCliVersionMatrixSourceTest {

  private static final String KEY = "matrix.json";
  private static final String URI = "s3://cli-version-matrix/matrix.json";
  private static final String MATRIX_JSON =
      "{\"1.5.0\": {\"snowflake\": {\"_default\": \"1.5.0.5\"}}}";

  @Test
  public void refreshLoadsAndCachesMatrix() {
    ObjectStorageClient client = mock(ObjectStorageClient.class);
    when(client.getObjectAsString(KEY)).thenReturn(MATRIX_JSON);

    ObjectStorageIngestionCliVersionMatrixSource source = newSource(client);
    try {
      source.refresh();

      assertEquals(
          source
              .getMatrix()
              .getEntriesForServer("1.5.0")
              .getConnectorEntry("snowflake")
              .getDefaultVersion(),
          "1.5.0.5");
      assertTrue(
          source.getLastFetchedAtMillis() > 0,
          "successful fetch should stamp the last-fetched timestamp");
    } finally {
      source.shutdown();
    }
  }

  @Test
  public void getMatrixServesCachedInstanceBetweenRefreshes() {
    ObjectStorageClient client = mock(ObjectStorageClient.class);
    when(client.getObjectAsString(KEY)).thenReturn(MATRIX_JSON);

    ObjectStorageIngestionCliVersionMatrixSource source = newSource(client);
    // Stop the background scheduler up front so its startup tick can't race the reads below;
    // refresh() is a plain method and still populates the cache deterministically.
    source.shutdown();
    source.refresh();
    IngestionCliVersionMatrix cached = source.getMatrix();

    // Between refreshes getMatrix() is a pure in-memory read: it returns the very same cached
    // instance every time and never re-reads storage. clearInvocations() drops the refresh() read
    // so we can assert the reads below touch storage zero times.
    clearInvocations(client);
    for (int i = 0; i < 5; i++) {
      assertSame(
          source.getMatrix(),
          cached,
          "getMatrix() must serve the cached instance between refreshes, not re-read");
    }
    verifyNoInteractions(client);
  }

  @Test
  public void retainsLastKnownMatrixWhenReadFails() {
    // A good load followed by a storage failure must retain the previously-loaded matrix (not blank
    // it) and leave the last-fetched timestamp untouched — in-flight resolutions never see a flap.
    ObjectStorageClient client = mock(ObjectStorageClient.class);
    when(client.getObjectAsString(KEY))
        .thenReturn(MATRIX_JSON)
        .thenThrow(new RuntimeException("storage unavailable"));

    ObjectStorageIngestionCliVersionMatrixSource source = newSource(client);
    source.shutdown(); // stop the scheduler so its startup tick can't consume a stubbed result
    source.refresh(); // good load
    long stampAfterGoodLoad = source.getLastFetchedAtMillis();
    source.refresh(); // read throws — must retain

    assertEquals(
        source
            .getMatrix()
            .getEntriesForServer("1.5.0")
            .getConnectorEntry("snowflake")
            .getDefaultVersion(),
        "1.5.0.5",
        "matrix must be retained when a later read fails");
    assertEquals(
        source.getLastFetchedAtMillis(),
        stampAfterGoodLoad,
        "a failed read must not advance the last-fetched timestamp");
  }

  @Test
  public void retainsLastKnownMatrixWhenDocumentIsSchemaInvalid() {
    // A file-level schema violation (root is not a JSON object) must not blank the cache either —
    // the parser throws and the source refuses to swap.
    ObjectStorageClient client = mock(ObjectStorageClient.class);
    when(client.getObjectAsString(KEY))
        .thenReturn(MATRIX_JSON)
        .thenReturn("[\"not\", \"a\", \"map\"]");

    ObjectStorageIngestionCliVersionMatrixSource source = newSource(client);
    source.shutdown();
    source.refresh(); // good load
    source.refresh(); // schema-invalid — must retain

    assertEquals(
        source
            .getMatrix()
            .getEntriesForServer("1.5.0")
            .getConnectorEntry("snowflake")
            .getDefaultVersion(),
        "1.5.0.5",
        "matrix must be retained when a later document violates the schema");
  }

  // ---------------------------------------------------------------------------
  // Failure classification — decides which fix an operator is pointed at
  // ---------------------------------------------------------------------------

  @Test
  public void classifiesAccessAndExistenceFailuresPerBackend() {
    // The storage clients wrap provider exceptions in a RuntimeException, so classification has to
    // walk the cause chain rather than inspect the top-level type.
    assertEquals(
        ObjectStorageIngestionCliVersionMatrixSource.classify(
            wrapped(S3Exception.builder().statusCode(403).message("Access Denied").build())),
        MatrixRefreshFailure.PERMISSION,
        "an S3 403 is an access problem, not a transient one");
    assertEquals(
        ObjectStorageIngestionCliVersionMatrixSource.classify(
            wrapped(S3Exception.builder().statusCode(404).message("NoSuchKey").build())),
        MatrixRefreshFailure.NOT_FOUND);
    assertEquals(
        ObjectStorageIngestionCliVersionMatrixSource.classify(
            wrapped(new StorageException(403, "Forbidden"))),
        MatrixRefreshFailure.PERMISSION,
        "GCS reports authorization failures with the same status shape as S3");
    assertEquals(
        ObjectStorageIngestionCliVersionMatrixSource.classify(
            wrapped(new AccessDeniedException("/matrix.json"))),
        MatrixRefreshFailure.PERMISSION,
        "the local backend surfaces access failures as java.nio.file exceptions");
    assertEquals(
        ObjectStorageIngestionCliVersionMatrixSource.classify(
            wrapped(new NoSuchFileException("/matrix.json"))),
        MatrixRefreshFailure.NOT_FOUND);
  }

  @Test
  public void classifiesServerErrorsAndUnknownFailuresAsTransport() {
    // A 5xx or an unrecognised error is retried on the next tick, so it must not be reported as
    // something the operator has to go fix.
    assertEquals(
        ObjectStorageIngestionCliVersionMatrixSource.classify(
            wrapped(S3Exception.builder().statusCode(503).message("SlowDown").build())),
        MatrixRefreshFailure.TRANSPORT);
    assertEquals(
        ObjectStorageIngestionCliVersionMatrixSource.classify(
            new RuntimeException("connection reset")),
        MatrixRefreshFailure.TRANSPORT);
  }

  /** Mirrors how the storage clients surface provider errors: wrapped in a RuntimeException. */
  private static Throwable wrapped(Throwable providerError) {
    return new RuntimeException("Failed to read " + URI + ": " + providerError, providerError);
  }

  private static ObjectStorageIngestionCliVersionMatrixSource newSource(
      ObjectStorageClient client) {
    return new ObjectStorageIngestionCliVersionMatrixSource(client, KEY, URI, 3600);
  }
}
