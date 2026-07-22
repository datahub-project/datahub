package com.linkedin.gms.factory.ingestion;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.ingestion.IngestionCliVersionMatrix;
import com.linkedin.metadata.utils.gcs.GcsUtil;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link GcsIngestionCliVersionMatrixSource}. The GCS call is mocked via {@link
 * GcsUtil}; JSON parsing/validation itself is covered by {@code
 * IngestionCliVersionMatrixParserTest}, so these tests focus on the source's wiring (fetch → parse
 * → cache → timestamp).
 *
 * <p>{@code refresh()} is driven directly rather than waiting for the background scheduler so the
 * assertions are deterministic. The stub always returns the same body, so the immediate startup
 * tick the constructor schedules can't race the explicit call to a different result.
 */
public class GcsIngestionCliVersionMatrixSourceTest {

  private static final String BUCKET = "cli-version-matrix";
  private static final String KEY = "matrix.json";
  private static final String MATRIX_JSON =
      "{\"1.5.0\": {\"snowflake\": {\"_default\": \"1.5.0.5\"}}}";

  @Test
  public void refreshLoadsAndCachesMatrixFromGcs() {
    GcsUtil gcsUtil = mock(GcsUtil.class);
    when(gcsUtil.getObjectAsString(BUCKET, KEY)).thenReturn(MATRIX_JSON);

    GcsIngestionCliVersionMatrixSource source =
        new GcsIngestionCliVersionMatrixSource(gcsUtil, BUCKET, KEY, 3600);
    try {
      source.refresh();

      IngestionCliVersionMatrix matrix = source.getMatrix();
      assertEquals(
          matrix.getEntriesForServer("1.5.0").getConnectorEntry("snowflake").getDefaultVersion(),
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
    GcsUtil gcsUtil = mock(GcsUtil.class);
    when(gcsUtil.getObjectAsString(BUCKET, KEY)).thenReturn(MATRIX_JSON);

    GcsIngestionCliVersionMatrixSource source =
        new GcsIngestionCliVersionMatrixSource(gcsUtil, BUCKET, KEY, 3600);
    // Stop the background scheduler up front so its startup tick can't race the reads below;
    // refresh() is a plain method and still populates the cache deterministically.
    source.shutdown();
    source.refresh();
    IngestionCliVersionMatrix cached = source.getMatrix();

    // Between refreshes getMatrix() is a pure in-memory read: it returns the very same cached
    // instance every time and never re-fetches from GCS / re-parses. clearInvocations() drops the
    // refresh() read so we can assert the reads below touch GCS zero times.
    clearInvocations(gcsUtil);
    for (int i = 0; i < 5; i++) {
      assertSame(
          source.getMatrix(),
          cached,
          "getMatrix() must serve the cached instance between refreshes, not re-fetch");
    }
    verifyNoInteractions(gcsUtil);
  }

  @Test
  public void retainsLastKnownMatrixWhenFetchFails() {
    // A good load followed by a GCS failure must retain the previously-loaded matrix (not blank
    // it) and leave the last-fetched timestamp untouched — in-flight resolutions never see a flap.
    GcsUtil gcsUtil = mock(GcsUtil.class);
    when(gcsUtil.getObjectAsString(BUCKET, KEY))
        .thenReturn(MATRIX_JSON)
        .thenThrow(new RuntimeException("gcs down"));

    GcsIngestionCliVersionMatrixSource source =
        new GcsIngestionCliVersionMatrixSource(gcsUtil, BUCKET, KEY, 3600);
    source.shutdown(); // stop the scheduler so its startup tick can't consume a stubbed result
    try {
      source.refresh(); // good load
      long stampAfterGoodLoad = source.getLastFetchedAtMillis();
      source.refresh(); // fetch throws — must retain

      assertEquals(
          source
              .getMatrix()
              .getEntriesForServer("1.5.0")
              .getConnectorEntry("snowflake")
              .getDefaultVersion(),
          "1.5.0.5",
          "matrix must be retained when a later fetch fails");
      assertEquals(
          source.getLastFetchedAtMillis(),
          stampAfterGoodLoad,
          "a failed fetch must not advance the last-fetched timestamp");
    } finally {
      source.shutdown();
    }
  }

  @Test
  public void retainsLastKnownMatrixWhenReparseFails() {
    // A good load followed by a body that parses as JSON but violates the matrix schema (root is
    // an array) hits the dedicated schema-error branch, which must retain the last-known-good
    // matrix.
    GcsUtil gcsUtil = mock(GcsUtil.class);
    when(gcsUtil.getObjectAsString(BUCKET, KEY))
        .thenReturn(MATRIX_JSON)
        .thenReturn("[ {\"snowflake\": {}} ]");

    GcsIngestionCliVersionMatrixSource source =
        new GcsIngestionCliVersionMatrixSource(gcsUtil, BUCKET, KEY, 3600);
    source.shutdown();
    try {
      source.refresh(); // good load
      long stampAfterGoodLoad = source.getLastFetchedAtMillis();
      source.refresh(); // schema violation — must retain

      assertEquals(
          source
              .getMatrix()
              .getEntriesForServer("1.5.0")
              .getConnectorEntry("snowflake")
              .getDefaultVersion(),
          "1.5.0.5",
          "matrix must be retained when a later body fails schema validation");
      assertEquals(
          source.getLastFetchedAtMillis(),
          stampAfterGoodLoad,
          "a rejected (schema-invalid) refresh must not advance the last-fetched timestamp");
    } finally {
      source.shutdown();
    }
  }

  @Test
  public void getMatrixIsEmptyBeforeAnyFetch() {
    // A source whose backing object errors on every read keeps serving the EMPTY matrix rather
    // than throwing on the hot path — resolution falls through to the application default.
    GcsUtil gcsUtil = mock(GcsUtil.class);
    when(gcsUtil.getObjectAsString(BUCKET, KEY)).thenThrow(new RuntimeException("gcs unavailable"));

    GcsIngestionCliVersionMatrixSource source =
        new GcsIngestionCliVersionMatrixSource(gcsUtil, BUCKET, KEY, 3600);
    try {
      source.refresh(); // swallows the error, retains EMPTY
      assertEquals(source.getMatrix().size(), 0, "no entries when the fetch never succeeds");
      assertEquals(source.getLastFetchedAtMillis(), 0L, "no successful fetch means no timestamp");
    } finally {
      source.shutdown();
    }
  }
}
