package com.linkedin.metadata.ingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link PollingIngestionCliVersionMatrixSource}. The backend is stubbed at the
 * {@link MatrixDocumentReader} boundary, so this one suite covers the refresh contract for every
 * backend — HTTP and object storage both poll through this class, and the readers themselves are
 * tested separately. JSON parsing and validation are covered by {@link
 * IngestionCliVersionMatrixParserTest}.
 *
 * <p>{@code refresh()} is driven directly rather than waiting for the background scheduler so the
 * assertions are deterministic.
 */
public class PollingIngestionCliVersionMatrixSourceTest {

  private static final String URI = "s3://cli-version-matrix/matrix.json";
  private static final String MATRIX_JSON =
      "{\"1.5.0\": {\"snowflake\": {\"_default\": \"1.5.0.5\"}}}";

  @Test
  public void refreshLoadsAndCachesMatrix() {
    StubReader reader = new StubReader(MATRIX_JSON);

    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    try {
      source.refresh();

      assertEquals(defaultVersion(source), "1.5.0.5");
      assertTrue(
          source.getLastFetchedAtMillis() > 0,
          "successful fetch should stamp the last-fetched timestamp");
      assertEquals(source.displayUri(), URI, "the polled location must be reportable");
    } finally {
      source.shutdown();
    }
  }

  @Test
  public void getMatrixServesCachedInstanceBetweenRefreshes() {
    StubReader reader = new StubReader(MATRIX_JSON);

    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    // Stop the background scheduler up front so its startup tick can't race the reads below;
    // refresh() is a plain method and still populates the cache deterministically.
    source.shutdown();
    source.refresh();
    IngestionCliVersionMatrix cached = source.getMatrix();
    int readsAfterRefresh = reader.reads.get();

    // Between refreshes getMatrix() is a pure in-memory read: it returns the very same cached
    // instance every time and never re-reads the backend.
    for (int i = 0; i < 5; i++) {
      assertSame(
          source.getMatrix(),
          cached,
          "getMatrix() must serve the cached instance between refreshes, not re-read");
    }
    assertEquals(reader.reads.get(), readsAfterRefresh, "getMatrix() must not touch the backend");
  }

  @Test
  public void retainsLastKnownMatrixWhenReadFails() {
    // A good load followed by a read failure must retain the previously-loaded matrix (not blank
    // it)
    // and leave the last-fetched timestamp untouched — in-flight resolutions never see a flap.
    StubReader reader = new StubReader(MATRIX_JSON);
    reader.failNextWith(new RuntimeException("storage unavailable"));

    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    source.shutdown(); // stop the scheduler so its startup tick can't consume a stubbed result
    source.refresh(); // good load
    long stampAfterGoodLoad = source.getLastFetchedAtMillis();
    source.refresh(); // read throws — must retain

    assertEquals(
        defaultVersion(source), "1.5.0.5", "matrix must be retained when a later read fails");
    assertEquals(
        source.getLastFetchedAtMillis(),
        stampAfterGoodLoad,
        "a failed read must not advance the last-fetched timestamp");
  }

  @Test
  public void retainsLastKnownMatrixWhenDocumentIsSchemaInvalid() {
    // A file-level schema violation (root is not a JSON object) must not blank the cache either —
    // the parser throws and the source refuses to swap.
    StubReader reader = new StubReader(MATRIX_JSON, "[\"not\", \"a\", \"map\"]");

    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    source.shutdown();
    source.refresh(); // good load
    source.refresh(); // schema-invalid — must retain

    assertEquals(
        defaultVersion(source),
        "1.5.0.5",
        "matrix must be retained when a later document violates the schema");
  }

  @Test
  public void retainsLastKnownMatrixWhenDocumentIsNotJson() {
    StubReader reader = new StubReader(MATRIX_JSON, "not json at all");

    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    source.shutdown();
    source.refresh();
    source.refresh();

    assertEquals(
        defaultVersion(source),
        "1.5.0.5",
        "matrix must be retained when a later document is not JSON");
  }

  @Test
  public void refreshSurvivesAnErrorSoTheScheduleIsNotCancelled() {
    // scheduleAtFixedRate silently cancels every future run if a task lets anything escape, so an
    // Error from a huge or deeply-nested document must not permanently freeze the refresh loop.
    StubReader reader = new StubReader(MATRIX_JSON);
    reader.failNextWith(new StackOverflowError("deeply nested document"));

    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    source.shutdown();
    source.refresh(); // Error — must be swallowed, not propagated
    source.refresh(); // the loop still works

    assertEquals(defaultVersion(source), "1.5.0.5", "an Error must not stop later refreshes");
  }

  // ---------------------------------------------------------------------------
  // Failure classification — decides which fix an operator is pointed at
  // ---------------------------------------------------------------------------

  @Test
  public void classifyUnwrapsTheVerdictAReaderAlreadyAttached() {
    // Readers classify what they can; a backend client may re-wrap on the way out, so the chain is
    // walked rather than only the top-level type inspected.
    MatrixReadException classified =
        new MatrixReadException(MatrixRefreshFailure.PERMISSION, "HTTP 403");

    assertEquals(
        PollingIngestionCliVersionMatrixSource.classify(classified),
        MatrixRefreshFailure.PERMISSION);
    assertEquals(
        PollingIngestionCliVersionMatrixSource.classify(
            new RuntimeException("wrapped", classified)),
        MatrixRefreshFailure.PERMISSION,
        "a re-wrapped verdict must still be found");
  }

  @Test
  public void classifyTreatsUnclassifiedFailuresAsTransport() {
    // Retried on the next tick, so it must not be reported as something an operator has to go fix.
    assertEquals(
        PollingIngestionCliVersionMatrixSource.classify(new RuntimeException("connection reset")),
        MatrixRefreshFailure.TRANSPORT);
  }

  @Test
  public void classifyTerminatesOnACyclicCauseChain() {
    // A self-referential chain must not spin the bounded walk.
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);

    assertEquals(
        PollingIngestionCliVersionMatrixSource.classify(a), MatrixRefreshFailure.TRANSPORT);
  }

  private static String defaultVersion(PollingIngestionCliVersionMatrixSource source) {
    return source
        .getMatrix()
        .getEntriesForServer("1.5.0")
        .getConnectorEntry("snowflake")
        .getDefaultVersion();
  }

  private static PollingIngestionCliVersionMatrixSource newSource(MatrixDocumentReader reader) {
    return new PollingIngestionCliVersionMatrixSource(reader, 3600);
  }

  /**
   * Serves queued bodies in order, repeating the last one once exhausted, and can be told to throw
   * on the next read. Hand-rolled rather than mocked so a queued {@link Error} is expressible.
   */
  private static final class StubReader implements MatrixDocumentReader {

    private final Deque<String> bodies;
    private final AtomicInteger reads = new AtomicInteger();
    private Throwable failNext;
    private String lastBody;

    StubReader(String... bodies) {
      this.bodies = new ArrayDeque<>(List.of(bodies));
    }

    void failNextWith(Throwable t) {
      this.failNext = t;
    }

    @Override
    public String read() throws Exception {
      reads.incrementAndGet();
      if (lastBody != null && failNext != null) {
        Throwable t = failNext;
        failNext = null;
        if (t instanceof Error error) {
          throw error;
        }
        throw (Exception) t;
      }
      if (!bodies.isEmpty()) {
        lastBody = bodies.poll();
      }
      return lastBody;
    }

    @Override
    public String displayUri() {
      return URI;
    }
  }
}
