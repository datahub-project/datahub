package com.linkedin.metadata.ingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link PollingIngestionCliVersionMatrixSource}. The backend is stubbed at the
 * {@link MatrixDocumentReader} boundary, so this one suite covers the refresh contract for every
 * backend — HTTP and object storage both poll through this class, and the readers themselves are
 * tested separately. JSON parsing and validation are covered by {@link
 * IngestionCliVersionMatrixParserTest}.
 *
 * <p>The constructor loads synchronously, so the first read is always the "good load"; {@code
 * refresh()} is then driven directly for the failure/retention scenarios rather than waiting for
 * the periodic background tick, so the assertions are deterministic.
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
    // Stop the periodic scheduler so no background tick can race the reads below.
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

    // The constructor's synchronous initial load is the "good load" here.
    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    source.shutdown(); // stop the periodic scheduler; the initial load already happened
    long stampAfterGoodLoad = source.getLastFetchedAtMillis();

    // Arm the failure only after the good load has landed: if the scheduler's startup tick still
    // raced ahead of shutdown() above, it can only have consumed a good read, never this one.
    reader.failNextWith(new RuntimeException("storage unavailable"));
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

    // The constructor's synchronous initial load is the "good load" here.
    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    source.shutdown();
    source.refresh(); // schema-invalid — must retain

    assertEquals(
        defaultVersion(source),
        "1.5.0.5",
        "matrix must be retained when a later document violates the schema");
  }

  @Test
  public void retainsLastKnownMatrixWhenDocumentIsNotJson() {
    StubReader reader = new StubReader(MATRIX_JSON, "not json at all");

    // The constructor's synchronous initial load is the "good load" here.
    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    source.shutdown();
    source.refresh(); // not JSON — must retain

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

    // The constructor's synchronous initial load is the "good load" here.
    PollingIngestionCliVersionMatrixSource source = newSource(reader);
    source.shutdown();
    source.refresh(); // Error — must be swallowed, not propagated
    source.refresh(); // the loop still works

    assertEquals(defaultVersion(source), "1.5.0.5", "an Error must not stop later refreshes");
  }

  @Test
  public void constructionDoesNotBlockPastTheInitialLoadBound() throws InterruptedException {
    // A backend slower than the bound must not stall the constructing thread (GMS startup); the
    // load keeps running on the background executor and eventually lands.
    BlockingReader reader = new BlockingReader(MATRIX_JSON);

    long startNanos = System.nanoTime();
    PollingIngestionCliVersionMatrixSource source =
        new PollingIngestionCliVersionMatrixSource(reader, 3600, 50, TimeUnit.MILLISECONDS);
    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
    try {
      assertTrue(
          elapsedMillis < 1000,
          "construction must return once the bound elapses, not wait for the slow read");

      reader.release();
      assertTrue(
          awaitLoad(source, 2000), "the background load must still land once the backend responds");
      assertEquals(defaultVersion(source), "1.5.0.5");
    } finally {
      source.shutdown();
    }
  }

  @Test
  public void interruptDuringInitialLoadWaitReturnsWithoutBlockingAndReassertsTheFlag()
      throws InterruptedException {
    // Interrupting the constructing thread mid-wait must not hang it — the bounded get() surfaces
    // InterruptedException, and the constructor must reassert the flag rather than swallow it.
    BlockingReader reader = new BlockingReader(MATRIX_JSON);
    AtomicBoolean interruptedAfterConstruction = new AtomicBoolean(false);
    AtomicReference<PollingIngestionCliVersionMatrixSource> sourceRef = new AtomicReference<>();
    CountDownLatch constructed = new CountDownLatch(1);

    Thread constructingThread =
        new Thread(
            () -> {
              PollingIngestionCliVersionMatrixSource source =
                  new PollingIngestionCliVersionMatrixSource(reader, 3600, 5, TimeUnit.SECONDS);
              interruptedAfterConstruction.set(Thread.currentThread().isInterrupted());
              sourceRef.set(source);
              constructed.countDown();
            });
    constructingThread.start();
    // Give the thread time to reach the blocked future.get() before interrupting it.
    Thread.sleep(100);
    constructingThread.interrupt();

    assertTrue(
        constructed.await(2, TimeUnit.SECONDS),
        "construction must return once interrupted, not hang for the full bound");
    assertTrue(
        interruptedAfterConstruction.get(),
        "the interrupt flag must be reasserted on the constructing thread");

    PollingIngestionCliVersionMatrixSource source = sourceRef.get();
    try {
      reader.release();
      assertTrue(
          awaitLoad(source, 2000), "the background load must still land after the interrupt");
      assertEquals(defaultVersion(source), "1.5.0.5");
    } finally {
      source.shutdown();
    }
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

  private static boolean awaitLoad(
      PollingIngestionCliVersionMatrixSource source, long timeoutMillis)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (source.getLastFetchedAtMillis() > 0) {
        return true;
      }
      Thread.sleep(10);
    }
    return false;
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

  /**
   * Blocks in {@link #read()} until released, simulating a backend slower than the initial-load
   * bound. Hand-rolled so the block is a real thread wait rather than a mocked sleep.
   */
  private static final class BlockingReader implements MatrixDocumentReader {

    private final String body;
    private final CountDownLatch release = new CountDownLatch(1);

    BlockingReader(String body) {
      this.body = body;
    }

    /** Lets the read blocked in {@link #read()} proceed, as if the slow backend just responded. */
    void release() {
      release.countDown();
    }

    @Override
    public String read() throws InterruptedException {
      release.await();
      return body;
    }

    @Override
    public String displayUri() {
      return "blocking://stub";
    }
  }
}
