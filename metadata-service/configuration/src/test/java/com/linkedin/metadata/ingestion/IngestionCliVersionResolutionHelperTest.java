package com.linkedin.metadata.ingestion;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.execution.CliVersionSource;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Focused unit tests for {@link IngestionCliVersionResolutionHelper}.
 *
 * <p>Covers the precedence ladder (source config override &gt; matrix cohort &gt; matrix connector
 * default &gt; application default) and the per-source normalization contract (null, empty, and
 * whitespace-only strings all fall through to the next tier). The whitespace case matters because
 * bootstrap YAML templating renders {@code version: "{{ config.version }}"} as three spaces when
 * the source has no version pin — forwarding that verbatim to the executor would silently pin to
 * the bundled CLI rather than the configured default.
 *
 * <p>The matrix service is always a wired Spring bean in production, so these tests pass a mock
 * (rather than {@code null}); the GMS server version stamped on the audit record is read from
 * {@link IngestionCliVersionMatrixService#getServerVersion()}.
 */
public class IngestionCliVersionResolutionHelperTest {

  private static final String DEFAULT_CLI = "0.14.0";
  private static final String SERVER_VERSION = "1.3.1.4";

  /** A matrix service mock that reports {@link #SERVER_VERSION} and resolves nothing by default. */
  private static IngestionCliVersionMatrixService matrixService() {
    IngestionCliVersionMatrixService svc = Mockito.mock(IngestionCliVersionMatrixService.class);
    Mockito.when(svc.getServerVersion()).thenReturn(SERVER_VERSION);
    return svc;
  }

  @Test
  public void testPerSourceVersionWins() {
    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve(
            "0.13.5", "snowflake", matrixService(), DEFAULT_CLI);

    assertEquals(result.getVersion(), "0.13.5");
    assertEquals(result.getStamp().getSource(), CliVersionSource.SOURCE_CONFIG_OVERRIDE);
    assertEquals(result.getStamp().getServerVersion(), SERVER_VERSION);
  }

  @Test
  public void testPerSourceWhitespaceIsTrimmed() {
    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve(
            "  0.13.5  ", "snowflake", matrixService(), DEFAULT_CLI);

    assertEquals(result.getVersion(), "0.13.5");
    assertEquals(result.getStamp().getSource(), CliVersionSource.SOURCE_CONFIG_OVERRIDE);
  }

  @Test
  public void testPerSourceNullFallsThroughToDefault() {
    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve(null, null, matrixService(), DEFAULT_CLI);

    assertEquals(result.getVersion(), DEFAULT_CLI);
    assertEquals(result.getStamp().getSource(), CliVersionSource.APPLICATION_DEFAULT);
  }

  @Test
  public void testPerSourceEmptyFallsThroughToDefault() {
    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve("", null, matrixService(), DEFAULT_CLI);

    assertEquals(result.getVersion(), DEFAULT_CLI);
    assertEquals(result.getStamp().getSource(), CliVersionSource.APPLICATION_DEFAULT);
  }

  @Test
  public void testPerSourceWhitespaceOnlyFallsThroughToDefault() {
    // A bootstrap YAML field rendered through Mustache as `version: "   "` (3 spaces, what we get
    // when the source has no version pin) must be treated as "unset" — otherwise we'd forward the
    // blank string to the executor, which would silently use its bundled CLI rather than the
    // configured application default.
    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve("   ", null, matrixService(), DEFAULT_CLI);

    assertEquals(result.getVersion(), DEFAULT_CLI);
    assertEquals(result.getStamp().getSource(), CliVersionSource.APPLICATION_DEFAULT);
  }

  @Test
  public void testMatrixConnectorDefaultWinsOverApplicationDefault() {
    IngestionCliVersionMatrixService matrixService = matrixService();
    Mockito.when(matrixService.resolveVersionWithSource("snowflake"))
        .thenReturn(
            Optional.of(
                new IngestionCliVersionMatrixService.MatrixResolution(
                    "0.13.5",
                    IngestionCliVersionMatrixService.MatrixSourceLevel.CONNECTOR_DEFAULT)));

    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve(null, "snowflake", matrixService, DEFAULT_CLI);

    assertEquals(result.getVersion(), "0.13.5");
    assertEquals(result.getStamp().getSource(), CliVersionSource.MATRIX_CONNECTOR_DEFAULT);
  }

  @Test
  public void testMatrixCohortWinsOverConnectorDefault() {
    IngestionCliVersionMatrixService matrixService = matrixService();
    Mockito.when(matrixService.resolveVersionWithSource("snowflake"))
        .thenReturn(
            Optional.of(
                new IngestionCliVersionMatrixService.MatrixResolution(
                    "0.13.6", IngestionCliVersionMatrixService.MatrixSourceLevel.COHORT)));

    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve(null, "snowflake", matrixService, DEFAULT_CLI);

    assertEquals(result.getVersion(), "0.13.6");
    assertEquals(result.getStamp().getSource(), CliVersionSource.MATRIX_COHORT);
  }

  @Test
  public void testNullConnectorTypeSkipsMatrix() {
    // A malformed test-connection recipe produces a null connector type; we must skip the matrix
    // and fall through to the application default rather than throwing.
    IngestionCliVersionMatrixService matrixService = matrixService();

    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve(null, null, matrixService, DEFAULT_CLI);

    assertEquals(result.getVersion(), DEFAULT_CLI);
    assertEquals(result.getStamp().getSource(), CliVersionSource.APPLICATION_DEFAULT);
    // The matrix is never consulted when the connector type is unknown (getServerVersion is still
    // read for the audit stamp, so we assert specifically on the resolution call).
    Mockito.verify(matrixService, Mockito.never()).resolveVersionWithSource(any());
  }

  @Test
  public void testNullDefaultStillReturnsStamp() {
    // OSS misconfiguration (defaultCliVersion not set) — we still emit a deterministic stamp so
    // forensic queries see a definite answer rather than a missing field.
    IngestionCliVersionResolutionHelper.Result result =
        IngestionCliVersionResolutionHelper.resolve(null, null, matrixService(), null);

    assertEquals(result.getVersion(), "");
    assertNotNull(result.getStamp());
    assertEquals(result.getStamp().getSource(), CliVersionSource.APPLICATION_DEFAULT);
  }
}
