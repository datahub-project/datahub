package com.linkedin.metadata.ingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.execution.CliVersionSource;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Focused unit tests for {@link CliVersionResolutionHelper}.
 *
 * <p>Covers the precedence ladder (per-source &gt; matrix cohort &gt; matrix connector default &gt;
 * workspace default) and the per-source normalization contract (null, empty, and whitespace-only
 * strings all fall through to the next tier). The whitespace case matches the contract of {@code
 * IngestionUtils.resolveIngestionCliVersion(...)} from #17471 — bootstrap YAML templating can
 * render any of the three, and forwarding them to the executor silently picks the bundled CLI
 * rather than the configured default.
 */
public class CliVersionResolutionHelperTest {

  private static final String DEFAULT_CLI = "0.14.0";
  private static final String SERVER_VERSION = "1.3.1.4";

  @Test
  public void testPerSourceVersionWins() {
    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve(
            "0.13.5", "snowflake", null, DEFAULT_CLI, SERVER_VERSION);

    assertEquals(result.getVersion(), "0.13.5");
    assertEquals(result.getStamp().getSource(), CliVersionSource.PER_SOURCE);
    assertEquals(result.getStamp().getServerVersion(), SERVER_VERSION);
  }

  @Test
  public void testPerSourceWhitespaceIsTrimmed() {
    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve(
            "  0.13.5  ", "snowflake", null, DEFAULT_CLI, SERVER_VERSION);

    assertEquals(result.getVersion(), "0.13.5");
    assertEquals(result.getStamp().getSource(), CliVersionSource.PER_SOURCE);
  }

  @Test
  public void testPerSourceNullFallsThroughToDefault() {
    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve(null, null, null, DEFAULT_CLI, SERVER_VERSION);

    assertEquals(result.getVersion(), DEFAULT_CLI);
    assertEquals(result.getStamp().getSource(), CliVersionSource.WORKSPACE_DEFAULT);
  }

  @Test
  public void testPerSourceEmptyFallsThroughToDefault() {
    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve("", null, null, DEFAULT_CLI, SERVER_VERSION);

    assertEquals(result.getVersion(), DEFAULT_CLI);
    assertEquals(result.getStamp().getSource(), CliVersionSource.WORKSPACE_DEFAULT);
  }

  @Test
  public void testPerSourceWhitespaceOnlyFallsThroughToDefault() {
    // Documents the contract from #17471: a bootstrap YAML field that renders as a blank string
    // must be treated as "unset" so we hit the workspace default rather than passing the blank
    // through to the executor.
    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve("   ", null, null, DEFAULT_CLI, SERVER_VERSION);

    assertEquals(result.getVersion(), DEFAULT_CLI);
    assertEquals(result.getStamp().getSource(), CliVersionSource.WORKSPACE_DEFAULT);
  }

  @Test
  public void testMatrixConnectorDefaultWinsOverWorkspaceDefault() {
    IngestionVersionMatrixService matrixService = Mockito.mock(IngestionVersionMatrixService.class);
    Mockito.when(matrixService.resolveVersionWithSource("snowflake"))
        .thenReturn(
            Optional.of(
                new IngestionVersionMatrixService.MatrixResolution(
                    "0.13.5", IngestionVersionMatrixService.MatrixSourceLevel.CONNECTOR_DEFAULT)));

    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve(
            null, "snowflake", matrixService, DEFAULT_CLI, SERVER_VERSION);

    assertEquals(result.getVersion(), "0.13.5");
    assertEquals(result.getStamp().getSource(), CliVersionSource.MATRIX_CONNECTOR_DEFAULT);
  }

  @Test
  public void testMatrixCohortWinsOverConnectorDefault() {
    IngestionVersionMatrixService matrixService = Mockito.mock(IngestionVersionMatrixService.class);
    Mockito.when(matrixService.resolveVersionWithSource("snowflake"))
        .thenReturn(
            Optional.of(
                new IngestionVersionMatrixService.MatrixResolution(
                    "0.13.6", IngestionVersionMatrixService.MatrixSourceLevel.COHORT)));

    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve(
            null, "snowflake", matrixService, DEFAULT_CLI, SERVER_VERSION);

    assertEquals(result.getVersion(), "0.13.6");
    assertEquals(result.getStamp().getSource(), CliVersionSource.MATRIX_COHORT);
  }

  @Test
  public void testNullConnectorTypeSkipsMatrix() {
    // A malformed test-connection recipe produces a null connector type; we must skip the matrix
    // and fall through to the workspace default rather than throwing.
    IngestionVersionMatrixService matrixService = Mockito.mock(IngestionVersionMatrixService.class);

    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve(null, null, matrixService, DEFAULT_CLI, SERVER_VERSION);

    assertEquals(result.getVersion(), DEFAULT_CLI);
    assertEquals(result.getStamp().getSource(), CliVersionSource.WORKSPACE_DEFAULT);
    Mockito.verifyNoInteractions(matrixService);
  }

  @Test
  public void testNullDefaultStillReturnsStamp() {
    // OSS misconfiguration (defaultCliVersion not set) — we still emit a deterministic stamp so
    // forensic queries see a definite answer rather than a missing field.
    CliVersionResolutionHelper.Result result =
        CliVersionResolutionHelper.resolve(null, null, null, null, SERVER_VERSION);

    assertEquals(result.getVersion(), "");
    assertNotNull(result.getStamp());
    assertEquals(result.getStamp().getSource(), CliVersionSource.WORKSPACE_DEFAULT);
  }
}
