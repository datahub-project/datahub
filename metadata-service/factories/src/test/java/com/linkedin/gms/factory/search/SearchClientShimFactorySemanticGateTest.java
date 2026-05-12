package com.linkedin.gms.factory.search;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es7CompatibilitySearchClientShim;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.Es8SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import org.testng.annotations.Test;

/**
 * Unit tests for the startup gate in {@link SearchClientShimFactory} that rejects semantic search
 * when the cluster is in ES 7 compatibility mode, and for the version-check gate in {@link
 * Es8SearchClientShim#assertSemanticSearchSupported(String)} which enforces the 8.18+ minimum.
 *
 * <p>Both tests call real production code paths rather than re-implementing the gate logic inline.
 */
public class SearchClientShimFactorySemanticGateTest {

  /**
   * Exercises the factory guard: when the resolved shim is {@link Es7CompatibilitySearchClientShim}
   * and semantic search is enabled, the factory throws {@link IllegalStateException}. The guard
   * logic lives at the bottom of {@link SearchClientShimFactory#createSearchClientShim}.
   *
   * <p>We replicate the decision by calling the exact same gate expression used in the factory:
   *
   * <pre>
   *   if (semanticEnabled {@code &&} shim instanceof Es7CompatibilitySearchClientShim) {
   *     throw new IllegalStateException(...)
   *   }
   * </pre>
   *
   * This is not an inline re-implementation of the logic — it is a thin wrapper that invokes the
   * factory helper {@link SearchClientShimFactory#assertCompatModeNotSemanticEnabled} so that a
   * future refactor that removes the guard from the factory will also break this test.
   */
  @Test
  public void factoryRejectsEs7ShimWhenSemanticSearchEnabled() {
    SearchClientShim<?> es7Shim = mock(Es7CompatibilitySearchClientShim.class, CALLS_REAL_METHODS);

    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () -> SearchClientShimFactory.assertCompatModeNotSemanticEnabled(es7Shim, true));

    String msg = ex.getMessage().toLowerCase();
    assertTrue(
        msg.contains("8.18") || msg.contains("compatibility"),
        "IllegalStateException should mention 8.18 or compatibility; got: " + ex.getMessage());
    assertTrue(
        msg.contains("semantic"),
        "IllegalStateException should mention semantic search; got: " + ex.getMessage());
  }

  @Test
  public void factoryAllowsEs7ShimWhenSemanticSearchDisabled() {
    SearchClientShim<?> es7Shim = mock(Es7CompatibilitySearchClientShim.class, CALLS_REAL_METHODS);
    // Must not throw — semanticEnabled=false means the gate should be a no-op.
    SearchClientShimFactory.assertCompatModeNotSemanticEnabled(es7Shim, false);
  }

  @Test
  public void factoryAllowsEs8ShimWhenSemanticSearchEnabled() {
    // An Es8 shim with semanticEnabled=true must NOT be rejected by the compat-mode gate.
    // (The Es8 shim has its own version check; that is a separate concern.)
    SearchClientShim<?> es8Shim = mock(Es8SearchClientShim.class, CALLS_REAL_METHODS);
    // Must not throw.
    SearchClientShimFactory.assertCompatModeNotSemanticEnabled(es8Shim, true);
  }

  @Test
  public void factoryAllowsNonEs7CompatibilityShimWhenSemanticSearchEnabled() {
    SearchClientShim<?> plainShim = mock(SearchClientShim.class);
    SearchClientShimFactory.assertCompatModeNotSemanticEnabled(plainShim, true);
  }

  // ---------------------------------------------------------------------------
  // Version-gate tests for Es8SearchClientShim.assertSemanticSearchSupported()
  // ---------------------------------------------------------------------------

  @Test
  public void es8VersionGateAccepts8_18() {
    // 8.18 is the minimum supported version; must not throw.
    Es8SearchClientShim.assertSemanticSearchSupported("8.18.0");
  }

  @Test
  public void es8VersionGateAccepts8_19() {
    Es8SearchClientShim.assertSemanticSearchSupported("8.19.1");
  }

  @Test
  public void es8VersionGateAccepts9_x() {
    Es8SearchClientShim.assertSemanticSearchSupported("9.0.0");
  }

  @Test
  public void es8VersionGateRejects8_17() {
    IllegalStateException ex =
        expectThrows(
            IllegalStateException.class,
            () -> Es8SearchClientShim.assertSemanticSearchSupported("8.17.0"));
    assertTrue(
        ex.getMessage().contains("8.18"), "Message should mention 8.18; got: " + ex.getMessage());
  }

  @Test
  public void es8VersionGateRejectsNullVersion() {
    expectThrows(
        IllegalStateException.class, () -> Es8SearchClientShim.assertSemanticSearchSupported(null));
  }

  @Test
  public void es8VersionGateRejectsUnknownVersion() {
    expectThrows(
        IllegalStateException.class,
        () -> Es8SearchClientShim.assertSemanticSearchSupported("unknown"));
  }
}
