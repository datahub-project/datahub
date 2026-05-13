package com.linkedin.metadata.ingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

/**
 * Direct unit tests for the JSON-schema validation rules in {@link
 * HttpUrlMatrixSource#parseMatrix}.
 *
 * <p>Two layers of validation are tested:
 *
 * <ul>
 *   <li><b>File-level (fail closed)</b>: {@link IllegalArgumentException} for root structure
 *       violations — caller refuses to swap the cache.
 *   <li><b>Entry-level (fail open + log)</b>: bad sub-entries are skipped; good entries around them
 *       are kept. We don't assert on the log lines themselves (brittle) — only on the resulting
 *       {@link Matrix} shape.
 * </ul>
 */
public class HttpUrlMatrixSourceValidationTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // ---------------------------------------------------------------------------
  // File-level (fail closed)
  // ---------------------------------------------------------------------------

  @Test
  public void rootNotObjectThrowsAndCallerRetainsCache() throws Exception {
    // A JSON array at the root is the realistic operator-error case (e.g. they
    // exported a list of versions instead of the keyed object). We refuse to swap.
    // The thrown IllegalArgumentException is the signal HttpUrlMatrixSource.refresh()
    // uses to retain the last-known-good cache.
    JsonNode root = MAPPER.readTree("[ {\"snowflake\": {} } ]");
    assertThrows(IllegalArgumentException.class, () -> HttpUrlMatrixSource.parseMatrix(root));
  }

  @Test
  public void rootNullThrows() {
    assertThrows(IllegalArgumentException.class, () -> HttpUrlMatrixSource.parseMatrix(null));
  }

  // ---------------------------------------------------------------------------
  // Entry-level (fail open + log) — good entries survive bad neighbors
  // ---------------------------------------------------------------------------

  @Test
  public void invalidDefaultVersionIgnoredButCohortsKept() throws Exception {
    // The "_default" is unusable (has a space), but cohorts are well-formed and
    // should still drive cohort matches. The connector falls through to
    // WORKSPACE_DEFAULT when no cohort matches.
    JsonNode root =
        MAPPER.readTree(
            "{\"1.5.0\": {\"snowflake\": {"
                + "\"_default\": \"not a version\","
                + "\"cohorts\": [{\"version\": \"1.5.0.6\", \"deployments\": [\"acme\"]}]"
                + "}}}");
    Matrix m = HttpUrlMatrixSource.parseMatrix(root);
    ConnectorEntry snowflake = m.getEntriesForServer("1.5.0").get("snowflake");
    assertNotNull(snowflake);
    assertNull(
        snowflake.getDefaultVersion(), "invalid _default should be dropped, not stored verbatim");
    assertEquals(snowflake.getCohorts().size(), 1, "cohort should still be present");
    assertEquals(snowflake.getCohorts().get(0).getVersion(), "1.5.0.6");
  }

  @Test
  public void cohortMissingVersionIsSkippedOthersKept() throws Exception {
    // First cohort has no version field — skipped. Second cohort is well-formed — kept.
    JsonNode root =
        MAPPER.readTree(
            "{\"1.5.0\": {\"snowflake\": {\"cohorts\": ["
                + "{\"deployments\": [\"acme\"]},"
                + "{\"version\": \"1.5.0.6\", \"deployments\": [\"acme\"]}"
                + "]}}}");
    Matrix m = HttpUrlMatrixSource.parseMatrix(root);
    ConnectorEntry snowflake = m.getEntriesForServer("1.5.0").get("snowflake");
    assertEquals(snowflake.getCohorts().size(), 1, "first cohort (no version) should be dropped");
    assertEquals(snowflake.getCohorts().get(0).getVersion(), "1.5.0.6");
  }

  @Test
  public void cohortWithGarbageVersionIsSkipped() throws Exception {
    // Operator pasted a string with HTML — pattern rejects it.
    JsonNode root =
        MAPPER.readTree(
            "{\"1.5.0\": {\"snowflake\": {\"cohorts\": ["
                + "{\"version\": \"<script>alert(1)</script>\", \"deployments\": [\"acme\"]}"
                + "]}}}");
    Matrix m = HttpUrlMatrixSource.parseMatrix(root);
    ConnectorEntry snowflake = m.getEntriesForServer("1.5.0").get("snowflake");
    assertTrue(
        snowflake.getCohorts().isEmpty(), "cohort with invalid version pattern should be dropped");
  }

  @Test
  public void permissiveVersionPatternAcceptsRealPyPiVersions() throws Exception {
    // Make sure we don't over-reject. Each is a real shape that appears on PyPI for acryl-datahub
    // or similar. Includes rc, post, dev, and PEP 440 epoch prefix.
    String[] realVersions = {
      "1.5.0.19", "1.5.0.6rc1", "1.5.0.13.post1", "1!0.0.0.dev0", "0.14.0.6rc3"
    };
    StringBuilder cohorts = new StringBuilder();
    for (int i = 0; i < realVersions.length; i++) {
      if (i > 0) cohorts.append(",");
      cohorts
          .append("{\"version\": \"")
          .append(realVersions[i])
          .append("\", \"deployments\": [\"d")
          .append(i)
          .append("\"]}");
    }
    JsonNode root =
        MAPPER.readTree("{\"1.5.0\": {\"snowflake\": {\"cohorts\": [" + cohorts + "]}}}");
    Matrix m = HttpUrlMatrixSource.parseMatrix(root);
    assertEquals(
        m.getEntriesForServer("1.5.0").get("snowflake").getCohorts().size(),
        realVersions.length,
        "all real PyPI-style versions should pass the permissive pattern");
  }

  @Test
  public void connectorValueNotObjectIsSkippedOthersKept() throws Exception {
    // "snowflake" got assigned an array by mistake — drop it. "bigquery" is fine — keep it.
    JsonNode root =
        MAPPER.readTree(
            "{\"1.5.0\": {"
                + "\"snowflake\": [\"oops\", \"this is wrong\"],"
                + "\"bigquery\": {\"_default\": \"1.4.0.3\"}"
                + "}}");
    Matrix m = HttpUrlMatrixSource.parseMatrix(root);
    assertFalse(
        m.getEntriesForServer("1.5.0").containsKey("snowflake"),
        "malformed connector entry should be dropped");
    assertNotNull(
        m.getEntriesForServer("1.5.0").get("bigquery"),
        "well-formed sibling connector should survive");
    assertEquals(m.getEntriesForServer("1.5.0").get("bigquery").getDefaultVersion(), "1.4.0.3");
  }

  @Test
  public void wellFormedMatrixParsesUnchanged() throws Exception {
    // Regression test — the happy-path schema we documented in the class Javadoc still parses.
    // Locks in the contract so future tightening of validation doesn't accidentally reject valid
    // input.
    JsonNode root =
        MAPPER.readTree(
            "{\"1.5.0\": {"
                + "\"snowflake\": {"
                + "  \"_default\": \"1.5.0.5\","
                + "  \"cohorts\": [{\"version\": \"1.5.0.6\", \"deployments\": [\"acme\", \"beta\"]}]"
                + "},"
                + "\"bigquery\": {\"_default\": \"1.4.0.3\"}"
                + "}}");
    Matrix m = HttpUrlMatrixSource.parseMatrix(root);
    ConnectorEntry snowflake = m.getEntriesForServer("1.5.0").get("snowflake");
    assertEquals(snowflake.getDefaultVersion(), "1.5.0.5");
    assertEquals(snowflake.getCohorts().size(), 1);
    Cohort cohort = snowflake.getCohorts().get(0);
    assertEquals(cohort.getVersion(), "1.5.0.6");
    assertEquals(cohort.getDeployments().size(), 2);
    assertTrue(cohort.getDeployments().contains("acme"));
    assertTrue(cohort.getDeployments().contains("beta"));

    ConnectorEntry bigquery = m.getEntriesForServer("1.5.0").get("bigquery");
    assertEquals(bigquery.getDefaultVersion(), "1.4.0.3");
    assertTrue(bigquery.getCohorts().isEmpty());
  }
}
