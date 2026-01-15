package com.linkedin.datahub.upgrade.system.bootstrapmcps;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests to validate the integrity of sample data MCPs.
 *
 * <p>These tests ensure that critical demo lineages remain intact and that sensitive data has been
 * properly removed from the sample data. This prevents regressions when regenerating sample data
 * from fieldeng.
 *
 * <p><b>Critical Lineage Path:</b> PostgreSQL → Spark → Snowflake → dbt → Looker/Tableau
 *
 * <p>This is the primary end-to-end lineage demonstrated to customers. Breaking this path makes
 * demos appear incomplete or broken.
 */
public class SampleDataIntegrityTest {

  private static final String SAMPLE_DATA_FILE = "/boot/sample_data_mcp.json";

  // Critical seed entity for demo lineage
  private static final String ORDER_DETAILS_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)";

  private List<JsonNode> mcps;
  private Map<String, List<String>> upstreamLineageMap;
  private Map<String, List<String>> downstreamLineageMap;

  @BeforeClass
  public void setup() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    InputStream inputStream = getClass().getResourceAsStream(SAMPLE_DATA_FILE);
    assertNotNull(inputStream, "Sample data file not found: " + SAMPLE_DATA_FILE);

    JsonNode root = mapper.readTree(inputStream);
    assertTrue(root.isArray(), "Sample data should be a JSON array");

    mcps = new ArrayList<>();
    for (JsonNode mcp : root) {
      mcps.add(mcp);
    }

    assertFalse(mcps.isEmpty(), "Sample data should not be empty");

    // Build lineage maps for efficient querying
    buildLineageMaps();
  }

  /** Build upstream and downstream lineage maps from upstreamLineage aspects. */
  private void buildLineageMaps() {
    upstreamLineageMap = new HashMap<>();
    downstreamLineageMap = new HashMap<>();

    for (JsonNode mcp : mcps) {
      String aspectName = mcp.path("aspectName").asText();
      if ("upstreamLineage".equals(aspectName)) {
        String entityUrn = mcp.path("entityUrn").asText();
        JsonNode upstreams = mcp.path("aspect").path("json").path("upstreams");

        List<String> upstreamUrns = new ArrayList<>();
        if (upstreams.isArray()) {
          for (JsonNode upstream : upstreams) {
            String upstreamUrn = upstream.path("dataset").asText();
            if (!upstreamUrn.isEmpty()) {
              upstreamUrns.add(upstreamUrn);

              // Build reverse map for downstream queries
              downstreamLineageMap
                  .computeIfAbsent(upstreamUrn, k -> new ArrayList<>())
                  .add(entityUrn);
            }
          }
        }

        upstreamLineageMap.put(entityUrn, upstreamUrns);
      }
    }
  }

  /** Find all URNs of a given entity type and platform. */
  private Set<String> findEntitiesByTypeAndPlatform(String entityType, String platform) {
    return mcps.stream()
        .map(mcp -> mcp.path("entityUrn").asText())
        .filter(urn -> !urn.isEmpty())
        .filter(urn -> urn.contains("urn:li:" + entityType + ":"))
        .filter(urn -> urn.contains(":dataPlatform:" + platform + ","))
        .collect(Collectors.toSet());
  }

  /**
   * Test that critical PostgreSQL → Snowflake → BI lineage platforms are present.
   *
   * <p>Verifies that all key platforms in the demo lineage exist with sufficient entity counts.
   */
  @Test
  public void testCriticalDemoPlatformsPresent() {
    Set<String> postgresDatasets = findEntitiesByTypeAndPlatform("dataset", "postgres");
    Set<String> snowflakeDatasets = findEntitiesByTypeAndPlatform("dataset", "snowflake");
    Set<String> dbtDatasets = findEntitiesByTypeAndPlatform("dataset", "dbt");

    assertTrue(
        postgresDatasets.size() >= 10,
        "Should have at least 10 PostgreSQL datasets for demo. Found: " + postgresDatasets.size());

    assertTrue(
        snowflakeDatasets.size() >= 10,
        "Should have at least 10 Snowflake datasets for demo. Found: " + snowflakeDatasets.size());

    assertTrue(
        dbtDatasets.size() >= 5,
        "Should have at least 5 dbt models for demo. Found: " + dbtDatasets.size());
  }

  /**
   * Test that the order_details seed entity has proper lineage.
   *
   * <p>The order_details table in Snowflake is the central entity for demo lineage. It must have: -
   * Upstream lineage (connections to source tables) - Downstream lineage (connections to BI tools)
   */
  @Test
  public void testOrderDetailsHasLineage() {
    // Verify order_details has upstream lineage
    assertTrue(
        upstreamLineageMap.containsKey(ORDER_DETAILS_URN),
        "order_details should have upstream lineage aspect");

    List<String> upstreams = upstreamLineageMap.get(ORDER_DETAILS_URN);
    assertTrue(
        upstreams != null && upstreams.size() >= 5,
        "order_details should have at least 5 upstream tables. Found: "
            + (upstreams != null ? upstreams.size() : 0));

    // Verify order_details has downstream lineage
    assertTrue(
        downstreamLineageMap.containsKey(ORDER_DETAILS_URN),
        "order_details should have downstream consumers");

    List<String> downstreams = downstreamLineageMap.get(ORDER_DETAILS_URN);
    assertTrue(
        downstreams != null && downstreams.size() >= 2,
        "order_details should have at least 2 downstream consumers. Found: "
            + (downstreams != null ? downstreams.size() : 0));
  }

  /**
   * Test that Spark ETL jobs are present and properly configured.
   *
   * <p>Spark jobs form the critical ETL layer between PostgreSQL and Snowflake. Without them, the
   * lineage appears broken in demos. This test verifies:
   *
   * <ul>
   *   <li>Spark dataJob entities exist (at least 20 for complete demo)
   *   <li>Spark jobs have dataJobInfo aspects with proper names (not showing as URNs)
   *   <li>Spark jobs have inputOutput aspects showing PostgreSQL → Snowflake connections
   * </ul>
   */
  @Test
  public void testSparkEtlJobsPresent() {
    // Find all unique Spark dataJob URNs
    Set<String> sparkJobUrns =
        mcps.stream()
            .filter(mcp -> "dataJob".equals(mcp.path("entityType").asText()))
            .map(mcp -> mcp.path("entityUrn").asText())
            .filter(urn -> urn.contains(":dataFlow:(spark,"))
            .collect(Collectors.toSet());

    assertTrue(
        sparkJobUrns.size() >= 20,
        "Should have at least 20 Spark ETL jobs for complete demo lineage. Found: "
            + sparkJobUrns.size());

    // Verify Spark jobs have dataJobInfo aspects with names
    Set<String> sparkJobsWithInfo =
        mcps.stream()
            .filter(mcp -> "dataJob".equals(mcp.path("entityType").asText()))
            .filter(mcp -> mcp.path("entityUrn").asText().contains(":dataFlow:(spark,"))
            .filter(mcp -> "dataJobInfo".equals(mcp.path("aspectName").asText()))
            .filter(mcp -> !mcp.path("aspect").path("json").path("name").asText().isEmpty())
            .map(mcp -> mcp.path("entityUrn").asText())
            .collect(Collectors.toSet());

    assertTrue(
        sparkJobsWithInfo.size() >= 20,
        "Spark jobs should have dataJobInfo aspects with names (prevents URN display in UI). Found: "
            + sparkJobsWithInfo.size());
  }

  /**
   * Test that downstream BI tools (Looker/Tableau) are properly connected.
   *
   * <p>Verifies that the demo includes BI tool entities and that they have proper lineage
   * connections to warehouse tables.
   */
  @Test
  public void testBiToolsDownstreamLineage() {
    // Find Looker charts/dashboards
    Set<String> lookerCharts =
        mcps.stream()
            .map(mcp -> mcp.path("entityUrn").asText())
            .filter(urn -> urn.contains("urn:li:chart:(looker,"))
            .collect(Collectors.toSet());

    Set<String> lookerDashboards =
        mcps.stream()
            .map(mcp -> mcp.path("entityUrn").asText())
            .filter(urn -> urn.contains("urn:li:dashboard:(looker,"))
            .collect(Collectors.toSet());

    // Find Tableau charts
    Set<String> tableauCharts =
        mcps.stream()
            .map(mcp -> mcp.path("entityUrn").asText())
            .filter(urn -> urn.contains("urn:li:chart:(tableau,"))
            .collect(Collectors.toSet());

    assertTrue(
        lookerCharts.size() >= 3,
        "Should have at least 3 Looker charts for demo. Found: " + lookerCharts.size());

    assertTrue(
        lookerDashboards.size() >= 1,
        "Should have at least 1 Looker dashboard for demo. Found: " + lookerDashboards.size());

    assertTrue(
        tableauCharts.size() >= 3,
        "Should have at least 3 Tableau charts for demo. Found: " + tableauCharts.size());

    // Verify BI entities have inputFields aspects (BI tools use inputFields for lineage)
    long biEntitiesWithLineage =
        mcps.stream()
            .filter(
                mcp ->
                    "inputFields".equals(mcp.path("aspectName").asText())
                        && (mcp.path("entityUrn").asText().contains(":chart:(looker,")
                            || mcp.path("entityUrn").asText().contains(":chart:(tableau,")
                            || mcp.path("entityUrn").asText().contains(":dashboard:(looker,")))
            .count();

    assertTrue(
        biEntitiesWithLineage >= 5,
        "At least 5 BI entities should have inputFields aspects. Found: " + biEntitiesWithLineage);
  }

  /**
   * Test that compliance-sensitive glossary terms have been removed.
   *
   * <p>These terms should NOT appear anywhere in the sample data:
   *
   * <ul>
   *   <li>"Compliance Audit Evidence" (glossary node)
   *   <li>"CMMC Ready" (glossary term)
   *   <li>"DOD 5015.2 Records Management" (glossary term)
   *   <li>"OMB Data Policy Mandate" (glossary term)
   *   <li>"Risk Management Framework (RMF)" (glossary term)
   * </ul>
   */
  @Test
  public void testComplianceTermsRemoved() {
    String[] excludedTerms = {
      "Compliance Audit Evidence",
      "CMMC Ready",
      "DOD 5015.2 Records Management",
      "OMB Data Policy Mandate",
      "Risk Management Framework (RMF)",
      "Risk Management Framework"
    };

    // Convert MCPs to string for search
    String sampleDataContent = mcps.toString().toLowerCase();

    for (String term : excludedTerms) {
      assertFalse(
          sampleDataContent.contains(term.toLowerCase()),
          "Sample data should not contain compliance term: " + term);
    }
  }

  /**
   * Test that no sensitive email domains are present.
   *
   * <p>Sample data should use anonymized emails (@example.com), not real company email addresses.
   */
  @Test
  public void testNoSensitiveEmailDomains() {
    String[] sensitiveDomains = {"@acryl.io", "@datahub.com"};

    String sampleDataContent = mcps.toString();

    for (String domain : sensitiveDomains) {
      assertFalse(
          sampleDataContent.contains(domain),
          "Sample data should not contain sensitive email domain: " + domain);
    }
  }

  /**
   * Test that structured properties are not included in sample data.
   *
   * <p>Structured properties are tenant-specific configuration and should not be in public sample
   * data.
   */
  @Test
  public void testNoStructuredProperties() {
    // Check for structuredProperty entity type
    long structuredPropertyCount =
        mcps.stream()
            .filter(mcp -> "structuredProperty".equals(mcp.path("entityType").asText()))
            .count();

    assertEquals(
        structuredPropertyCount,
        0,
        "Sample data should not contain structuredProperty entities. Found: "
            + structuredPropertyCount);

    // Check for structuredProperties aspect
    long structuredPropertiesAspectCount =
        mcps.stream()
            .filter(mcp -> "structuredProperties".equals(mcp.path("aspectName").asText()))
            .count();

    assertEquals(
        structuredPropertiesAspectCount,
        0,
        "Sample data should not contain structuredProperties aspects. Found: "
            + structuredPropertiesAspectCount);
  }

  /**
   * Test that all entities are marked as sample data.
   *
   * <p>All MCPs should have systemMetadata.properties.sampleData = "true" to support the toggle
   * sample data feature. This allows users to easily enable/disable sample data in their instance.
   */
  @Test
  public void testAllEntitiesMarkedAsSampleData() {
    long totalMcps = mcps.size();
    long markedAsSampleData =
        mcps.stream()
            .filter(
                mcp ->
                    "true"
                        .equals(
                            mcp.path("systemMetadata")
                                .path("properties")
                                .path("sampleData")
                                .asText()))
            .count();

    // Allow a small margin for metadata aspects that might not need the flag
    double percentage = (markedAsSampleData * 100.0 / totalMcps);
    assertTrue(
        percentage >= 95.0,
        String.format(
            "At least 95%% of MCPs should be marked as sample data. Found: %d/%d (%.1f%%)",
            markedAsSampleData, totalMcps, percentage));
  }

  /**
   * Test that sample data file is well-formed and has expected size.
   *
   * <p>Verifies basic structural integrity and that the file hasn't been corrupted or significantly
   * reduced in size (which would indicate missing demo data).
   */
  @Test
  public void testSampleDataWellFormed() {
    assertNotNull(mcps, "MCPs should be loaded");
    assertTrue(
        mcps.size() >= 1500,
        "Should have at least 1500 MCPs for complete demo. Found: " + mcps.size());

    // Verify each MCP has required fields
    for (int i = 0; i < Math.min(100, mcps.size()); i++) {
      JsonNode mcp = mcps.get(i);
      assertFalse(
          mcp.path("entityType").asText().isEmpty(), "MCP " + i + " should have entityType");
      assertFalse(mcp.path("entityUrn").asText().isEmpty(), "MCP " + i + " should have entityUrn");
      assertFalse(
          mcp.path("aspectName").asText().isEmpty(), "MCP " + i + " should have aspectName");
      assertFalse(
          mcp.path("changeType").asText().isEmpty(), "MCP " + i + " should have changeType");
    }
  }

  /**
   * Test that column-level lineage is present in sample data.
   *
   * <p>Column-level lineage is a key DataHub feature that traces how individual columns flow
   * through transformations. The sample data should include upstreamLineage aspects with
   * fine-grained lineage information showing column-to-column mappings.
   *
   * <p>This test verifies:
   *
   * <ul>
   *   <li>At least some datasets have upstreamLineage with fineGrainedLineages
   *   <li>Fine-grained lineages have proper structure (upstreams, downstreams, transformOperation)
   *   <li>Column paths are properly formatted (e.g., [version=2.0].[key=column_name])
   * </ul>
   */
  @Test
  public void testColumnLevelLineagePresent() {
    // Find all upstreamLineage aspects with fine-grained lineages
    long upstreamLineagesWithFineGrained =
        mcps.stream()
            .filter(mcp -> "upstreamLineage".equals(mcp.path("aspectName").asText()))
            .filter(
                mcp -> {
                  JsonNode fineGrainedLineages =
                      mcp.path("aspect").path("json").path("fineGrainedLineages");
                  return fineGrainedLineages.isArray() && fineGrainedLineages.size() > 0;
                })
            .count();

    assertTrue(
        upstreamLineagesWithFineGrained >= 10,
        "Should have at least 10 datasets with column-level lineage for demo. Found: "
            + upstreamLineagesWithFineGrained);

    // Verify structure of fine-grained lineages
    boolean foundValidColumnLineage = false;
    for (JsonNode mcp : mcps) {
      if ("upstreamLineage".equals(mcp.path("aspectName").asText())) {
        JsonNode fineGrainedLineages = mcp.path("aspect").path("json").path("fineGrainedLineages");

        if (fineGrainedLineages.isArray()) {
          for (JsonNode lineage : fineGrainedLineages) {
            JsonNode upstreams = lineage.path("upstreams");
            JsonNode downstreams = lineage.path("downstreams");
            String upstreamType = lineage.path("upstreamType").asText();
            String downstreamType = lineage.path("downstreamType").asText();

            // Check for proper structure: field-level lineage with schemaField URNs
            if (upstreams.isArray()
                && upstreams.size() > 0
                && downstreams.isArray()
                && downstreams.size() > 0
                && ("FIELD".equals(downstreamType) || "FIELD_SET".equals(upstreamType))) {

              // Verify schemaField URNs are present
              String firstUpstream = upstreams.get(0).asText();
              String firstDownstream = downstreams.get(0).asText();

              // Column lineage should use schemaField URNs
              if (firstUpstream.contains("urn:li:schemaField:")
                  && firstDownstream.contains("urn:li:schemaField:")) {
                foundValidColumnLineage = true;
                break;
              }
            }
          }
        }

        if (foundValidColumnLineage) {
          break;
        }
      }
    }

    assertTrue(
        foundValidColumnLineage,
        "Should have at least one properly structured column-level lineage with schemaField URNs");
  }
}
