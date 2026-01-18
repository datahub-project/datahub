package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Tests for Chart entity builder and patch-based operations. */
public class ChartTest {

  // ==================== Builder Tests ====================

  @Test
  public void testChartBuilderMinimal() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("my_chart").build();

    assertNotNull(chart);
    assertNotNull(chart.getUrn());
    assertNotNull(chart.getChartUrn());
    assertEquals("chart", chart.getEntityType());
    assertTrue(chart.isNewEntity());
  }

  @Test
  public void testChartBuilderWithTitle() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("tableau")
            .id("sales_chart")
            .title("Sales Performance")
            .description("Sales performance metrics")
            .build();

    assertNotNull(chart);
    List<MetadataChangeProposalWrapper> mcps = chart.toMCPs();
    assertFalse(mcps.isEmpty());

    // Should have ChartInfo aspect cached from builder
    boolean hasChartInfo =
        mcps.stream()
            .anyMatch(mcp -> mcp.getAspect().getClass().getSimpleName().equals("ChartInfo"));
    assertTrue("Should have ChartInfo aspect", hasChartInfo);
  }

  @Test
  public void testChartBuilderWithAllOptions() throws Exception {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("refresh_rate", "daily");
    customProps.put("dashboard_id", "dashboard_123");

    Chart chart =
        Chart.builder()
            .tool("superset")
            .id("revenue_chart")
            .title("Revenue Overview")
            .description("Monthly revenue breakdown by region")
            .customProperties(customProps)
            .build();

    assertNotNull(chart);
    assertTrue(chart.getUrn().toString().contains("superset"));
    assertTrue(chart.getUrn().toString().contains("revenue_chart"));

    // Verify ChartInfo aspect was created with all properties
    List<MetadataChangeProposalWrapper> mcps = chart.toMCPs();
    assertFalse(mcps.isEmpty());
    boolean hasChartInfo =
        mcps.stream()
            .anyMatch(mcp -> mcp.getAspect().getClass().getSimpleName().equals("ChartInfo"));
    assertTrue("Should have ChartInfo aspect with properties", hasChartInfo);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testChartBuilderMissingTool() throws Exception {
    Chart.builder().id("my_chart").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testChartBuilderMissingId() throws Exception {
    Chart.builder().tool("looker").build();
  }

  // ==================== Lineage Operation Tests ====================

  @Test
  public void testChartAddInputDataset() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("my_chart").build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");
    chart.addInputDataset(dataset);

    // Verify patch MCP was created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testChartAddMultipleInputDatasets() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("sales_chart").build();

    DatasetUrn dataset1 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)");
    DatasetUrn dataset2 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.customers,PROD)");

    chart.addInputDataset(dataset1);
    chart.addInputDataset(dataset2);

    // Should have 2 patch MCPs
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testChartSetInputDatasets() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("dashboard_chart").build();

    DatasetUrn dataset1 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD)");
    DatasetUrn dataset2 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.orders,PROD)");
    DatasetUrn dataset3 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.products,PROD)");

    List<DatasetUrn> datasets = Arrays.asList(dataset1, dataset2, dataset3);
    chart.setInputDatasets(datasets);

    // Verify single patch MCP was created with all datasets
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testChartRemoveInputDataset() throws Exception {
    Chart chart = Chart.builder().tool("superset").id("metrics_chart").build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,analytics.metrics,PROD)");

    chart.addInputDataset(dataset);
    chart.removeInputDataset(dataset);

    // Should have 2 patches (add and remove)
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testChartGetInputDatasets() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("test_chart").build();

    // For a new chart with no loaded aspects, should return empty list
    List<DatasetUrn> datasets = chart.getInputDatasets();
    assertNotNull(datasets);
    assertTrue("Should return empty list for new chart", datasets.isEmpty());
  }

  // ==================== Chart-Specific Property Tests ====================

  @Test
  public void testChartSetLastRefreshed() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("refresh_test_chart").build();

    long timestamp = System.currentTimeMillis();
    chart.setLastRefreshed(timestamp);

    // Verify that reads throw after mutation (strict contract)
    assertThrows(
        "Should throw PendingMutationsException after mutation",
        datahub.client.v2.exceptions.PendingMutationsException.class,
        () -> chart.getLastRefreshed());

    // Verify entity is marked as dirty
    assertTrue("Entity should be dirty after mutation", chart.isDirty());
  }

  @Test
  public void testChartSetChartType() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("type_test_chart").build();

    chart.setChartType("BAR");

    // Verify that reads throw after mutation (strict contract)
    assertThrows(
        "Should throw PendingMutationsException after mutation",
        datahub.client.v2.exceptions.PendingMutationsException.class,
        () -> chart.getChartType());

    // Verify entity is marked as dirty
    assertTrue("Entity should be dirty after mutation", chart.isDirty());
  }

  @Test
  public void testChartSetAccess() throws Exception {
    Chart chart = Chart.builder().tool("superset").id("access_test_chart").build();

    chart.setAccess("PUBLIC");

    // Verify that reads throw after mutation (strict contract)
    assertThrows(
        "Should throw PendingMutationsException after mutation",
        datahub.client.v2.exceptions.PendingMutationsException.class,
        () -> chart.getAccess());

    // Verify entity is marked as dirty
    assertTrue("Entity should be dirty after mutation", chart.isDirty());
  }

  @Test
  public void testChartSetChartUrl() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("url_test_chart").build();

    String url = "https://tableau.company.com/charts/123";
    chart.setChartUrl(url);

    // Verify that reads throw after mutation (strict contract)
    assertThrows(
        "Should throw PendingMutationsException after mutation",
        datahub.client.v2.exceptions.PendingMutationsException.class,
        () -> chart.getChartUrl());

    // Verify entity is marked as dirty
    assertTrue("Entity should be dirty after mutation", chart.isDirty());
  }

  @Test
  public void testChartSetExternalUrl() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("external_url_chart").build();

    String externalUrl = "https://looker.company.com/looks/456";
    chart.setExternalUrl(externalUrl);

    // Verify patch MCP was created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  // ==================== Standard Metadata Tests ====================

  @Test
  public void testChartAddTag() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("tag_test_chart").build();

    chart.addTag("pii");
    chart.addTag("urn:li:tag:sensitive");

    // Verify patch MCPs were created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("globalTags", patches.get(0).getAspectName());
  }

  @Test
  public void testChartRemoveTag() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("tag_removal_chart").build();

    chart.addTag("temp_tag");
    chart.removeTag("temp_tag");

    // Should have add and remove patches
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("globalTags", patches.get(0).getAspectName());
  }

  @Test
  public void testChartAddOwner() throws Exception {
    Chart chart = Chart.builder().tool("superset").id("owner_test_chart").build();

    chart.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);

    // Verify patch MCP was created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testChartAddMultipleOwners() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("multi_owner_chart").build();

    chart.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
    chart.addOwner("urn:li:corpuser:janedoe", OwnershipType.DATA_STEWARD);

    // Should have 2 patch MCPs
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testChartRemoveOwner() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("owner_removal_chart").build();

    chart.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
    chart.removeOwner("urn:li:corpuser:johndoe");

    // Should have 2 patches (add and remove)
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testChartAddTerm() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("term_test_chart").build();

    chart.addTerm("urn:li:glossaryTerm:CustomerData");
    chart.addTerm("urn:li:glossaryTerm:Revenue");

    // Verify patch MCPs were created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());
  }

  @Test
  public void testChartRemoveTerm() throws Exception {
    Chart chart = Chart.builder().tool("superset").id("term_removal_chart").build();

    chart.addTerm("urn:li:glossaryTerm:TestTerm");
    chart.removeTerm("urn:li:glossaryTerm:TestTerm");

    // Should have add and remove patches
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());
  }

  @Test
  public void testChartSetDomain() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("domain_test_chart").build();

    chart.setDomain("urn:li:domain:Marketing");

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = chart.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testChartRemoveDomain() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("domain_removal_chart").build();

    Urn marketingDomain = Urn.createFromString("urn:li:domain:Marketing");
    chart.setDomain(marketingDomain.toString());
    chart.removeDomain(marketingDomain);

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = chart.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testChartAddCustomProperty() throws Exception {
    Chart chart = Chart.builder().tool("superset").id("custom_prop_chart").build();

    chart.addCustomProperty("refresh_interval", "60");
    chart.addCustomProperty("query_id", "query_123");

    // Verify patch MCPs were created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testChartRemoveCustomProperty() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("custom_prop_removal_chart").build();

    chart.addCustomProperty("temp_prop", "temp_value");
    chart.removeCustomProperty("temp_prop");

    // Should have add and remove patches
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testChartSetCustomProperties() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("batch_props_chart").build();

    Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "value1");
    properties.put("prop2", "value2");
    properties.put("prop3", "value3");

    chart.setCustomProperties(properties);

    // Verify patch MCP was created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testChartSetDescription() throws Exception {
    Chart chart = Chart.builder().tool("superset").id("description_chart").build();

    chart.setDescription("This chart shows sales performance by region");

    // Verify patch MCP was created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have description patch", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testChartSetTitle() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("title_chart").build();

    chart.setTitle("Updated Sales Dashboard");

    // Verify patch MCP was created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have title patch", patches.isEmpty());
    assertEquals("chartInfo", patches.get(0).getAspectName());
  }

  // ==================== Fluent API Tests ====================

  @Test
  public void testChartFluentAPI() throws Exception {
    DatasetUrn dataset1 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)");
    DatasetUrn dataset2 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)");

    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("sales_performance")
            .title("Sales Performance Dashboard")
            .description("Dashboard showing sales metrics")
            .build();

    // Test fluent method chaining
    chart
        .addTag("pii")
        .addTag("critical")
        .addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:Sales")
        .setDomain("urn:li:domain:Finance")
        .addInputDataset(dataset1)
        .addInputDataset(dataset2)
        .setDescription("Sales metrics by region and product")
        .addCustomProperty("refresh_rate", "hourly");

    // Verify patches were accumulated
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testChartFluentAPIWithLineageAndProperties() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("tableau")
            .id("comprehensive_chart")
            .title("Comprehensive Dashboard")
            .description("Initial description")
            .build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)");

    // Chain multiple operations including chart-specific methods
    chart
        .addInputDataset(dataset)
        .setChartType("LINE")
        .setAccess("PRIVATE")
        .setExternalUrl("https://tableau.company.com/chart/123")
        .addTag("metrics")
        .addOwner("urn:li:corpuser:datateam", OwnershipType.DATA_STEWARD)
        .setDescription("Updated comprehensive dashboard description");

    // Verify multiple patches were created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());

    // Verify that reads throw after mutations (strict contract)
    assertThrows(
        "Should throw PendingMutationsException after mutations",
        datahub.client.v2.exceptions.PendingMutationsException.class,
        () -> chart.getChartType());
    assertThrows(
        "Should throw PendingMutationsException after mutations",
        datahub.client.v2.exceptions.PendingMutationsException.class,
        () -> chart.getAccess());

    // Verify entity is marked as dirty
    assertTrue("Entity should be dirty after mutations", chart.isDirty());
  }

  // ==================== Utility Tests ====================

  @Test
  public void testChartToMCPs() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart")
            .title("Test Chart")
            .description("Test description")
            .build();

    // toMCPs returns cached aspects (from builder), not patches
    List<MetadataChangeProposalWrapper> mcps = chart.toMCPs();

    assertNotNull(mcps);
    assertFalse(mcps.isEmpty());

    // Verify all MCPs have correct entity type and URN
    for (MetadataChangeProposalWrapper mcp : mcps) {
      assertEquals("chart", mcp.getEntityType());
      assertNotNull(mcp.getEntityUrn());
      assertNotNull(mcp.getAspect());
    }
  }

  @Test
  public void testChartClearPendingPatches() throws Exception {
    Chart chart = Chart.builder().tool("superset").id("clear_patch_chart").build();

    // Add some patches
    chart.addTag("tag1");
    chart.addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER);
    chart.setDescription("Test description");

    assertTrue("Should have pending patches", chart.hasPendingPatches());

    // Clear patches
    chart.clearPendingPatches();

    assertFalse("Should not have pending patches", chart.hasPendingPatches());
  }

  @Test
  public void testChartEqualsAndHashCode() throws Exception {
    Chart chart1 = Chart.builder().tool("tableau").id("equality_chart").build();

    Chart chart2 = Chart.builder().tool("tableau").id("equality_chart").build();

    // Should be equal based on URN
    assertEquals(chart1, chart2);
    assertEquals(chart1.hashCode(), chart2.hashCode());
  }

  @Test
  public void testChartToString() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("string_test_chart").build();

    String str = chart.toString();
    assertNotNull(str);
    assertTrue(str.contains("Chart"));
    assertTrue(str.contains("urn"));
  }

  @Test
  public void testChartPatchVsFullAspect() throws Exception {
    // Builder-provided properties go into cached aspects
    Chart chart1 =
        Chart.builder()
            .tool("looker")
            .id("aspect_chart_1")
            .title("Title from builder")
            .description("Description from builder")
            .build();

    List<MetadataChangeProposalWrapper> mcps1 = chart1.toMCPs();
    assertFalse("Should have cached aspects", mcps1.isEmpty());

    // Setter creates a patch
    Chart chart2 = Chart.builder().tool("tableau").id("aspect_chart_2").build();

    chart2.setDescription("Description from setter");
    List<MetadataChangeProposal> patches2 = chart2.getPendingPatches();
    assertFalse("Should have pending patches", patches2.isEmpty());
  }

  @Test
  public void testChartWithComplexLineage() throws Exception {
    Chart chart = Chart.builder().tool("superset").id("complex_lineage_chart").build();

    // Create multiple datasets from different platforms
    DatasetUrn snowflakeDataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD)");
    DatasetUrn bigqueryDataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.customers,PROD)");
    DatasetUrn postgresDataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.products,PROD)");

    // Add lineage to multiple datasets
    chart
        .addInputDataset(snowflakeDataset)
        .addInputDataset(bigqueryDataset)
        .addInputDataset(postgresDataset);

    // Verify patches were created
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    for (MetadataChangeProposal patch : patches) {
      assertEquals("chartInfo", patch.getAspectName());
    }
  }

  @Test
  public void testChartBuilderTitleOverridesDefault() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("default_title_chart")
            .title("Custom Title")
            .description("Chart with custom title")
            .build();

    String title = chart.getTitle();
    assertNotNull("Title should be set", title);
    assertEquals("Custom Title", title);
  }

  @Test
  public void testChartGettersReturnNullForUnsetProperties() throws Exception {
    Chart chart = Chart.builder().tool("tableau").id("null_props_chart").build();

    // For a newly created chart, getters should return null
    assertNull("Description should be null", chart.getDescription());
    assertNull("External URL should be null", chart.getExternalUrl());
    assertNull("Chart URL should be null", chart.getChartUrl());
    assertNull("Last refreshed should be null", chart.getLastRefreshed());
    assertNull("Chart type should be null", chart.getChartType());
    assertNull("Access should be null", chart.getAccess());
  }

  @Test
  public void testChartMultiplePropertyUpdates() throws Exception {
    Chart chart = Chart.builder().tool("looker").id("multi_update_chart").build();

    // Update the same property multiple times
    chart.setDescription("First description");
    chart.setDescription("Second description");
    chart.setDescription("Third description");

    // Each update should create a new patch
    List<MetadataChangeProposal> patches = chart.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    for (MetadataChangeProposal patch : patches) {
      assertEquals("chartInfo", patch.getAspectName());
    }
  }
}
