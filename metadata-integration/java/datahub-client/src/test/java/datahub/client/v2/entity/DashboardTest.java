package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Tests for Dashboard entity builder and patch-based operations. */
public class DashboardTest {

  // ==================== Builder Tests ====================

  @Test
  public void testDashboardBuilderMinimal() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    assertNotNull("Dashboard should not be null", dashboard);
    assertNotNull("URN should not be null", dashboard.getUrn());
    assertNotNull("Dashboard URN should not be null", dashboard.getDashboardUrn());
    assertEquals("Entity type should be dashboard", "dashboard", dashboard.getEntityType());
    assertTrue("Dashboard should be new entity", dashboard.isNewEntity());
  }

  @Test
  public void testDashboardBuilderWithTitle() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("sales_dashboard")
            .title("Sales Performance Dashboard")
            .description("Sales performance metrics dashboard")
            .build();

    assertNotNull("Dashboard should not be null", dashboard);
    List<MetadataChangeProposalWrapper> mcps = dashboard.toMCPs();
    assertFalse("MCPs should not be empty", mcps.isEmpty());

    boolean hasDashboardInfo =
        mcps.stream()
            .anyMatch(mcp -> mcp.getAspect().getClass().getSimpleName().equals("DashboardInfo"));
    assertTrue("Should have DashboardInfo aspect", hasDashboardInfo);
  }

  @Test
  public void testDashboardBuilderWithAllOptions() throws Exception {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("refresh_frequency", "hourly");
    customProps.put("owner_team", "analytics");

    Dashboard dashboard =
        Dashboard.builder()
            .tool("powerbi")
            .id("executive_dashboard")
            .title("Executive Dashboard")
            .description("KPIs and metrics for executive team")
            .customProperties(customProps)
            .build();

    assertNotNull("Dashboard should not be null", dashboard);
    assertTrue("URN should contain tool", dashboard.getUrn().toString().contains("powerbi"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDashboardBuilderMissingTool() throws Exception {
    Dashboard.builder().id("my_dashboard").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDashboardBuilderMissingId() throws Exception {
    Dashboard.builder().tool("tableau").build();
  }

  // ==================== Input Dataset Lineage Tests ====================

  @Test
  public void testDashboardAddInputDataset() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics_db.public.sales_summary,PROD)");
    dashboard.addInputDataset(dataset);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals(
        "Aspect name should be dashboardInfo", "dashboardInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDashboardAddInputDatasetString() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    String datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)";
    dashboard.addInputDataset(datasetUrn);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals(
        "Aspect name should be dashboardInfo", "dashboardInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDashboardSetInputDatasets() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    List<DatasetUrn> datasets = new ArrayList<>();
    datasets.add(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.orders,PROD)"));
    datasets.add(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)"));
    datasets.add(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD)"));

    dashboard.setInputDatasets(datasets);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals(
        "Aspect name should be dashboardInfo", "dashboardInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDashboardRemoveInputDataset() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");
    dashboard.addInputDataset(dataset);
    dashboard.removeInputDataset(dataset);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals(
        "First patch aspect should be dashboardInfo",
        "dashboardInfo",
        patches.get(0).getAspectName());
    // Second patch removed: accumulated patches reduce patch count
  }

  @Test
  public void testDashboardRemoveInputDatasetString() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("my_dashboard").build();

    String datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:postgres,public.table,PROD)";
    dashboard.addInputDataset(datasetUrn);
    dashboard.removeInputDataset(datasetUrn);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardGetInputDatasets() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    List<DatasetUrn> initialDatasets = dashboard.getInputDatasets();
    assertNotNull("Initial datasets should not be null", initialDatasets);
    assertTrue("Initial datasets should be empty", initialDatasets.isEmpty());
  }

  // ==================== Chart Relationship Tests ====================

  @Test
  public void testDashboardAddChart() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    ChartUrn chart = new ChartUrn("tableau", "sales_by_region_chart");
    dashboard.addChart(chart);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals(
        "Aspect name should be dashboardInfo", "dashboardInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDashboardAddChartString() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    String chartUrn = "urn:li:chart:(looker,revenue_chart)";
    dashboard.addChart(chartUrn);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals(
        "Aspect name should be dashboardInfo", "dashboardInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDashboardSetCharts() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("executive_dashboard").build();

    List<ChartUrn> charts = new ArrayList<>();
    charts.add(new ChartUrn("powerbi", "revenue_chart"));
    charts.add(new ChartUrn("powerbi", "customer_growth_chart"));
    charts.add(new ChartUrn("powerbi", "expenses_chart"));

    dashboard.setCharts(charts);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals(
        "Aspect name should be dashboardInfo", "dashboardInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDashboardRemoveChart() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    ChartUrn chart = new ChartUrn("tableau", "chart_1");
    dashboard.addChart(chart);
    dashboard.removeChart(chart);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals(
        "First patch aspect should be dashboardInfo",
        "dashboardInfo",
        patches.get(0).getAspectName());
    // Second patch removed: accumulated patches reduce patch count
  }

  @Test
  public void testDashboardRemoveChartString() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    String chartUrn = "urn:li:chart:(looker,chart_1)";
    dashboard.addChart(chartUrn);
    dashboard.removeChart(chartUrn);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardGetCharts() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    List<ChartUrn> initialCharts = dashboard.getCharts();
    assertNotNull("Initial charts should not be null", initialCharts);
    assertTrue("Initial charts should be empty", initialCharts.isEmpty());
  }

  // ==================== Dashboard-Specific Property Tests ====================

  @Test
  public void testDashboardSetLastRefreshed() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    long timestamp = System.currentTimeMillis();
    dashboard.setLastRefreshed(timestamp);

    Long lastRefreshed = dashboard.getLastRefreshed();
    assertNotNull("Last refreshed should not be null", lastRefreshed);
    assertEquals("Last refreshed should match", timestamp, lastRefreshed.longValue());
  }

  @Test
  public void testDashboardSetDashboardUrl() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("sales_dashboard").build();

    String url = "https://looker.company.com/dashboards/123";
    dashboard.setDashboardUrl(url);

    String dashboardUrl = dashboard.getDashboardUrl();
    assertNotNull("Dashboard URL should not be null", dashboardUrl);
    assertEquals("Dashboard URL should match", url, dashboardUrl);
  }

  @Test
  public void testDashboardGetDashboardUrl() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("my_dashboard").build();

    String url = dashboard.getDashboardUrl();
    assertNull("Dashboard URL should be null initially", url);

    dashboard.setDashboardUrl("https://powerbi.company.com/reports/456");
    url = dashboard.getDashboardUrl();
    assertNotNull("Dashboard URL should not be null after setting", url);
  }

  @Test
  public void testDashboardGetLastRefreshed() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    Long timestamp = dashboard.getLastRefreshed();
    assertNull("Last refreshed should be null initially", timestamp);

    long now = System.currentTimeMillis();
    dashboard.setLastRefreshed(now);
    timestamp = dashboard.getLastRefreshed();
    assertNotNull("Last refreshed should not be null after setting", timestamp);
    assertEquals("Last refreshed should match", now, timestamp.longValue());
  }

  // ==================== Standard Metadata Tests ====================

  @Test
  public void testDashboardAddTag() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    dashboard.addTag("executive");
    dashboard.addTag("urn:li:tag:financial");

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals(
        "First patch aspect should be globalTags", "globalTags", patches.get(0).getAspectName());
    // Second patch removed: accumulated patches reduce patch count
  }

  @Test
  public void testDashboardAddOwner() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    dashboard.addOwner("urn:li:corpuser:johndoe", OwnershipType.BUSINESS_OWNER);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("Aspect name should be ownership", "ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testDashboardAddTerm() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("my_dashboard").build();

    dashboard.addTerm("urn:li:glossaryTerm:SalesMetrics");
    dashboard.addTerm("urn:li:glossaryTerm:KPI");

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals(
        "First patch aspect should be glossaryTerms",
        "glossaryTerms",
        patches.get(0).getAspectName());
    // Second patch removed: accumulated patches reduce patch count
  }

  @Test
  public void testDashboardSetDomain() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    dashboard.setDomain("urn:li:domain:Sales");

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dashboard.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDashboardAddCustomProperty() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    dashboard.addCustomProperty("refresh_schedule", "daily");
    dashboard.addCustomProperty("business_unit", "sales");

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Fluent API Tests ====================

  @Test
  public void testDashboardFluentAPI() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("tableau")
            .id("comprehensive_dashboard")
            .title("Comprehensive Sales Dashboard")
            .description("Complete view of sales performance")
            .build();

    DatasetUrn dataset1 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,sales.transactions,PROD)");
    DatasetUrn dataset2 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:postgres,crm.customers,PROD)");
    ChartUrn chart1 = new ChartUrn("tableau", "revenue_chart");
    ChartUrn chart2 = new ChartUrn("tableau", "growth_chart");

    dashboard
        .addTag("executive")
        .addTag("financial")
        .addOwner("urn:li:corpuser:owner1", OwnershipType.BUSINESS_OWNER)
        .addTerm("urn:li:glossaryTerm:Revenue")
        .setDomain("urn:li:domain:Finance")
        .addInputDataset(dataset1)
        .addInputDataset(dataset2)
        .addChart(chart1)
        .addChart(chart2)
        .setDashboardUrl("https://tableau.company.com/dashboards/123")
        .setLastRefreshed(System.currentTimeMillis());

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardFluentAPIWithLineageAndCharts() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("analytics_dashboard")
            .title("Analytics Dashboard")
            .description("Analytics metrics dashboard")
            .build();

    List<DatasetUrn> datasets =
        Arrays.asList(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table1,PROD)"),
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table2,PROD)"));

    List<ChartUrn> charts =
        Arrays.asList(
            new ChartUrn("looker", "chart1"),
            new ChartUrn("looker", "chart2"),
            new ChartUrn("looker", "chart3"));

    dashboard
        .setInputDatasets(datasets)
        .setCharts(charts)
        .addTag("analytics")
        .setDomain("urn:li:domain:Analytics");

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Utility Tests ====================

  @Test
  public void testDashboardToMCPs() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("tableau")
            .id("my_dashboard")
            .title("My Dashboard")
            .description("Test dashboard description")
            .build();

    List<MetadataChangeProposalWrapper> mcps = dashboard.toMCPs();

    assertNotNull("MCPs should not be null", mcps);
    assertFalse("MCPs should not be empty", mcps.isEmpty());

    for (MetadataChangeProposalWrapper mcp : mcps) {
      assertEquals("Entity type should be dashboard", "dashboard", mcp.getEntityType());
      assertNotNull("Entity URN should not be null", mcp.getEntityUrn());
      assertNotNull("Aspect should not be null", mcp.getAspect());
    }
  }

  @Test
  public void testDashboardClearPendingPatches() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    dashboard.addTag("tag1");
    dashboard.addOwner("urn:li:corpuser:owner1", OwnershipType.BUSINESS_OWNER);
    dashboard.addChart(new ChartUrn("looker", "chart1"));

    assertTrue("Should have pending patches", dashboard.hasPendingPatches());

    dashboard.clearPendingPatches();

    assertFalse("Should not have pending patches", dashboard.hasPendingPatches());
  }

  @Test
  public void testDashboardEqualsAndHashCode() throws Exception {
    Dashboard dashboard1 = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    Dashboard dashboard2 = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    assertEquals("Dashboards should be equal based on URN", dashboard1, dashboard2);
    assertEquals("Hash codes should be equal", dashboard1.hashCode(), dashboard2.hashCode());
  }

  @Test
  public void testDashboardToString() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("my_dashboard").build();

    String str = dashboard.toString();
    assertNotNull("toString should not return null", str);
    assertTrue("toString should contain Dashboard", str.contains("Dashboard"));
    assertTrue("toString should contain urn", str.contains("urn"));
  }

  // ==================== Additional Edge Cases ====================

  @Test
  public void testDashboardRemoveOwner() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    dashboard.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
    dashboard.removeOwner("urn:li:corpuser:johndoe");

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardRemoveTag() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    dashboard.addTag("tag1");
    dashboard.removeTag("tag1");

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardRemoveTerm() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("my_dashboard").build();

    dashboard.addTerm("urn:li:glossaryTerm:term1");
    dashboard.removeTerm("urn:li:glossaryTerm:term1");

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardRemoveDomain() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    Urn marketingDomain = Urn.createFromString("urn:li:domain:Marketing");
    dashboard.setDomain(marketingDomain.toString());
    dashboard.removeDomain(marketingDomain);

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dashboard.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDashboardRemoveCustomProperty() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    dashboard.addCustomProperty("env", "production");
    dashboard.removeCustomProperty("env");

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardSetCustomProperties() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("my_dashboard").build();

    Map<String, String> properties = new HashMap<>();
    properties.put("refresh_rate", "hourly");
    properties.put("owner_email", "team@company.com");

    dashboard.setCustomProperties(properties);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardGetTitle() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("tableau")
            .id("my_dashboard")
            .title("Sales Dashboard")
            .description("Sales dashboard description")
            .build();

    String title = dashboard.getTitle();
    assertNotNull("Title should not be null", title);
    assertEquals("Title should match", "Sales Dashboard", title);
  }

  @Test
  public void testDashboardGetDescription() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("my_dashboard")
            .title("Sales Dashboard")
            .description("Dashboard for sales analytics")
            .build();

    String description = dashboard.getDescription();
    assertNotNull("Description should not be null", description);
    assertEquals("Description should match", "Dashboard for sales analytics", description);
  }

  @Test
  public void testDashboardBuilderChaining() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("key", "value");

    Dashboard dashboard =
        Dashboard.builder()
            .tool("tableau")
            .id("test_dashboard")
            .title("Test")
            .description("Description")
            .customProperties(props)
            .build();

    assertNotNull("Dashboard should not be null", dashboard);
    assertEquals("Title should match", "Test", dashboard.getTitle());
    assertEquals("Description should match", "Description", dashboard.getDescription());
  }

  @Test
  public void testDashboardPatchVsFullAspect() throws Exception {
    Dashboard dashboard1 =
        Dashboard.builder()
            .tool("tableau")
            .id("dashboard1")
            .title("Title from builder")
            .description("Dashboard description")
            .build();

    List<MetadataChangeProposalWrapper> mcps1 = dashboard1.toMCPs();
    assertFalse("Should have cached aspects", mcps1.isEmpty());

    Dashboard dashboard2 = Dashboard.builder().tool("looker").id("dashboard2").build();

    dashboard2.addChart(new ChartUrn("looker", "chart1"));
    List<MetadataChangeProposal> patches2 = dashboard2.getPendingPatches();
    assertFalse("Should have pending patches", patches2.isEmpty());
  }

  @Test
  public void testDashboardComplexLineageScenario() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("complex_dashboard").build();

    DatasetUrn ds1 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table1,PROD)");
    DatasetUrn ds2 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table2,PROD)");
    DatasetUrn ds3 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)");

    dashboard
        .addInputDataset(ds1)
        .addInputDataset(ds2)
        .addInputDataset(ds3)
        .removeInputDataset(ds2);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardComplexChartScenario() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("complex_dashboard").build();

    ChartUrn chart1 = new ChartUrn("powerbi", "chart1");
    ChartUrn chart2 = new ChartUrn("powerbi", "chart2");
    ChartUrn chart3 = new ChartUrn("powerbi", "chart3");

    dashboard.addChart(chart1).addChart(chart2).addChart(chart3).removeChart(chart2);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDashboardAddInputDatasetInvalidUrn() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();
    dashboard.addInputDataset("invalid-urn");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDashboardAddChartInvalidUrn() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();
    dashboard.addChart("invalid-chart-urn");
  }

  @Test
  public void testDashboardMultipleOwners() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    dashboard.addOwner("urn:li:corpuser:owner1", OwnershipType.BUSINESS_OWNER);
    dashboard.addOwner("urn:li:corpuser:owner2", OwnershipType.TECHNICAL_OWNER);
    dashboard.addOwner("urn:li:corpuser:owner3", OwnershipType.DATA_STEWARD);

    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDashboardClearDomains() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    dashboard.setDomain("urn:li:domain:Sales");
    dashboard.clearDomains();

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dashboard.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDashboardRemoveDomainWithString() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("my_dashboard").build();

    dashboard.setDomain("urn:li:domain:Marketing");
    dashboard.removeDomain("urn:li:domain:Marketing");

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dashboard.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDashboardSetTags() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("powerbi").id("my_dashboard").build();

    List<String> tags = Arrays.asList("tag1", "tag2", "tag3");
    dashboard.setTags(tags);

    // setTags() creates a full MCP, not patches
    List<MetadataChangeProposalWrapper> pendingMCPs = dashboard.getPendingMCPs();
    assertFalse("Should have pending MCPs", pendingMCPs.isEmpty());
    assertEquals(
        "Aspect name should be globalTags", "globalTags", pendingMCPs.get(0).getAspectName());
  }

  @Test
  public void testDashboardSetTerms() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("tableau").id("my_dashboard").build();

    List<String> terms =
        Arrays.asList(
            "urn:li:glossaryTerm:SalesMetrics",
            "urn:li:glossaryTerm:Revenue",
            "urn:li:glossaryTerm:KPI");
    dashboard.setTerms(terms);

    // setTerms() creates a full MCP, not patches
    List<MetadataChangeProposalWrapper> pendingMCPs = dashboard.getPendingMCPs();
    assertFalse("Should have pending MCPs", pendingMCPs.isEmpty());
    assertEquals(
        "Aspect name should be glossaryTerms", "glossaryTerms", pendingMCPs.get(0).getAspectName());
  }

  @Test
  public void testDashboardFluentChaining() throws Exception {
    Dashboard dashboard = Dashboard.builder().tool("looker").id("chaining_dashboard").build();

    dashboard
        .addTag("tag1")
        .addTag("tag2")
        .addOwner("urn:li:corpuser:owner1", OwnershipType.BUSINESS_OWNER)
        .addTerm("urn:li:glossaryTerm:Term1")
        .setDomain("urn:li:domain:Engineering")
        .addCustomProperty("key1", "value1")
        .setDashboardUrl("https://looker.example.com/dashboards/123")
        .setLastRefreshed(System.currentTimeMillis());

    assertTrue("Should have pending patches", dashboard.hasPendingPatches());
    List<MetadataChangeProposal> patches = dashboard.getPendingPatches();
    assertFalse("Should have multiple patches", patches.isEmpty());
  }
}
