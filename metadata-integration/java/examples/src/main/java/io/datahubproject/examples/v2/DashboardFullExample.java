package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dashboard;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example demonstrating all Dashboard metadata operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a dashboard with complete metadata
 *   <li>Adding tags, owners, glossary terms
 *   <li>Setting domain and custom properties
 *   <li>Adding input dataset lineage
 *   <li>Adding chart relationships
 *   <li>Setting dashboard-specific properties (URL, last refreshed)
 *   <li>Combining all operations in single entity
 * </ul>
 */
public class DashboardFullExample {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException, URISyntaxException {
    // Create client
    DataHubClientV2 client =
        DataHubClientV2.builder()
            .server(System.getenv().getOrDefault("DATAHUB_SERVER", "http://localhost:8080"))
            .token(System.getenv("DATAHUB_TOKEN"))
            .build();

    try {
      // Test connection
      if (!client.testConnection()) {
        System.err.println("Failed to connect to DataHub server");
        return;
      }
      System.out.println("✓ Connected to DataHub");

      // Build comprehensive dashboard with all metadata types
      Dashboard dashboard =
          Dashboard.builder()
              .tool("tableau")
              .id("executive_kpi_dashboard")
              .title("Executive KPI Dashboard")
              .description(
                  "Comprehensive executive dashboard showing key performance indicators across all business units. "
                      + "Includes financial metrics, operational KPIs, customer satisfaction scores, and employee engagement data. "
                      + "Updated hourly with real-time data from multiple source systems.")
              .build();

      System.out.println("✓ Built dashboard with URN: " + dashboard.getUrn());

      // Add multiple tags for categorization
      dashboard
          .addTag("executive")
          .addTag("kpi")
          .addTag("real-time")
          .addTag("cross-functional")
          .addTag("production");

      System.out.println("✓ Added 5 tags");

      // Add multiple owners with different roles
      dashboard
          .addOwner("urn:li:corpuser:ceo", OwnershipType.BUSINESS_OWNER)
          .addOwner("urn:li:corpuser:bi_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:data_governance", OwnershipType.DATA_STEWARD);

      System.out.println("✓ Added 3 owners");

      // Add glossary terms for business context
      dashboard
          .addTerm("urn:li:glossaryTerm:ExecutiveMetrics")
          .addTerm("urn:li:glossaryTerm:KeyPerformanceIndicator")
          .addTerm("urn:li:glossaryTerm:BusinessIntelligence");

      System.out.println("✓ Added 3 glossary terms");

      // Set domain for organizational structure
      dashboard.setDomain("urn:li:domain:Executive");

      System.out.println("✓ Set domain");

      // Add comprehensive custom properties
      dashboard
          .addCustomProperty("team", "business-intelligence")
          .addCustomProperty("refresh_schedule", "hourly")
          .addCustomProperty("data_sources", "snowflake,salesforce,workday")
          .addCustomProperty("dashboard_type", "executive_summary")
          .addCustomProperty("sla_tier", "tier1")
          .addCustomProperty("business_criticality", "critical")
          .addCustomProperty("access_level", "executive-only")
          .addCustomProperty("compliance_reviewed", "2024-01-15")
          .addCustomProperty("primary_contact", "bi-team@company.com")
          .addCustomProperty("documentation_url", "https://wiki.company.com/dashboards/exec-kpi");

      System.out.println("✓ Added 10 custom properties");

      // Add input datasets (lineage from data sources)
      List<DatasetUrn> inputDatasets =
          Arrays.asList(
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:snowflake,financial.metrics,PROD)"),
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:salesforce,Account,PROD)"),
              DatasetUrn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:workday,EmployeeData,PROD)"));

      for (DatasetUrn datasetUrn : inputDatasets) {
        dashboard.addInputDataset(datasetUrn);
      }

      System.out.println("✓ Added 3 input datasets (lineage)");

      // Add charts contained in this dashboard
      List<ChartUrn> charts =
          Arrays.asList(
              new ChartUrn("tableau", "revenue_chart"),
              new ChartUrn("tableau", "customer_satisfaction_chart"),
              new ChartUrn("tableau", "employee_engagement_chart"),
              new ChartUrn("tableau", "operational_metrics_chart"));

      for (ChartUrn chartUrn : charts) {
        dashboard.addChart(chartUrn);
      }

      System.out.println("✓ Added 4 charts (relationships)");

      // Set dashboard-specific properties
      dashboard.setDashboardUrl("https://tableau.company.com/views/executive_kpi_dashboard");
      dashboard.setLastRefreshed(System.currentTimeMillis());

      System.out.println("✓ Set dashboard URL and last refreshed timestamp");

      // Count accumulated patches
      System.out.println("\nAccumulated " + dashboard.getPendingPatches().size() + " patches");

      // Upsert to DataHub - all metadata in single operation
      client.entities().upsert(dashboard);

      System.out.println("\n✓ Successfully created comprehensive dashboard in DataHub!");
      System.out.println("\nSummary:");
      System.out.println("  URN: " + dashboard.getUrn());
      System.out.println("  Tool: tableau");
      System.out.println("  Tags: 5");
      System.out.println("  Owners: 3");
      System.out.println("  Glossary Terms: 3");
      System.out.println("  Domain: Executive");
      System.out.println("  Custom Properties: 10");
      System.out.println("  Input Datasets: 3");
      System.out.println("  Charts: 4");
      System.out.println("  Dashboard URL: Set");
      System.out.println("  Last Refreshed: " + System.currentTimeMillis());
      System.out.println(
          "\n  View in DataHub: "
              + client.getConfig().getServer()
              + "/dashboard/"
              + dashboard.getUrn());

    } finally {
      client.close();
    }
  }
}
