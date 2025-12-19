package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.entity.Dashboard;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Integration tests for Dashboard entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class DashboardIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testDashboardCreateMinimal() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_minimal_" + System.currentTimeMillis())
            .build();

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());
  }

  @Test
  public void testDashboardCreateWithMetadata() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_with_metadata_" + System.currentTimeMillis())
            .title("Test Sales Dashboard")
            .description("This is a test dashboard created by Java SDK V2")
            .build();

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());
  }

  @Test
  public void testDashboardWithTags() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_with_tags_" + System.currentTimeMillis())
            .title("Dashboard with tags")
            .description("Test dashboard for tag functionality")
            .build();

    dashboard.addTag("test-tag-1");
    dashboard.addTag("test-tag-2");
    dashboard.addTag("executive");

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());
  }

  @Test
  public void testDashboardWithOwners() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_with_owners_" + System.currentTimeMillis())
            .title("Dashboard with owners")
            .description("Test dashboard for ownership functionality")
            .build();

    dashboard.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dashboard.addOwner("urn:li:corpuser:admin", OwnershipType.BUSINESS_OWNER);

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());
  }

  @Test
  public void testDashboardWithDomain() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_with_domain_" + System.currentTimeMillis())
            .title("Dashboard with domain")
            .description("Test dashboard for domain functionality")
            .build();

    dashboard.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());
  }

  @Test
  public void testDashboardWithCustomProperties() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_with_custom_props_" + System.currentTimeMillis())
            .build();

    dashboard.addCustomProperty("dashboard", "executive_dashboard");
    dashboard.addCustomProperty("refresh_schedule", "hourly");
    dashboard.addCustomProperty("created_by", "java_sdk_v2");

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());
  }

  @Test
  public void testDashboardWithInputDatasets() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_with_input_datasets_" + System.currentTimeMillis())
            .title("Dashboard with input datasets")
            .description("Test dashboard for lineage functionality")
            .build();

    // Add input datasets
    dashboard.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)"));
    dashboard.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"));
    dashboard.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"));

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());

    // Validate lineage was written correctly
    Dashboard fetched = client.entities().get(dashboard.getUrn().toString(), Dashboard.class);
    assertNotNull(fetched);

    // Verify the 3 input datasets are present
    com.linkedin.dashboard.DashboardInfo info =
        fetched.getAspectLazy(com.linkedin.dashboard.DashboardInfo.class);
    assertNotNull("DashboardInfo aspect should exist", info);

    // Check if datasets field is populated
    if (info.hasDatasets() && info.getDatasets() != null && !info.getDatasets().isEmpty()) {
      assertEquals("Should have 3 input datasets", 3, info.getDatasets().size());

      // Verify the specific dataset URNs
      java.util.List<String> inputUrns = new java.util.ArrayList<>();
      for (com.linkedin.common.urn.Urn urn : info.getDatasets()) {
        inputUrns.add(urn.toString());
      }
      assertTrue(
          "Should contain sales dataset",
          inputUrns.contains(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)"));
      assertTrue(
          "Should contain customers dataset",
          inputUrns.contains(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"));
      assertTrue(
          "Should contain orders dataset",
          inputUrns.contains(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"));
    } else if (info.hasDatasetEdges()
        && info.getDatasetEdges() != null
        && !info.getDatasetEdges().isEmpty()) {
      // Check for newer datasetEdges field
      assertEquals("Should have 3 input datasets", 3, info.getDatasetEdges().size());

      // Verify the specific dataset URNs
      java.util.List<String> inputUrns = new java.util.ArrayList<>();
      for (com.linkedin.common.Edge edge : info.getDatasetEdges()) {
        inputUrns.add(edge.getDestinationUrn().toString());
      }
      assertTrue(
          "Should contain sales dataset",
          inputUrns.contains(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)"));
      assertTrue(
          "Should contain customers dataset",
          inputUrns.contains(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"));
      assertTrue(
          "Should contain orders dataset",
          inputUrns.contains(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"));
    } else {
      fail("Dashboard should have either datasets or datasetEdges populated");
    }
  }

  @Test
  public void testDashboardWithCharts() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_with_charts_" + System.currentTimeMillis())
            .title("Dashboard with charts")
            .description("Test dashboard for chart relationships")
            .build();

    // Add charts
    dashboard.addChart(new ChartUrn("looker", "chart_sales_1"));
    dashboard.addChart(new ChartUrn("looker", "chart_revenue_2"));
    dashboard.addChart(new ChartUrn("looker", "chart_growth_3"));

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());
  }

  @Test
  public void testDashboardWithProperties() throws Exception {
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_with_properties_" + System.currentTimeMillis())
            .title("Dashboard with dashboard-specific properties")
            .description("Test dashboard for dashboard-specific properties")
            .build();

    dashboard.setLastRefreshed(System.currentTimeMillis());
    dashboard.setDashboardUrl("https://looker.example.com/dashboards/test");

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());
  }

  @Test
  public void testDashboardFullMetadata() throws Exception {
    long testRun = System.currentTimeMillis();
    Map<String, String> customProps = new HashMap<>();
    customProps.put("created_by", "java_sdk_v2");
    customProps.put("test_run", String.valueOf(testRun));

    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_full_metadata_" + testRun)
            .title("Sales Performance Dashboard")
            .description("Complete dashboard with all metadata from Java SDK V2")
            .customProperties(customProps)
            .build();

    // Add all types of metadata
    dashboard.addTag("java-sdk-v2");
    dashboard.addTag("integration-test");
    dashboard.addTag("sales");

    dashboard.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dashboard.setDomain("urn:li:domain:Engineering");

    // Add lineage with input datasets
    dashboard.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"));
    dashboard.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.metrics,PROD)"));

    // Add charts
    dashboard.addChart(new ChartUrn("looker", "chart_sales_perf"));
    dashboard.addChart(new ChartUrn("looker", "chart_revenue_trend"));

    // Set dashboard-specific properties
    dashboard.setLastRefreshed(System.currentTimeMillis());
    dashboard.setDashboardUrl("https://looker.example.com/dashboards/sales_dashboard");

    client.entities().upsert(dashboard);

    assertNotNull(dashboard.getUrn());

    // Validate all metadata was written correctly
    validateEntityDescription(
        dashboard.getUrn().toString(),
        Dashboard.class,
        "Complete dashboard with all metadata from Java SDK V2");

    validateEntityHasTags(
        dashboard.getUrn().toString(),
        Dashboard.class,
        Arrays.asList("java-sdk-v2", "integration-test", "sales"));

    validateEntityHasOwners(
        dashboard.getUrn().toString(), Dashboard.class, Arrays.asList("urn:li:corpuser:datahub"));

    validateEntityCustomProperties(dashboard.getUrn().toString(), Dashboard.class, customProps);
  }

  @Test
  public void testMultipleDashboardCreation() throws Exception {
    // Create multiple dashboards in sequence
    for (int i = 0; i < 5; i++) {
      Dashboard dashboard =
          Dashboard.builder()
              .tool("looker")
              .id("test_dashboard_multi_" + i + "_" + System.currentTimeMillis())
              .build();

      dashboard.addTag("batch-test");
      dashboard.addCustomProperty("index", String.valueOf(i));

      client.entities().upsert(dashboard);
    }

    // If we get here, all dashboards were created successfully
    assertTrue(true);
  }

  @Test
  public void testGetDashboardWithSpecificAspects() throws Exception {
    // Create a dashboard with multiple aspects
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_specific_aspects_" + System.currentTimeMillis())
            .title("Dashboard for specific aspects test")
            .description("Testing fetching with specific aspect list")
            .build();

    dashboard.addTag("test-aspect-fetching");
    dashboard.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dashboard.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(dashboard);
    assertNotNull(dashboard.getUrn());

    // Fetch the dashboard with only specific aspects
    java.util.List<Class<? extends com.linkedin.data.template.RecordTemplate>> specificAspects =
        new java.util.ArrayList<>();
    specificAspects.add(com.linkedin.dashboard.DashboardInfo.class);
    specificAspects.add(com.linkedin.common.GlobalTags.class);

    Dashboard fetchedDashboard =
        client.entities().get(dashboard.getUrn().toString(), Dashboard.class, specificAspects);

    assertNotNull("Fetched dashboard should not be null", fetchedDashboard);
    assertEquals("URNs should match", dashboard.getUrn(), fetchedDashboard.getUrn());

    // Verify DashboardInfo aspect is present
    com.linkedin.dashboard.DashboardInfo info =
        fetchedDashboard.getAspectLazy(com.linkedin.dashboard.DashboardInfo.class);
    assertNotNull("DashboardInfo aspect should be present", info);
    assertEquals("Title should match", "Dashboard for specific aspects test", info.getTitle());

    // Verify GlobalTags aspect is present
    com.linkedin.common.GlobalTags tags =
        fetchedDashboard.getAspectLazy(com.linkedin.common.GlobalTags.class);
    assertNotNull("GlobalTags aspect should be present", tags);
    assertTrue("Should have at least one tag", tags.getTags().size() > 0);
  }

  @Test
  public void testGetDashboardWithDefaultAspects() throws Exception {
    // Create a dashboard
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_default_aspects_" + System.currentTimeMillis())
            .title("Dashboard for default aspects test")
            .description("Testing fetching with default aspects")
            .build();

    client.entities().upsert(dashboard);
    assertNotNull(dashboard.getUrn());

    // Fetch the dashboard with default aspects (using the get method without aspect list)
    Dashboard fetchedDashboard =
        client.entities().get(dashboard.getUrn().toString(), Dashboard.class);

    assertNotNull("Fetched dashboard should not be null", fetchedDashboard);
    assertEquals("URNs should match", dashboard.getUrn(), fetchedDashboard.getUrn());

    // Verify DashboardInfo aspect is present (should be in default aspects)
    com.linkedin.dashboard.DashboardInfo info =
        fetchedDashboard.getAspectLazy(com.linkedin.dashboard.DashboardInfo.class);
    assertNotNull("DashboardInfo aspect should be present", info);
    assertEquals("Title should match", "Dashboard for default aspects test", info.getTitle());
  }

  @Test
  public void testGetDashboardAspectDirectly() throws Exception {
    // Create a dashboard
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_aspect_direct_" + System.currentTimeMillis())
            .title("Dashboard for direct aspect fetch")
            .description("Testing direct aspect fetching via getAspect")
            .build();

    client.entities().upsert(dashboard);
    assertNotNull(dashboard.getUrn());

    // Fetch DashboardInfo aspect directly using getAspect
    datahub.client.v2.operations.AspectWithMetadata<com.linkedin.dashboard.DashboardInfo>
        aspectResult =
            client
                .entities()
                .getAspect(dashboard.getUrn(), com.linkedin.dashboard.DashboardInfo.class);

    assertNotNull("AspectWithMetadata should not be null", aspectResult);
    assertNotNull("Aspect value should not be null", aspectResult.getAspect());
    assertEquals(
        "Title should match",
        "Dashboard for direct aspect fetch",
        aspectResult.getAspect().getTitle());
    assertNotNull("System metadata should be present", aspectResult.getSystemMetadata());
    assertNotEquals(
        "Version should not be -1 for existing aspect", "-1", aspectResult.getVersion());
  }

  @Test
  public void testGetNonExistentDashboardAspect() throws Exception {
    // Try to fetch an aspect for a dashboard that doesn't exist
    com.linkedin.common.urn.DashboardUrn nonExistentUrn =
        new com.linkedin.common.urn.DashboardUrn(
            "looker", "nonexistent_dashboard_" + System.currentTimeMillis());

    datahub.client.v2.operations.AspectWithMetadata<com.linkedin.dashboard.DashboardInfo>
        aspectResult =
            client.entities().getAspect(nonExistentUrn, com.linkedin.dashboard.DashboardInfo.class);

    assertNotNull("AspectWithMetadata should not be null", aspectResult);
    assertNull("Aspect value should be null", aspectResult.getAspect());
    assertEquals("Version should be -1 for non-existent aspect", "-1", aspectResult.getVersion());
  }

  @Test
  public void testGetDashboardWithMixedAspects() throws Exception {
    // Create a dashboard with only DashboardInfo (no tags)
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_mixed_aspects_" + System.currentTimeMillis())
            .title("Dashboard for mixed aspects test")
            .description("Testing fetching with some aspects present and some absent")
            .build();

    client.entities().upsert(dashboard);
    assertNotNull(dashboard.getUrn());

    // Request multiple aspects, including one that doesn't exist (GlobalTags was not added)
    java.util.List<Class<? extends com.linkedin.data.template.RecordTemplate>> requestedAspects =
        new java.util.ArrayList<>();
    requestedAspects.add(com.linkedin.dashboard.DashboardInfo.class);
    requestedAspects.add(com.linkedin.common.GlobalTags.class);
    requestedAspects.add(com.linkedin.common.Ownership.class);

    Dashboard fetchedDashboard =
        client.entities().get(dashboard.getUrn().toString(), Dashboard.class, requestedAspects);

    assertNotNull("Fetched dashboard should not be null", fetchedDashboard);

    // DashboardInfo should exist (was created)
    com.linkedin.dashboard.DashboardInfo info =
        fetchedDashboard.getAspectLazy(com.linkedin.dashboard.DashboardInfo.class);
    assertNotNull("DashboardInfo aspect should be present", info);

    // GlobalTags and Ownership might not exist since we didn't add them
    // Just verify we can fetch without errors
  }

  @Test
  public void testDashboardUpdateAndRefetch() throws Exception {
    // Create initial dashboard with custom properties
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_update_refetch_" + System.currentTimeMillis())
            .title("Dashboard for update test")
            .description("Testing update and refetch")
            .build();

    dashboard.addCustomProperty("version", "1.0");
    dashboard.addCustomProperty("environment", "dev");
    client.entities().upsert(dashboard);
    assertNotNull(dashboard.getUrn());

    // Fetch it back and verify initial custom properties
    Dashboard fetched1 = client.entities().get(dashboard.getUrn().toString(), Dashboard.class);
    com.linkedin.dashboard.DashboardInfo info1 =
        fetched1.getAspectLazy(com.linkedin.dashboard.DashboardInfo.class);
    assertNotNull(info1);
    assertEquals("1.0", info1.getCustomProperties().get("version"));
    assertEquals("dev", info1.getCustomProperties().get("environment"));

    // Update custom properties
    dashboard.addCustomProperty("version", "2.0");
    dashboard.addCustomProperty("environment", "prod");
    dashboard.addCustomProperty("updated", "true");
    client.entities().upsert(dashboard);

    // Fetch again and verify updates
    Dashboard fetched2 = client.entities().get(dashboard.getUrn().toString(), Dashboard.class);
    com.linkedin.dashboard.DashboardInfo info2 =
        fetched2.getAspectLazy(com.linkedin.dashboard.DashboardInfo.class);
    assertNotNull(info2);
    assertEquals("2.0", info2.getCustomProperties().get("version"));
    assertEquals("prod", info2.getCustomProperties().get("environment"));
    assertEquals("true", info2.getCustomProperties().get("updated"));
  }

  @Test
  public void testDashboardWithMultipleAspectUpdates() throws Exception {
    // Create a dashboard
    Dashboard dashboard =
        Dashboard.builder()
            .tool("looker")
            .id("test_dashboard_multi_aspect_updates_" + System.currentTimeMillis())
            .title("Dashboard for multi-aspect updates")
            .description("Testing multiple aspect updates in sequence")
            .build();

    client.entities().upsert(dashboard);
    assertNotNull(dashboard.getUrn());

    // Update different aspects
    dashboard.addTag("tag1");
    client.entities().upsert(dashboard);

    dashboard.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    client.entities().upsert(dashboard);

    dashboard.setDomain("urn:li:domain:Engineering");
    client.entities().upsert(dashboard);

    // Fetch and verify all aspects are present
    Dashboard fetched = client.entities().get(dashboard.getUrn().toString(), Dashboard.class);
    assertNotNull(fetched);

    // Verify DashboardInfo
    com.linkedin.dashboard.DashboardInfo info =
        fetched.getAspectLazy(com.linkedin.dashboard.DashboardInfo.class);
    assertNotNull("DashboardInfo should exist", info);

    // Verify GlobalTags
    com.linkedin.common.GlobalTags tags =
        fetched.getAspectLazy(com.linkedin.common.GlobalTags.class);
    assertNotNull("GlobalTags should exist", tags);
    assertTrue("Should have at least one tag", tags.getTags().size() > 0);

    // Verify Ownership
    com.linkedin.common.Ownership ownership =
        fetched.getAspectLazy(com.linkedin.common.Ownership.class);
    assertNotNull("Ownership should exist", ownership);
    assertTrue("Should have at least one owner", ownership.getOwners().size() > 0);
  }
}
