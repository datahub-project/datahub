package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.entity.Chart;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Integration tests for Chart entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class ChartIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testChartCreateMinimal() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_minimal_" + System.currentTimeMillis())
            .build();

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());
  }

  @Test
  public void testChartCreateWithMetadata() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("tableau")
            .id("test_chart_with_metadata_" + System.currentTimeMillis())
            .title("Test Sales Chart")
            .description("This is a test chart created by Java SDK V2")
            .build();

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());
  }

  @Test
  public void testChartWithTags() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_with_tags_" + System.currentTimeMillis())
            .title("Chart with tags")
            .description("Test chart for tag functionality")
            .build();

    chart.addTag("test-tag-1");
    chart.addTag("test-tag-2");
    chart.addTag("executive");

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());
  }

  @Test
  public void testChartWithOwners() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_with_owners_" + System.currentTimeMillis())
            .title("Chart with owners")
            .description("Test chart for ownership functionality")
            .build();

    chart.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    chart.addOwner("urn:li:corpuser:admin", OwnershipType.BUSINESS_OWNER);

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());
  }

  @Test
  public void testChartWithDomain() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_with_domain_" + System.currentTimeMillis())
            .title("Chart with domain")
            .description("Test chart for domain functionality")
            .build();

    chart.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());
  }

  @Test
  public void testChartWithCustomProperties() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_with_custom_props_" + System.currentTimeMillis())
            .title("Chart with custom properties")
            .description("Test chart for custom properties functionality")
            .build();

    chart.addCustomProperty("dashboard", "executive_dashboard");
    chart.addCustomProperty("refresh_schedule", "hourly");
    chart.addCustomProperty("created_by", "java_sdk_v2");

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());

    // Validate custom properties were written correctly
    Map<String, String> expectedCustomProps = new HashMap<>();
    expectedCustomProps.put("dashboard", "executive_dashboard");
    expectedCustomProps.put("refresh_schedule", "hourly");
    expectedCustomProps.put("created_by", "java_sdk_v2");
    validateEntityCustomProperties(chart.getUrn().toString(), Chart.class, expectedCustomProps);
  }

  @Test
  public void testChartWithLineage() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_with_lineage_" + System.currentTimeMillis())
            .title("Chart with lineage")
            .description("Test chart for lineage functionality")
            .build();

    // Add input datasets
    chart.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)"));
    chart.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"));

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());

    // Validate lineage was written correctly
    Chart fetched = client.entities().get(chart.getUrn().toString(), Chart.class);
    assertNotNull(fetched);

    // Verify input datasets are present
    com.linkedin.chart.ChartInfo info = fetched.getAspectLazy(com.linkedin.chart.ChartInfo.class);
    assertNotNull("ChartInfo aspect should exist", info);

    // Check if inputEdges is populated (newer API), otherwise check inputs (older deprecated API)
    if (info.hasInputEdges() && info.getInputEdges() != null && !info.getInputEdges().isEmpty()) {
      assertEquals("Should have 2 input datasets", 2, info.getInputEdges().size());

      // Verify the specific dataset URNs
      java.util.List<String> inputUrns = new java.util.ArrayList<>();
      for (com.linkedin.common.Edge edge : info.getInputEdges()) {
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
    } else if (info.hasInputs() && info.getInputs() != null && !info.getInputs().isEmpty()) {
      // Fallback to deprecated inputs field
      assertEquals("Should have 2 input datasets", 2, info.getInputs().size());

      // Verify the specific dataset URNs from inputs
      java.util.List<String> inputUrns = new java.util.ArrayList<>();
      for (com.linkedin.chart.ChartDataSourceType sourceType : info.getInputs()) {
        if (sourceType.isDatasetUrn()) {
          inputUrns.add(sourceType.getDatasetUrn().toString());
        }
      }
      assertTrue(
          "Should contain sales dataset",
          inputUrns.contains(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)"));
      assertTrue(
          "Should contain customers dataset",
          inputUrns.contains(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"));
    } else {
      fail("Chart should have either inputEdges or inputs populated");
    }
  }

  @Test
  public void testChartWithProperties() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_with_properties_" + System.currentTimeMillis())
            .title("Chart with chart-specific properties")
            .description("Test chart for chart-specific properties")
            .build();

    chart.setChartType("BAR");
    chart.setAccess("PUBLIC");
    chart.setExternalUrl("https://looker.example.com/charts/test");
    chart.setChartUrl("https://looker.example.com/embed/charts/test");
    chart.setLastRefreshed(System.currentTimeMillis());

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());
  }

  @Test
  public void testChartFullMetadata() throws Exception {
    Chart chart =
        Chart.builder()
            .tool("tableau")
            .id("test_chart_full_metadata_" + System.currentTimeMillis())
            .title("Sales Performance Chart")
            .description("Complete chart with all metadata from Java SDK V2")
            .build();

    // Add all types of metadata
    chart.addTag("java-sdk-v2");
    chart.addTag("integration-test");
    chart.addTag("sales");

    chart.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    chart.setDomain("urn:li:domain:Engineering");

    String testRunValue = String.valueOf(System.currentTimeMillis());
    chart.addCustomProperty("created_by", "java_sdk_v2");
    chart.addCustomProperty("test_run", testRunValue);

    chart.setChartType("LINE");
    chart.setAccess("PUBLIC");

    // Add lineage
    chart.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"));

    client.entities().upsert(chart);

    assertNotNull(chart.getUrn());

    // Validate all metadata was written correctly
    validateEntityDescription(
        chart.getUrn().toString(),
        Chart.class,
        "Complete chart with all metadata from Java SDK V2");

    validateEntityHasTags(
        chart.getUrn().toString(),
        Chart.class,
        Arrays.asList("java-sdk-v2", "integration-test", "sales"));

    validateEntityHasOwners(
        chart.getUrn().toString(), Chart.class, Arrays.asList("urn:li:corpuser:datahub"));

    Map<String, String> expectedCustomProps = new HashMap<>();
    expectedCustomProps.put("created_by", "java_sdk_v2");
    expectedCustomProps.put("test_run", testRunValue);
    validateEntityCustomProperties(chart.getUrn().toString(), Chart.class, expectedCustomProps);
  }

  @Test
  public void testMultipleChartCreation() throws Exception {
    // Create multiple charts in sequence
    for (int i = 0; i < 5; i++) {
      Chart chart =
          Chart.builder()
              .tool("looker")
              .id("test_chart_multi_" + i + "_" + System.currentTimeMillis())
              .title("Chart number " + i)
              .description("Test chart number " + i)
              .build();

      chart.addTag("batch-test");
      chart.addCustomProperty("index", String.valueOf(i));

      client.entities().upsert(chart);
    }

    // If we get here, all charts were created successfully
    assertTrue(true);
  }

  @Test
  public void testGetChartWithSpecificAspects() throws Exception {
    // Create a chart with multiple aspects
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_specific_aspects_" + System.currentTimeMillis())
            .title("Chart for specific aspects test")
            .description("Testing fetching with specific aspect list")
            .build();

    chart.addTag("test-aspect-fetching");
    chart.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    chart.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(chart);
    assertNotNull(chart.getUrn());

    // Fetch the chart with only specific aspects
    java.util.List<Class<? extends com.linkedin.data.template.RecordTemplate>> specificAspects =
        new java.util.ArrayList<>();
    specificAspects.add(com.linkedin.chart.ChartInfo.class);
    specificAspects.add(com.linkedin.common.GlobalTags.class);

    Chart fetchedChart =
        client.entities().get(chart.getUrn().toString(), Chart.class, specificAspects);

    assertNotNull("Fetched chart should not be null", fetchedChart);
    assertEquals("URNs should match", chart.getUrn(), fetchedChart.getUrn());

    // Verify ChartInfo aspect is present
    com.linkedin.chart.ChartInfo info =
        fetchedChart.getAspectLazy(com.linkedin.chart.ChartInfo.class);
    assertNotNull("ChartInfo aspect should be present", info);
    assertEquals("Title should match", "Chart for specific aspects test", info.getTitle());

    // Verify GlobalTags aspect is present
    com.linkedin.common.GlobalTags tags =
        fetchedChart.getAspectLazy(com.linkedin.common.GlobalTags.class);
    assertNotNull("GlobalTags aspect should be present", tags);
    assertTrue("Should have at least one tag", tags.getTags().size() > 0);
  }

  @Test
  public void testGetChartWithDefaultAspects() throws Exception {
    // Create a chart
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_default_aspects_" + System.currentTimeMillis())
            .title("Chart for default aspects test")
            .description("Testing fetching with default aspects")
            .build();

    client.entities().upsert(chart);
    assertNotNull(chart.getUrn());

    // Fetch the chart with default aspects (using the get method without aspect list)
    Chart fetchedChart = client.entities().get(chart.getUrn().toString(), Chart.class);

    assertNotNull("Fetched chart should not be null", fetchedChart);
    assertEquals("URNs should match", chart.getUrn(), fetchedChart.getUrn());

    // Verify ChartInfo aspect is present (should be in default aspects)
    com.linkedin.chart.ChartInfo info =
        fetchedChart.getAspectLazy(com.linkedin.chart.ChartInfo.class);
    assertNotNull("ChartInfo aspect should be present", info);
    assertEquals("Title should match", "Chart for default aspects test", info.getTitle());
  }

  @Test
  public void testGetChartAspectDirectly() throws Exception {
    // Create a chart
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_aspect_direct_" + System.currentTimeMillis())
            .title("Chart for direct aspect fetch")
            .description("Testing direct aspect fetching via getAspect")
            .build();

    client.entities().upsert(chart);
    assertNotNull(chart.getUrn());

    // Fetch ChartInfo aspect directly using getAspect
    datahub.client.v2.operations.AspectWithMetadata<com.linkedin.chart.ChartInfo> aspectResult =
        client.entities().getAspect(chart.getUrn(), com.linkedin.chart.ChartInfo.class);

    assertNotNull("AspectWithMetadata should not be null", aspectResult);
    assertNotNull("Aspect value should not be null", aspectResult.getAspect());
    assertEquals(
        "Title should match", "Chart for direct aspect fetch", aspectResult.getAspect().getTitle());
    assertNotNull("System metadata should be present", aspectResult.getSystemMetadata());
    assertNotEquals(
        "Version should not be -1 for existing aspect", "-1", aspectResult.getVersion());
  }

  @Test
  public void testGetNonExistentChartAspect() throws Exception {
    // Try to fetch an aspect for a chart that doesn't exist
    com.linkedin.common.urn.ChartUrn nonExistentUrn =
        new com.linkedin.common.urn.ChartUrn(
            "looker", "nonexistent_chart_" + System.currentTimeMillis());

    datahub.client.v2.operations.AspectWithMetadata<com.linkedin.chart.ChartInfo> aspectResult =
        client.entities().getAspect(nonExistentUrn, com.linkedin.chart.ChartInfo.class);

    assertNotNull("AspectWithMetadata should not be null", aspectResult);
    assertNull("Aspect value should be null", aspectResult.getAspect());
    assertEquals("Version should be -1 for non-existent aspect", "-1", aspectResult.getVersion());
  }

  @Test
  public void testGetChartWithMixedAspects() throws Exception {
    // Create a chart with only ChartInfo (no tags)
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_mixed_aspects_" + System.currentTimeMillis())
            .title("Chart for mixed aspects test")
            .description("Testing fetching with some aspects present and some absent")
            .build();

    client.entities().upsert(chart);
    assertNotNull(chart.getUrn());

    // Request multiple aspects, including one that doesn't exist (GlobalTags was not added)
    java.util.List<Class<? extends com.linkedin.data.template.RecordTemplate>> requestedAspects =
        new java.util.ArrayList<>();
    requestedAspects.add(com.linkedin.chart.ChartInfo.class);
    requestedAspects.add(com.linkedin.common.GlobalTags.class);
    requestedAspects.add(com.linkedin.common.Ownership.class);

    Chart fetchedChart =
        client.entities().get(chart.getUrn().toString(), Chart.class, requestedAspects);

    assertNotNull("Fetched chart should not be null", fetchedChart);

    // ChartInfo should exist (was created)
    com.linkedin.chart.ChartInfo info =
        fetchedChart.getAspectLazy(com.linkedin.chart.ChartInfo.class);
    assertNotNull("ChartInfo aspect should be present", info);

    // GlobalTags and Ownership might not exist since we didn't add them
    // Just verify we can fetch without errors
  }

  @Test
  public void testChartUpdateAndRefetch() throws Exception {
    // Create initial chart
    Chart chart =
        Chart.builder()
            .tool("looker")
            .id("test_chart_update_refetch_" + System.currentTimeMillis())
            .title("Initial Title")
            .description("Initial Description")
            .build();

    client.entities().upsert(chart);
    assertNotNull(chart.getUrn());

    // Fetch it back
    Chart fetched1 = client.entities().get(chart.getUrn().toString(), Chart.class);
    com.linkedin.chart.ChartInfo info1 = fetched1.getAspectLazy(com.linkedin.chart.ChartInfo.class);
    assertEquals("Initial Title", info1.getTitle());

    // Update the chart
    chart.setTitle("Updated Title");
    chart.setDescription("Updated Description");
    client.entities().upsert(chart);

    // Fetch again and verify updates
    Chart fetched2 = client.entities().get(chart.getUrn().toString(), Chart.class);
    com.linkedin.chart.ChartInfo info2 = fetched2.getAspectLazy(com.linkedin.chart.ChartInfo.class);
    assertEquals("Updated Title", info2.getTitle());
    assertEquals("Updated Description", info2.getDescription());
  }
}
