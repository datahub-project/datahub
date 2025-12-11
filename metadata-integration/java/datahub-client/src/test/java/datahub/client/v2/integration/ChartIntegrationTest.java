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
}
