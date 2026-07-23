package io.datahubproject.examples;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.DataFlow;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Basic example of creating a DataFlow entity using the Java SDK V2.
 *
 * <p>This example demonstrates:
 *
 * <ul>
 *   <li>Creating a DataFlow with basic properties
 *   <li>Adding tags for categorization
 *   <li>Adding owners for governance
 *   <li>Setting custom properties
 * </ul>
 */
public class DataFlowCreateExample {

  private DataFlowCreateExample() {}

  /**
   * Main method that demonstrates DataFlow creation.
   *
   * @param args command line arguments (not used)
   */
  public static void main(String[] args) {
    // Initialize DataHub client
    String token = System.getenv("DATAHUB_TOKEN");
    try (DataHubClientV2 client =
        DataHubClientV2.builder().server("http://localhost:8080").token(token).build()) {

      // Example 1: Create a basic Airflow DataFlow
      System.out.println("Creating Airflow DataFlow...");
      DataFlow airflowDataFlow =
          DataFlow.builder()
              .orchestrator("airflow")
              .flowId("customer_etl_daily")
              .cluster("prod")
              .displayName("Customer ETL Daily Pipeline")
              .description(
                  "Daily ETL pipeline that processes customer data from MySQL to Snowflake")
              .build();

      // Add tags
      airflowDataFlow.addTag("etl").addTag("production").addTag("daily");

      // Add owners
      airflowDataFlow
          .addOwner("urn:li:corpuser:data_engineering", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:analytics_team", OwnershipType.BUSINESS_OWNER);

      // Add custom properties
      airflowDataFlow
          .addCustomProperty("schedule", "0 2 * * *")
          .addCustomProperty("team", "data-engineering")
          .addCustomProperty("sla_hours", "4");

      // Upsert to DataHub
      client.entities().upsert(airflowDataFlow);
      System.out.println("Created DataFlow: " + airflowDataFlow.getUrn());

      // Example 2: Create a Spark DataFlow
      System.out.println("\nCreating Spark DataFlow...");
      Map<String, String> sparkProperties = new HashMap<>();
      sparkProperties.put("spark.executor.memory", "4g");
      sparkProperties.put("spark.driver.memory", "2g");
      sparkProperties.put("application_type", "batch");

      DataFlow sparkDataFlow =
          DataFlow.builder()
              .orchestrator("spark")
              .flowId("weekly_revenue_report")
              .cluster("prod")
              .displayName("Weekly Revenue Report")
              .description("Weekly aggregation job that generates revenue reports")
              .customProperties(sparkProperties)
              .build();

      sparkDataFlow
          .addTag("reporting")
          .addTag("weekly")
          .addOwner("urn:li:corpuser:analytics_team", OwnershipType.TECHNICAL_OWNER)
          .setDomain("urn:li:domain:Finance");

      client.entities().upsert(sparkDataFlow);
      System.out.println("Created DataFlow: " + sparkDataFlow.getUrn());

      // Example 3: Create a dbt DataFlow
      System.out.println("\nCreating dbt DataFlow...");
      DataFlow dbtDataFlow =
          DataFlow.builder()
              .orchestrator("dbt")
              .flowId("marketing_analytics")
              .cluster("prod")
              .displayName("Marketing Analytics Models")
              .description("dbt project for marketing analytics transformations")
              .build();

      dbtDataFlow
          .addTag("transformation")
          .addTag("analytics")
          .addOwner("urn:li:corpuser:analytics_team", OwnershipType.TECHNICAL_OWNER)
          .addCustomProperty("dbt_version", "1.5.0")
          .addCustomProperty("target", "production")
          .setExternalUrl("https://github.com/mycompany/dbt-marketing")
          .setProject("marketing");

      client.entities().upsert(dbtDataFlow);
      System.out.println("Created DataFlow: " + dbtDataFlow.getUrn());

      System.out.println("\nAll DataFlows created successfully!");

    } catch (ExecutionException | InterruptedException e) {
      System.err.println("Failed to create DataFlow: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } catch (Exception e) {
      System.err.println("Unexpected error: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
