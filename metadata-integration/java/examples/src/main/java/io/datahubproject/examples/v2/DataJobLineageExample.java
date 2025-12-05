package io.datahubproject.examples.v2;

import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.DataJob;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Focused example demonstrating DataJob lineage operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a DataJob with input datasets and output datasets
 *   <li>Adding and removing individual inputs and outputs
 *   <li>Setting multiple inputs/outputs at once
 *   <li>Building a complete ETL pipeline with lineage
 *   <li>Understanding the relationship between DataJob and DataFlow
 * </ul>
 *
 * <p>Lineage Concepts:
 *
 * <ul>
 *   <li><b>Input Datasets</b> - Datasets that the job reads from
 *   <li><b>Output Datasets</b> - Datasets that the job writes to
 *   <li>Lineage flows: Dataset (input) → DataJob → Dataset (output)
 * </ul>
 */
public class DataJobLineageExample {

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
      System.out.println("✓ Connected to DataHub\n");

      // ==================== Example 1: Simple ETL Job with Lineage ====================
      System.out.println("Example 1: Creating ETL job with lineage");
      System.out.println("==========================================");

      DataJob etlJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("customer_etl_pipeline")
              .cluster("prod")
              .jobId("load_customers")
              .description("Extracts customer data from PostgreSQL and loads into Snowflake")
              .name("Load Customers")
              .type("BATCH")
              .build();

      // Add input dataset (source)
      etlJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:postgres,public.customers,PROD)");

      // Add output dataset (destination)
      etlJob.addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.customers,PROD)");

      client.entities().upsert(etlJob);

      System.out.println("✓ Created ETL job: " + etlJob.getUrn());
      System.out.println(
          "  Lineage: postgres.public.customers → [Load Customers] → snowflake.dwh.customers\n");

      // ==================== Example 2: Complex Aggregation Job ====================
      System.out.println("Example 2: Complex aggregation with multiple inputs/outputs");
      System.out.println("============================================================");

      DataJob aggregationJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("analytics_pipeline")
              .cluster("prod")
              .jobId("aggregate_sales_metrics")
              .description("Aggregates sales data from multiple sources into summary tables")
              .name("Aggregate Sales Metrics")
              .type("BATCH")
              .build();

      // Add multiple input datasets (different sources)
      aggregationJob
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)")
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.customers,PROD)")
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.products,PROD)")
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:kafka,events.purchases,PROD)");

      // Add multiple output datasets (different aggregation levels)
      aggregationJob
          .addOutputDataset(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.daily_sales,PROD)")
          .addOutputDataset(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.monthly_sales,PROD)")
          .addOutputDataset(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_summary,PROD)");

      client.entities().upsert(aggregationJob);

      System.out.println("✓ Created aggregation job: " + aggregationJob.getUrn());
      System.out.println("  Input datasets: 4 (Snowflake tables + Kafka topic)");
      System.out.println("  Output datasets: 3 (daily, monthly, customer summaries)\n");

      // ==================== Example 3: Using setInputDatasets/setOutputDatasets
      // ====================
      System.out.println("Example 3: Setting multiple inlets/outlets at once");
      System.out.println("===================================================");

      DataJob batchJob =
          DataJob.builder()
              .orchestrator("dagster")
              .flowId("data_quality_pipeline")
              .cluster("prod")
              .jobId("validate_warehouse_tables")
              .description("Runs data quality checks on warehouse tables")
              .name("Validate Warehouse Tables")
              .type("BATCH")
              .build();

      // Set all inlets at once
      batchJob.setInputDatasets(
          Arrays.asList(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.orders,PROD)",
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.customers,PROD)",
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.products,PROD)"));

      // Set all outlets at once
      batchJob.setOutputDatasets(
          Arrays.asList(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,quality.validation_results,PROD)",
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,quality.data_quality_metrics,PROD)"));

      client.entities().upsert(batchJob);

      System.out.println("✓ Created validation job: " + batchJob.getUrn());
      System.out.println("  Set 3 inlets and 2 outlets in batch operations\n");

      // ==================== Example 4: Updating Lineage ====================
      System.out.println("Example 4: Updating existing job lineage");
      System.out.println("=========================================");

      // Create a simple job first
      DataJob updateJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("data_processing_pipeline")
              .cluster("prod")
              .jobId("process_events")
              .description("Processes event data")
              .name("Process Events")
              .type("BATCH")
              .build();

      // Initial lineage
      updateJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:kafka,events.raw,PROD)");
      updateJob.addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,processed/events,PROD)");

      client.entities().upsert(updateJob);
      System.out.println("✓ Created job with initial lineage");

      // Add another input source (e.g., requirement changed)
      updateJob.addInputDataset(
          "urn:li:dataset:(urn:li:dataPlatform:kafka,events.enrichment,PROD)");

      // Add another output destination
      updateJob.addOutputDataset(
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.events,PROD)");

      client.entities().upsert(updateJob);
      System.out.println("✓ Updated job with additional inlet and outlet\n");

      // ==================== Example 5: Using DatasetUrn directly ====================
      System.out.println("Example 5: Using DatasetUrn objects for type safety");
      System.out.println("====================================================");

      DataJob typedJob =
          DataJob.builder()
              .orchestrator("spark")
              .flowId("ml_feature_pipeline")
              .cluster("prod")
              .jobId("generate_features")
              .description("Generates ML features from raw data")
              .name("Generate ML Features")
              .type("BATCH")
              .build();

      // Create DatasetUrn objects for type safety
      DatasetUrn sourceUrn =
          DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,user_events,PROD)");
      DatasetUrn featureUrn =
          DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,ml_features,PROD)");

      // Use typed URN objects
      typedJob.addInputDataset(sourceUrn).addOutputDataset(featureUrn);

      client.entities().upsert(typedJob);

      System.out.println("✓ Created job using DatasetUrn objects");
      System.out.println("  Type-safe URN creation prevents errors\n");

      // ==================== Example 6: Complete Data Pipeline ====================
      System.out.println("Example 6: Modeling a complete ETL pipeline");
      System.out.println("============================================");
      System.out.println("Pipeline: Extract → Transform → Load");
      System.out.println();

      // Extract job
      DataJob extractJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("complete_etl_pipeline")
              .cluster("prod")
              .jobId("extract_from_source")
              .description("Extracts data from operational database")
              .name("Extract from Source DB")
              .type("BATCH")
              .build();

      extractJob
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:mysql,production.orders,PROD)")
          .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders_raw,PROD)");

      client.entities().upsert(extractJob);
      System.out.println("✓ Extract: mysql.orders → s3.staging/orders_raw");

      // Transform job
      DataJob transformJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("complete_etl_pipeline")
              .cluster("prod")
              .jobId("transform_data")
              .description("Cleanses and transforms extracted data")
              .name("Transform Data")
              .type("BATCH")
              .build();

      transformJob
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders_raw,PROD)")
          .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders_clean,PROD)");

      client.entities().upsert(transformJob);
      System.out.println("✓ Transform: s3.staging/orders_raw → s3.staging/orders_clean");

      // Load job
      DataJob loadJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("complete_etl_pipeline")
              .cluster("prod")
              .jobId("load_to_warehouse")
              .description("Loads transformed data into data warehouse")
              .name("Load to Warehouse")
              .type("BATCH")
              .build();

      loadJob
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:s3,staging/orders_clean,PROD)")
          .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)");

      client.entities().upsert(loadJob);
      System.out.println("✓ Load: s3.staging/orders_clean → snowflake.analytics.orders");
      System.out.println();
      System.out.println("Complete lineage created:");
      System.out.println(
          "  mysql.orders → [Extract] → s3.raw → [Transform] → s3.clean → [Load] → snowflake.analytics\n");

      // ==================== Summary ====================
      System.out.println("==========================================");
      System.out.println("Summary: Successfully created 9 data jobs with comprehensive lineage");
      System.out.println("==========================================");
      System.out.println("\nKey Takeaways:");
      System.out.println("  • Inlets represent input datasets (sources)");
      System.out.println("  • Outlets represent output datasets (destinations)");
      System.out.println("  • Use addInputDataset/addOutputDataset for single operations");
      System.out.println("  • Use setInputDatasets/setOutputDatasets for batch operations");
      System.out.println("  • DatasetUrn objects provide type safety");
      System.out.println("  • All jobs belong to a DataFlow (pipeline/DAG)");
      System.out.println("\nView your lineage graphs in DataHub UI!");

    } finally {
      client.close();
    }
  }
}
