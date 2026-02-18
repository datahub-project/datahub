package io.datahubproject.examples;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.DataFlow;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example of creating a DataFlow entity with all available metadata.
 *
 * <p>This example demonstrates:
 *
 * <ul>
 *   <li>Creating a DataFlow with comprehensive metadata
 *   <li>Adding multiple tags for detailed categorization
 *   <li>Adding multiple owners with different ownership types
 *   <li>Adding glossary terms for semantic enrichment
 *   <li>Setting domain for data governance
 *   <li>Adding extensive custom properties
 *   <li>Setting timestamps and external URLs
 * </ul>
 */
public class DataFlowFullExample {

  private DataFlowFullExample() {}

  /**
   * Main method that demonstrates comprehensive DataFlow creation.
   *
   * @param args command line arguments (not used)
   */
  public static void main(String[] args) {
    String token = System.getenv("DATAHUB_TOKEN");
    try (DataHubClientV2 client =
        DataHubClientV2.builder().server("http://localhost:8080").token(token).build()) {

      // Example 1: Comprehensive Airflow DataFlow
      System.out.println("Creating comprehensive Airflow DataFlow...");

      Map<String, String> airflowCustomProps = new HashMap<>();
      airflowCustomProps.put("schedule", "0 2 * * *");
      airflowCustomProps.put("catchup", "false");
      airflowCustomProps.put("max_active_runs", "1");
      airflowCustomProps.put("dagrun_timeout", "3600");
      airflowCustomProps.put("team", "data-engineering");
      airflowCustomProps.put("slack_channel", "#data-alerts");
      airflowCustomProps.put("on_call", "data-engineering-oncall");
      airflowCustomProps.put("sla_hours", "4");
      airflowCustomProps.put("criticality", "high");

      DataFlow comprehensiveAirflowFlow =
          DataFlow.builder()
              .orchestrator("airflow")
              .flowId("enterprise_customer_data_pipeline")
              .cluster("prod-us-west-2")
              .displayName("Enterprise Customer Data Pipeline")
              .description(
                  "Production pipeline that ingests customer data from multiple sources (MySQL, Salesforce, "
                      + "PostgreSQL), performs data quality checks, transformations, and loads into Snowflake data warehouse. "
                      + "Runs daily at 2 AM PST with 4-hour SLA.")
              .customProperties(airflowCustomProps)
              .build();

      // Add comprehensive tags
      comprehensiveAirflowFlow
          .addTag("etl")
          .addTag("production")
          .addTag("daily")
          .addTag("customer-data")
          .addTag("pii")
          .addTag("high-priority")
          .addTag("data-quality")
          .addTag("snowflake");

      // Add multiple owners with different types
      comprehensiveAirflowFlow
          .addOwner("urn:li:corpuser:john_doe", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:jane_smith", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:analytics_lead", OwnershipType.BUSINESS_OWNER)
          .addOwner("urn:li:corpuser:data_steward", OwnershipType.DATA_STEWARD);

      // Add glossary terms
      comprehensiveAirflowFlow
          .addTerm("urn:li:glossaryTerm:ETL")
          .addTerm("urn:li:glossaryTerm:CustomerData")
          .addTerm("urn:li:glossaryTerm:DataWarehouse")
          .addTerm("urn:li:glossaryTerm:DataQuality");

      // Set domain
      comprehensiveAirflowFlow.setDomain("urn:li:domain:DataEngineering");

      // Set additional properties
      comprehensiveAirflowFlow
          .setProject("customer_analytics")
          .setExternalUrl("https://airflow.example.com/dags/enterprise_customer_data_pipeline")
          .setCreated(System.currentTimeMillis() - 86400000L * 30) // Created 30 days ago
          .setLastModified(System.currentTimeMillis() - 86400000L); // Modified 1 day ago

      // Upsert to DataHub
      client.entities().upsert(comprehensiveAirflowFlow);
      System.out.println("Created comprehensive DataFlow: " + comprehensiveAirflowFlow.getUrn());

      // Example 2: Comprehensive Spark DataFlow
      System.out.println("\nCreating comprehensive Spark DataFlow...");

      Map<String, String> sparkCustomProps = new HashMap<>();
      sparkCustomProps.put("spark.executor.memory", "8g");
      sparkCustomProps.put("spark.driver.memory", "4g");
      sparkCustomProps.put("spark.executor.cores", "4");
      sparkCustomProps.put("spark.dynamicAllocation.enabled", "true");
      sparkCustomProps.put("application_type", "batch");
      sparkCustomProps.put("team", "ml-engineering");
      sparkCustomProps.put("cost_center", "ML-001");
      sparkCustomProps.put("estimated_cost_per_run", "$45");

      DataFlow comprehensiveSparkFlow =
          DataFlow.builder()
              .orchestrator("spark")
              .flowId("ml_feature_generation_pipeline")
              .cluster("prod-emr-cluster")
              .displayName("ML Feature Generation Pipeline")
              .description(
                  "Large-scale Spark job that generates ML features from raw event data. "
                      + "Processes ~10TB of data daily, generating 500+ features for recommendation models.")
              .customProperties(sparkCustomProps)
              .build();

      // Add tags
      comprehensiveSparkFlow
          .addTag("spark")
          .addTag("machine-learning")
          .addTag("feature-engineering")
          .addTag("large-scale")
          .addTag("production")
          .addTag("emr");

      // Add owners
      comprehensiveSparkFlow
          .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:recommendations_pm", OwnershipType.BUSINESS_OWNER);

      // Add glossary terms
      comprehensiveSparkFlow
          .addTerm("urn:li:glossaryTerm:MachineLearning")
          .addTerm("urn:li:glossaryTerm:FeatureEngineering")
          .addTerm("urn:li:glossaryTerm:BigData");

      comprehensiveSparkFlow
          .setDomain("urn:li:domain:MachineLearning")
          .setProject("recommendations")
          .setExternalUrl("https://spark-ui.example.com/ml_feature_generation");

      client.entities().upsert(comprehensiveSparkFlow);
      System.out.println(
          "Created comprehensive Spark DataFlow: " + comprehensiveSparkFlow.getUrn());

      // Example 3: Comprehensive dbt DataFlow
      System.out.println("\nCreating comprehensive dbt DataFlow...");

      Map<String, String> dbtCustomProps = new HashMap<>();
      dbtCustomProps.put("dbt_version", "1.5.0");
      dbtCustomProps.put("target", "production");
      dbtCustomProps.put("warehouse", "snowflake");
      dbtCustomProps.put("schema_prefix", "analytics_");
      dbtCustomProps.put("models_count", "87");
      dbtCustomProps.put("tests_count", "245");
      dbtCustomProps.put("team", "analytics-engineering");

      DataFlow comprehensiveDbtFlow =
          DataFlow.builder()
              .orchestrator("dbt")
              .flowId("marketing_analytics_transformations")
              .cluster("prod")
              .displayName("Marketing Analytics Transformations")
              .description(
                  "dbt project containing all marketing analytics transformations. "
                      + "Includes customer segmentation, campaign attribution, ROI calculations, and funnel analysis. "
                      + "87 models with 245 data quality tests.")
              .customProperties(dbtCustomProps)
              .build();

      // Add tags
      comprehensiveDbtFlow
          .addTag("dbt")
          .addTag("transformation")
          .addTag("analytics")
          .addTag("marketing")
          .addTag("data-quality")
          .addTag("tested");

      // Add owners
      comprehensiveDbtFlow
          .addOwner("urn:li:corpuser:analytics_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:marketing_vp", OwnershipType.BUSINESS_OWNER)
          .addOwner("urn:li:corpuser:data_quality_team", OwnershipType.DATA_STEWARD);

      // Add glossary terms
      comprehensiveDbtFlow
          .addTerm("urn:li:glossaryTerm:DataTransformation")
          .addTerm("urn:li:glossaryTerm:MarketingAnalytics")
          .addTerm("urn:li:glossaryTerm:DataModeling");

      comprehensiveDbtFlow
          .setDomain("urn:li:domain:Marketing")
          .setProject("marketing-analytics")
          .setExternalUrl("https://github.com/mycompany/dbt-marketing-analytics")
          .setCreated(System.currentTimeMillis() - 86400000L * 90) // Created 90 days ago
          .setLastModified(System.currentTimeMillis() - 3600000L); // Modified 1 hour ago

      client.entities().upsert(comprehensiveDbtFlow);
      System.out.println("Created comprehensive dbt DataFlow: " + comprehensiveDbtFlow.getUrn());

      // Example 4: Real-time streaming DataFlow (Apache Flink)
      System.out.println("\nCreating Flink streaming DataFlow...");

      Map<String, String> flinkCustomProps = new HashMap<>();
      flinkCustomProps.put("parallelism", "16");
      flinkCustomProps.put("checkpoint_interval", "60000");
      flinkCustomProps.put("state_backend", "rocksdb");
      flinkCustomProps.put("team", "real-time-systems");

      DataFlow streamingFlow =
          DataFlow.builder()
              .orchestrator("flink")
              .flowId("real_time_fraud_detection")
              .cluster("prod-flink-cluster")
              .displayName("Real-time Fraud Detection")
              .description(
                  "Real-time Flink streaming job for fraud detection on payment transactions. "
                      + "Processes ~10k transactions per second with <100ms latency.")
              .customProperties(flinkCustomProps)
              .build();

      streamingFlow
          .addTag("streaming")
          .addTag("real-time")
          .addTag("fraud-detection")
          .addTag("critical")
          .addTag("flink")
          .addOwner("urn:li:corpuser:security_team", OwnershipType.TECHNICAL_OWNER)
          .addTerm("urn:li:glossaryTerm:FraudDetection")
          .setDomain("urn:li:domain:Security")
          .setProject("fraud-prevention");

      client.entities().upsert(streamingFlow);
      System.out.println("Created Flink streaming DataFlow: " + streamingFlow.getUrn());

      System.out.println("\n=== All comprehensive DataFlows created successfully! ===");
      System.out.println("\nDataFlows created:");
      System.out.println("1. " + comprehensiveAirflowFlow.getUrn());
      System.out.println("2. " + comprehensiveSparkFlow.getUrn());
      System.out.println("3. " + comprehensiveDbtFlow.getUrn());
      System.out.println("4. " + streamingFlow.getUrn());

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
