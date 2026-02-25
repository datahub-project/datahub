package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.DataJob;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example demonstrating all DataJob metadata operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a data job with complete metadata
 *   <li>Adding tags, owners, glossary terms
 *   <li>Setting domain and custom properties
 *   <li>Defining lineage with input datasets (inlets) and output datasets (outlets)
 *   <li>Combining all operations in single entity
 * </ul>
 */
public class DataJobFullExample {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
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

      // Build comprehensive data job with all metadata types
      DataJob dataJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("financial_reporting_pipeline")
              .cluster("prod")
              .jobId("aggregate_customer_transactions")
              .description(
                  "Critical ETL job that aggregates daily customer transaction data from multiple sources "
                      + "and loads into the enterprise data warehouse. Includes data quality checks, "
                      + "PII tokenization, and regulatory compliance validation.")
              .name("Aggregate Customer Transactions")
              .type("BATCH")
              .build();

      System.out.println("✓ Built data job with URN: " + dataJob.getUrn());

      // Add multiple tags for categorization
      dataJob
          .addTag("critical")
          .addTag("pii")
          .addTag("financial")
          .addTag("etl")
          .addTag("production");

      System.out.println("✓ Added 5 tags");

      // Add multiple owners with different roles
      dataJob
          .addOwner("urn:li:corpuser:data_engineering", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:finance_team", OwnershipType.BUSINESS_OWNER)
          .addOwner("urn:li:corpuser:compliance_team", OwnershipType.DATA_STEWARD);

      System.out.println("✓ Added 3 owners");

      // Add glossary terms for business context
      dataJob
          .addTerm("urn:li:glossaryTerm:DataProcessing")
          .addTerm("urn:li:glossaryTerm:ETL")
          .addTerm("urn:li:glossaryTerm:FinancialReporting");

      System.out.println("✓ Added 3 glossary terms");

      // Set domain for organizational structure
      dataJob.setDomain("urn:li:domain:Finance");

      System.out.println("✓ Set domain");

      // Add comprehensive custom properties
      dataJob
          .addCustomProperty("team", "data-platform")
          .addCustomProperty("schedule", "0 2 * * *") // Daily at 2 AM
          .addCustomProperty("retries", "3")
          .addCustomProperty("timeout", "7200") // 2 hours
          .addCustomProperty("sla_hours", "4")
          .addCustomProperty("priority", "high")
          .addCustomProperty("notification_channel", "#data-alerts")
          .addCustomProperty("requires_manual_approval", "false")
          .addCustomProperty("data_classification", "highly-confidential")
          .addCustomProperty("compliance_level", "PCI-DSS");

      System.out.println("✓ Added 10 custom properties");

      // Add lineage: Define input datasets (inlets) that this job reads from
      dataJob
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)")
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.customers,PROD)")
          .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:kafka,events.user_activity,PROD)");

      System.out.println("✓ Added 3 input datasets (inlets)");

      // Add lineage: Define output datasets (outlets) that this job writes to
      dataJob
          .addOutputDataset(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_transactions,PROD)")
          .addOutputDataset(
              "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.daily_summary,PROD)");

      System.out.println("✓ Added 2 output datasets (outlets)");

      // Count accumulated patches
      System.out.println("\nAccumulated " + dataJob.getPendingPatches().size() + " patches");

      // Upsert to DataHub - all metadata in single operation
      client.entities().upsert(dataJob);

      System.out.println("\n✓ Successfully created comprehensive data job in DataHub!");
      System.out.println("\nSummary:");
      System.out.println("  URN: " + dataJob.getUrn());
      System.out.println("  Orchestrator: airflow");
      System.out.println("  Flow: financial_reporting_pipeline");
      System.out.println("  Job: aggregate_customer_transactions");
      System.out.println("  Tags: 5");
      System.out.println("  Owners: 3");
      System.out.println("  Glossary Terms: 3");
      System.out.println("  Domain: Finance");
      System.out.println("  Custom Properties: 10");
      System.out.println("  Input Datasets (Inlets): 3");
      System.out.println("  Output Datasets (Outlets): 2");
      System.out.println(
          "\n  View in DataHub: "
              + client.getConfig().getServer()
              + "/dataJob/"
              + dataJob.getUrn());
      System.out.println("\nLineage Flow:");
      System.out.println(
          "  raw.transactions, raw.customers, events.user_activity → [Job] → customer_transactions, daily_summary");

    } finally {
      client.close();
    }
  }
}
