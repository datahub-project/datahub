package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Dataset;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example demonstrating all Dataset metadata operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a dataset with complete metadata
 *   <li>Adding tags, owners, glossary terms
 *   <li>Setting domain and custom properties
 *   <li>Combining all operations in single entity
 * </ul>
 */
public class DatasetFullExample {

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

      // Build comprehensive dataset with all metadata types
      Dataset dataset =
          Dataset.builder()
              .platform("snowflake")
              .name("analytics.public.customer_transactions")
              .env("PROD")
              .description(
                  "Complete customer transaction history including purchases, refunds, and adjustments. "
                      + "This dataset is the source of truth for financial reporting and customer analytics.")
              .displayName("Customer Transactions")
              .build();

      System.out.println("✓ Built dataset with URN: " + dataset.getUrn());

      // Add multiple tags for categorization
      dataset
          .addTag("pii")
          .addTag("financial")
          .addTag("analytics")
          .addTag("gdpr")
          .addTag("production");

      System.out.println("✓ Added 5 tags");

      // Add multiple owners with different roles
      dataset
          .addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:finance_team", OwnershipType.DATA_STEWARD)
          .addOwner("urn:li:corpuser:compliance_team", OwnershipType.DATA_STEWARD);

      System.out.println("✓ Added 3 owners");

      // Add glossary terms for business context
      dataset
          .addTerm("urn:li:glossaryTerm:CustomerData")
          .addTerm("urn:li:glossaryTerm:FinancialTransaction")
          .addTerm("urn:li:glossaryTerm:GDPR.PersonalData");

      System.out.println("✓ Added 3 glossary terms");

      // Set domain for organizational structure
      dataset.setDomain("urn:li:domain:Finance");

      System.out.println("✓ Set domain");

      // Add comprehensive custom properties
      dataset
          .addCustomProperty("team", "data-platform")
          .addCustomProperty("retention_days", "2555") // 7 years for financial data
          .addCustomProperty("refresh_schedule", "hourly")
          .addCustomProperty("source_system", "payment_gateway")
          .addCustomProperty("sla_tier", "tier1")
          .addCustomProperty("encryption", "AES-256")
          .addCustomProperty("backup_frequency", "continuous")
          .addCustomProperty("compliance_level", "PCI-DSS")
          .addCustomProperty("data_classification", "highly-confidential")
          .addCustomProperty("business_criticality", "mission-critical");

      System.out.println("✓ Added 10 custom properties");

      // Count accumulated patches
      System.out.println("\nAccumulated " + dataset.getPendingPatches().size() + " patches");

      // Upsert to DataHub - all metadata in single operation
      client.entities().upsert(dataset);

      System.out.println("\n✓ Successfully created comprehensive dataset in DataHub!");
      System.out.println("\nSummary:");
      System.out.println("  URN: " + dataset.getUrn());
      System.out.println("  Platform: snowflake");
      System.out.println("  Tags: 5");
      System.out.println("  Owners: 3");
      System.out.println("  Glossary Terms: 3");
      System.out.println("  Domain: Finance");
      System.out.println("  Custom Properties: 10");
      System.out.println(
          "\n  View in DataHub: "
              + client.getConfig().getServer()
              + "/dataset/"
              + dataset.getUrn());

    } finally {
      client.close();
    }
  }
}
