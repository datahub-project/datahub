package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.Container;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example demonstrating Container hierarchies using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a three-level container hierarchy (Database → Schema → Table grouping)
 *   <li>Setting up parent-child relationships
 *   <li>Adding complete metadata at each level
 *   <li>Using custom properties for additional context
 *   <li>Proper organization for data warehouse structures
 * </ul>
 */
public class ContainerFullExample {

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

      // ===== Level 1: Database Container =====

      Map<String, String> databaseProperties = new HashMap<>();
      databaseProperties.put("database_type", "analytics");
      databaseProperties.put("size_gb", "2500");
      databaseProperties.put("created_by", "data_platform_team");

      Container database =
          Container.builder()
              .platform("snowflake")
              .database("analytics_db")
              .env("PROD")
              .displayName("Analytics Database")
              .qualifiedName("prod.snowflake.analytics_db")
              .description(
                  "Primary production database for analytics, reporting, and data science workloads. "
                      + "Contains customer data, financial metrics, and operational insights.")
              .externalUrl("https://snowflake.example.com/databases/analytics_db")
              .customProperties(databaseProperties)
              .build();

      System.out.println("✓ Built database container: " + database.getContainerUrn());

      // Add database-level metadata
      database
          .addTag("production")
          .addTag("analytics")
          .addTag("tier1")
          .addOwner("urn:li:corpuser:data_platform_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:analytics_lead", OwnershipType.DATA_STEWARD)
          .addTerm("urn:li:glossaryTerm:ProductionDatabase")
          .setDomain("urn:li:domain:Analytics");

      System.out.println("✓ Added database metadata (3 tags, 2 owners, 1 term, domain)");

      // ===== Level 2: Schema Container =====

      Map<String, String> schemaProperties = new HashMap<>();
      schemaProperties.put("schema_type", "fact_tables");
      schemaProperties.put("table_count", "45");
      schemaProperties.put("refresh_schedule", "hourly");

      Container schema =
          Container.builder()
              .platform("snowflake")
              .database("analytics_db")
              .schema("public")
              .env("PROD")
              .displayName("Public Schema")
              .qualifiedName("prod.snowflake.analytics_db.public")
              .description(
                  "Main schema containing core fact and dimension tables for analytics. "
                      + "All production applications read from this schema.")
              .externalUrl("https://snowflake.example.com/databases/analytics_db/schemas/public")
              .parentContainer(database.getContainerUrn())
              .customProperties(schemaProperties)
              .build();

      System.out.println("✓ Built schema container: " + schema.getContainerUrn());

      // Add schema-level metadata
      schema
          .addTag("public")
          .addTag("production-ready")
          .addTag("high-traffic")
          .addOwner("urn:li:corpuser:analytics_team", OwnershipType.TECHNICAL_OWNER)
          .addTerm("urn:li:glossaryTerm:AnalyticsSchema")
          .setDomain("urn:li:domain:Analytics");

      System.out.println("✓ Added schema metadata (3 tags, 1 owner, 1 term, domain)");

      // ===== Level 3: Table Group Container (Logical grouping) =====

      Map<String, String> groupProperties = new HashMap<>();
      groupProperties.put("group_type", "customer_analytics");
      groupProperties.put("table_count", "12");
      groupProperties.put("owner_team", "customer_insights");

      Container tableGroup =
          Container.builder()
              .platform("snowflake")
              .database("analytics_db")
              .schema("public")
              .env("PROD")
              .displayName("Customer Analytics Tables")
              .qualifiedName("prod.snowflake.analytics_db.public.customer_group")
              .description(
                  "Logical grouping of customer-related tables including transactions, profiles, "
                      + "and engagement metrics. Used for customer 360 analytics.")
              .parentContainer(schema.getContainerUrn())
              .customProperties(groupProperties)
              .build();

      System.out.println("✓ Built table group container: " + tableGroup.getContainerUrn());

      // Add table group metadata
      tableGroup
          .addTag("customer-data")
          .addTag("pii")
          .addTag("gdpr")
          .addOwner("urn:li:corpuser:customer_insights_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:compliance_team", OwnershipType.DATA_STEWARD)
          .addTerm("urn:li:glossaryTerm:CustomerData")
          .addTerm("urn:li:glossaryTerm:GDPR.PersonalData")
          .setDomain("urn:li:domain:CustomerAnalytics");

      System.out.println("✓ Added table group metadata (3 tags, 2 owners, 2 terms, domain)");

      // ===== Upsert All Containers =====

      System.out.println("\nUpserting container hierarchy to DataHub...");

      // Upsert in order: parent before children
      client.entities().upsert(database);
      System.out.println("  ✓ Upserted database");

      client.entities().upsert(schema);
      System.out.println("  ✓ Upserted schema");

      client.entities().upsert(tableGroup);
      System.out.println("  ✓ Upserted table group");

      // ===== Summary =====

      System.out.println("\n✓ Successfully created container hierarchy in DataHub!");
      System.out.println("\nHierarchy Structure:");
      System.out.println("  📁 Analytics Database (analytics_db)");
      System.out.println("     └─ 📁 Public Schema (public)");
      System.out.println("        └─ 📁 Customer Analytics Tables");

      System.out.println("\nMetadata Summary:");
      System.out.println("  Database:");
      System.out.println("    - Tags: 3 | Owners: 2 | Terms: 1 | Custom Properties: 3");
      System.out.println("    - URN: " + database.getContainerUrn());
      System.out.println(
          "    - URL: "
              + client.getConfig().getServer()
              + "/container/"
              + database.getContainerUrn());

      System.out.println("\n  Schema:");
      System.out.println("    - Tags: 3 | Owners: 1 | Terms: 1 | Custom Properties: 3");
      System.out.println("    - Parent: Database");
      System.out.println("    - URN: " + schema.getContainerUrn());

      System.out.println("\n  Table Group:");
      System.out.println("    - Tags: 3 | Owners: 2 | Terms: 2 | Custom Properties: 3");
      System.out.println("    - Parent: Schema");
      System.out.println("    - URN: " + tableGroup.getContainerUrn());

    } finally {
      client.close();
    }
  }
}
