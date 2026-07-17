package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.MLModelGroup;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating how to create an ML Model Group using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating a DataHubClientV2
 *   <li>Building an ML Model Group with fluent builder
 *   <li>Adding metadata (tags, owners, description)
 *   <li>Adding training job references
 *   <li>Upserting to DataHub
 * </ul>
 */
public class MLModelGroupCreateExample {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    // Create client (use environment variables or pass explicit values)
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
      System.out.println("Connected to DataHub");

      // Build ML model group for versioned recommendation models
      MLModelGroup modelGroup =
          MLModelGroup.builder()
              .platform("mlflow")
              .groupId("product-recommender")
              .env("PROD")
              .name("Product Recommendation Model Family")
              .description(
                  "Collection of product recommendation models trained on user interaction data")
              .build();

      System.out.println("Built ML model group with URN: " + modelGroup.getUrn());

      // Add tags
      modelGroup.addTag("recommendation").addTag("ml-production").addTag("customer-facing");

      System.out.println("Added 3 tags");

      // Add owners
      modelGroup
          .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:product_team", OwnershipType.BUSINESS_OWNER);

      System.out.println("Added 2 owners");

      // Add training job reference
      modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_pipeline_2024_01");

      System.out.println("Added training job reference");

      // Set domain
      modelGroup.setDomain("urn:li:domain:MachineLearning");

      System.out.println("Set domain");

      // Upsert to DataHub
      client.entities().upsert(modelGroup);

      System.out.println("Successfully created ML model group in DataHub!");
      System.out.println("\n  URN: " + modelGroup.getUrn());
      System.out.println(
          "  View in DataHub: "
              + client.getConfig().getServer()
              + "/mlModelGroup/"
              + modelGroup.getUrn());

    } finally {
      client.close();
    }
  }
}
