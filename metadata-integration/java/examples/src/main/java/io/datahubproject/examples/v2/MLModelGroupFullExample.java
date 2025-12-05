package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.MLModelGroup;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example demonstrating all ML Model Group metadata operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating an ML model group with complete metadata
 *   <li>Adding tags, owners, glossary terms
 *   <li>Setting domain and custom properties
 *   <li>Adding training and downstream job references
 *   <li>Combining all operations in single entity
 * </ul>
 */
public class MLModelGroupFullExample {

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
      System.out.println("Connected to DataHub");

      // Prepare custom properties
      Map<String, String> customProperties = new HashMap<>();
      customProperties.put("model_family", "transformer");
      customProperties.put("framework", "pytorch");
      customProperties.put("use_case", "customer_churn_prediction");
      customProperties.put("model_type", "classification");
      customProperties.put("target_metric", "f1_score");
      customProperties.put("deployment_status", "production");
      customProperties.put("model_owner_team", "ml-platform");
      customProperties.put("sla_tier", "tier1");

      // Build comprehensive ML model group with all metadata types
      MLModelGroup modelGroup =
          MLModelGroup.builder()
              .platform("sagemaker")
              .groupId("customer-churn-predictor")
              .env("PROD")
              .name("Customer Churn Prediction Model Family")
              .description(
                  "Collection of customer churn prediction models trained on historical customer "
                      + "behavior, subscription data, and engagement metrics. Models are retrained "
                      + "monthly and deployed across all customer touchpoints. This model group "
                      + "contains versions spanning the past 2 years, each representing improvements "
                      + "in feature engineering and hyperparameter tuning.")
              .externalUrl("https://ml-platform.example.com/models/churn-predictor")
              .customProperties(customProperties)
              .trainingJobs(
                  Arrays.asList(
                      "urn:li:dataProcessInstance:training_pipeline_v1_2024_01",
                      "urn:li:dataProcessInstance:training_pipeline_v2_2024_03",
                      "urn:li:dataProcessInstance:training_pipeline_v3_2024_06"))
              .downstreamJobs(
                  Arrays.asList(
                      "urn:li:dataProcessInstance:prediction_service_deployment",
                      "urn:li:dataProcessInstance:batch_scoring_job",
                      "urn:li:dataProcessInstance:real_time_inference_api"))
              .build();

      System.out.println("Built ML model group with URN: " + modelGroup.getUrn());

      // Add multiple tags for categorization
      modelGroup
          .addTag("churn-prediction")
          .addTag("classification")
          .addTag("production")
          .addTag("customer-analytics")
          .addTag("ml-model")
          .addTag("high-priority");

      System.out.println("Added 6 tags");

      // Add multiple owners with different roles
      modelGroup
          .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:data_science_lead", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:product_team", OwnershipType.BUSINESS_OWNER)
          .addOwner("urn:li:corpuser:customer_success", OwnershipType.DATA_STEWARD);

      System.out.println("Added 4 owners");

      // Add glossary terms for business context
      modelGroup
          .addTerm("urn:li:glossaryTerm:MachineLearning.Classification")
          .addTerm("urn:li:glossaryTerm:CustomerAnalytics.ChurnPrediction")
          .addTerm("urn:li:glossaryTerm:ProductionModel");

      System.out.println("Added 3 glossary terms");

      // Set domain for organizational structure
      modelGroup.setDomain("urn:li:domain:MachineLearning");

      System.out.println("Set domain");

      // Add additional training job (demonstrates fluent API)
      modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_pipeline_v4_2024_09");

      System.out.println("Added additional training job");

      // Add additional downstream job
      modelGroup.addDownstreamJob("urn:li:dataProcessInstance:model_monitoring_job");

      System.out.println("Added additional downstream job");

      // Set description for business context (demonstrates patch-based update)
      modelGroup.setDescription(
          "Collection of customer churn prediction models trained on historical customer "
              + "behavior, subscription data, and engagement metrics. Models are retrained "
              + "monthly and deployed across all customer touchpoints. This model group "
              + "contains versions spanning the past 2 years, each representing improvements "
              + "in feature engineering and hyperparameter tuning. Updated with latest accuracy "
              + "metrics and monitoring dashboards.");

      System.out.println("Updated description");

      // Count accumulated patches
      System.out.println("\nAccumulated " + modelGroup.getPendingPatches().size() + " patches");

      // Upsert to DataHub - all metadata in single operation
      client.entities().upsert(modelGroup);

      System.out.println("\nSuccessfully created comprehensive ML model group in DataHub!");
      System.out.println("\nSummary:");
      System.out.println("  URN: " + modelGroup.getUrn());
      System.out.println("  Platform: sagemaker");
      System.out.println("  Tags: 6");
      System.out.println("  Owners: 4");
      System.out.println("  Glossary Terms: 3");
      System.out.println("  Domain: MachineLearning");
      System.out.println("  Custom Properties: 8");
      System.out.println("  Training Jobs: 4");
      System.out.println("  Downstream Jobs: 4");
      System.out.println(
          "\n  View in DataHub: "
              + client.getConfig().getServer()
              + "/mlModelGroup/"
              + modelGroup.getUrn());

    } finally {
      client.close();
    }
  }
}
