package io.datahubproject.examples.v2;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.DataHubClientV2;
import datahub.client.v2.entity.MLModel;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Comprehensive example demonstrating all ML Model metadata operations using Java SDK V2.
 *
 * <p>This example shows:
 *
 * <ul>
 *   <li>Creating an ML model with complete metadata
 *   <li>Adding comprehensive training metrics and hyperparameters
 *   <li>Setting model group relationships
 *   <li>Adding training and downstream jobs for lineage
 *   <li>Adding deployment information
 *   <li>Adding tags, owners, glossary terms, and domain
 *   <li>Combining all ML-specific operations
 * </ul>
 */
public class MLModelFullExample {

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

      // Build comprehensive ML model with all metadata types
      MLModel model =
          MLModel.builder()
              .platform("tensorflow")
              .name("customer_lifetime_value_predictor")
              .env("PROD")
              .displayName("Customer Lifetime Value Prediction Model")
              .description(
                  "Deep learning model predicting customer lifetime value (CLV) based on historical "
                      + "purchase behavior, engagement metrics, and demographic information. This model "
                      + "is used by the marketing team for customer segmentation and targeted campaigns.")
              .externalUrl("https://mlflow.company.com/experiments/42/runs/abc123")
              .build();

      System.out.println("Built ML model with URN: " + model.getUrn());

      // Add comprehensive training metrics
      model
          .addTrainingMetric("accuracy", "0.947")
          .addTrainingMetric("precision", "0.934")
          .addTrainingMetric("recall", "0.921")
          .addTrainingMetric("f1_score", "0.927")
          .addTrainingMetric("auc_roc", "0.965")
          .addTrainingMetric("auc_pr", "0.952")
          .addTrainingMetric("log_loss", "0.183")
          .addTrainingMetric("mse", "0.042")
          .addTrainingMetric("mae", "0.156")
          .addTrainingMetric("r2_score", "0.889")
          .addTrainingMetric("training_time_minutes", "142.5")
          .addTrainingMetric("validation_accuracy", "0.941");

      System.out.println("Added 12 training metrics");

      // Add comprehensive hyperparameters
      model
          .addHyperParam("learning_rate", "0.001")
          .addHyperParam("batch_size", "64")
          .addHyperParam("epochs", "100")
          .addHyperParam("optimizer", "adam")
          .addHyperParam("loss_function", "binary_crossentropy")
          .addHyperParam("dropout_rate", "0.3")
          .addHyperParam("hidden_layers", "3")
          .addHyperParam("hidden_units", "[256, 128, 64]")
          .addHyperParam("activation", "relu")
          .addHyperParam("l2_regularization", "0.001")
          .addHyperParam("early_stopping_patience", "10")
          .addHyperParam("learning_rate_decay", "0.95");

      System.out.println("Added 12 hyperparameters");

      // Set model group (collection of related models)
      model.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,clv_models,PROD)");

      System.out.println("Set model group");

      // Add training jobs for lineage tracking
      model
          .addTrainingJob(
              "urn:li:dataJob:(urn:li:dataFlow:(airflow,ml_training_dag,prod),train_clv_model)")
          .addTrainingJob(
              "urn:li:dataProcessInstance:(urn:li:dataFlow:(airflow,ml_training_dag,prod),2025-10-15T08:00:00Z)");

      System.out.println("Added 2 training jobs");

      // Add downstream jobs that use this model
      model
          .addDownstreamJob(
              "urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_scoring_dag,prod),score_customers)")
          .addDownstreamJob(
              "urn:li:dataJob:(urn:li:dataFlow:(airflow,campaign_targeting_dag,prod),select_high_value_customers)");

      System.out.println("Added 2 downstream jobs");

      // Add deployments
      model
          .addDeployment(
              "urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,clv-model-staging)")
          .addDeployment(
              "urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,clv-model-production)")
          .addDeployment(
              "urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,clv-model-canary)");

      System.out.println("Added 3 deployments");

      // Add multiple tags for categorization
      model
          .addTag("production")
          .addTag("ml-model")
          .addTag("deep-learning")
          .addTag("tensorflow")
          .addTag("classification")
          .addTag("customer-analytics")
          .addTag("business-critical");

      System.out.println("Added 7 tags");

      // Add multiple owners with different roles
      model
          .addOwner("urn:li:corpuser:ml_platform_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:data_science_team", OwnershipType.DATA_STEWARD)
          .addOwner("urn:li:corpuser:marketing_analytics", OwnershipType.DATA_STEWARD);

      System.out.println("Added 3 owners");

      // Add glossary terms for business context
      model
          .addTerm("urn:li:glossaryTerm:MachineLearning.Model")
          .addTerm("urn:li:glossaryTerm:CustomerAnalytics.LifetimeValue")
          .addTerm("urn:li:glossaryTerm:Marketing.Prediction");

      System.out.println("Added 3 glossary terms");

      // Set domain for organizational structure
      model.setDomain("urn:li:domain:MachineLearning");

      System.out.println("Set domain");

      // Add comprehensive custom properties
      model
          .addCustomProperty("framework", "TensorFlow 2.14")
          .addCustomProperty("framework_version", "2.14.0")
          .addCustomProperty("training_date", "2025-10-15")
          .addCustomProperty("model_version", "2.1.0")
          .addCustomProperty("training_dataset", "customer_events_2023_2025")
          .addCustomProperty("training_dataset_size", "5000000")
          .addCustomProperty("training_duration_hours", "2.4")
          .addCustomProperty("model_type", "deep_neural_network")
          .addCustomProperty("deployment_environment", "aws_sagemaker")
          .addCustomProperty("model_size_mb", "245")
          .addCustomProperty("inference_latency_ms", "15")
          .addCustomProperty("production_deployment_date", "2025-10-20")
          .addCustomProperty("last_retrained_date", "2025-10-15")
          .addCustomProperty("retrain_frequency", "monthly")
          .addCustomProperty("monitoring_dashboard", "https://grafana.company.com/d/clv-model");

      System.out.println("Added 15 custom properties");

      // Count accumulated patches
      System.out.println(
          "\nAccumulated " + model.getPendingPatches().size() + " patches and cached aspects");

      // Upsert to DataHub - all metadata in single operation
      client.entities().upsert(model);

      System.out.println("\nSuccessfully created comprehensive ML model in DataHub!");
      System.out.println("\nSummary:");
      System.out.println("  URN: " + model.getUrn());
      System.out.println("  Platform: tensorflow");
      System.out.println("  Training Metrics: 12");
      System.out.println("  Hyperparameters: 12");
      System.out.println("  Model Group: clv_models");
      System.out.println("  Training Jobs: 2");
      System.out.println("  Downstream Jobs: 2");
      System.out.println("  Deployments: 3");
      System.out.println("  Tags: 7");
      System.out.println("  Owners: 3");
      System.out.println("  Glossary Terms: 3");
      System.out.println("  Domain: MachineLearning");
      System.out.println("  Custom Properties: 15");
      System.out.println(
          "\n  View in DataHub: " + client.getConfig().getServer() + "/mlModel/" + model.getUrn());

    } finally {
      client.close();
    }
  }
}
