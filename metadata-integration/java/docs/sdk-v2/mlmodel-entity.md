# MLModel Entity

## Overview

The `MLModel` entity represents a machine learning model in DataHub. ML models are trained on data and deployed to production environments, with comprehensive metadata including training metrics, hyperparameters, model groups, training jobs, downstream jobs, and deployments.

## URN Structure

MLModel URNs follow this pattern:

```
urn:li:mlModel:(urn:li:dataPlatform:{platform},{model_name},{environment})
```

**Components:**

- `platform`: The ML platform (e.g., tensorflow, pytorch, sklearn, sagemaker)
- `model_name`: Unique identifier for the model
- `environment`: Fabric type (PROD, DEV, STAGING, TEST, etc.)

**Examples:**

```
urn:li:mlModel:(urn:li:dataPlatform:tensorflow,user_churn_predictor,PROD)
urn:li:mlModel:(urn:li:dataPlatform:pytorch,recommendation_model_v2,STAGING)
urn:li:mlModel:(urn:li:dataPlatform:sklearn,fraud_detector,PROD)
```

## ML-Specific Concepts

### Training Metrics

Metrics collected during model training that measure performance:

- Classification: accuracy, precision, recall, f1_score, auc_roc, auc_pr
- Regression: mse, mae, rmse, r2_score
- Loss metrics: log_loss, cross_entropy
- Custom metrics: training_time, validation_accuracy

### Hyperparameters

Configuration parameters used during model training:

- Learning configuration: learning_rate, batch_size, epochs
- Architecture: hidden_layers, hidden_units, dropout_rate
- Optimization: optimizer, learning_rate_decay, momentum
- Regularization: l1_regularization, l2_regularization

### Model Groups

Collections of related models (e.g., different versions of the same model family). A model can belong to one group, enabling version tracking and A/B testing scenarios.

### Training Jobs

Data processing jobs or pipelines that produced this model. Creates lineage from training data to model.

### Downstream Jobs

Jobs that consume or use this model for inference, scoring, or predictions. Creates lineage from model to downstream applications.

### Deployments

Production environments where the model is deployed (e.g., SageMaker endpoints, Kubernetes services, REST APIs).

## Creating an ML Model

### Basic Example

```java
MLModel model = MLModel.builder()
    .platform("tensorflow")
    .name("user_churn_predictor")
    .env("PROD")
    .displayName("User Churn Prediction Model")
    .description("XGBoost model predicting user churn probability")
    .build();

// Add training metrics
model.addTrainingMetric("accuracy", "0.94")
     .addTrainingMetric("f1_score", "0.92")
     .addTrainingMetric("auc_roc", "0.96");

// Add hyperparameters
model.addHyperParam("learning_rate", "0.01")
     .addHyperParam("max_depth", "6")
     .addHyperParam("n_estimators", "100");

// Add standard metadata
model.addTag("production")
     .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
     .setDomain("urn:li:domain:MachineLearning");

// Save to DataHub
client.entities().upsert(model);
```

### Builder Options

```java
MLModel model = MLModel.builder()
    .platform("pytorch")              // Required: ML platform
    .name("recommendation_model")     // Required: Model identifier
    .env("PROD")                      // Optional: Default is PROD
    .displayName("Product Recommender") // Optional: Human-readable name
    .description("Collaborative filtering model") // Optional
    .externalUrl("https://mlflow.company.com/models/123") // Optional
    .build();
```

## ML-Specific Operations

### Training Metrics

```java
// Add individual metrics
model.addTrainingMetric("accuracy", "0.947")
     .addTrainingMetric("precision", "0.934")
     .addTrainingMetric("recall", "0.921");

// Set all metrics at once
MLMetric metric1 = new MLMetric();
metric1.setName("f1_score");
metric1.setValue("0.927");

MLMetric metric2 = new MLMetric();
metric2.setName("auc_roc");
metric2.setValue("0.965");

model.setTrainingMetrics(List.of(metric1, metric2));

// Get metrics
List<MLMetric> metrics = model.getTrainingMetrics();
```

### Hyperparameters

```java
// Add individual hyperparameters
model.addHyperParam("learning_rate", "0.001")
     .addHyperParam("batch_size", "64")
     .addHyperParam("epochs", "100");

// Set all hyperparameters at once
MLHyperParam param1 = new MLHyperParam();
param1.setName("dropout_rate");
param1.setValue("0.3");

MLHyperParam param2 = new MLHyperParam();
param2.setName("optimizer");
param2.setValue("adam");

model.setHyperParams(List.of(param1, param2));

// Get hyperparameters
List<MLHyperParam> params = model.getHyperParams();
```

### Model Groups

```java
// Set model group (creates relationship)
model.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,churn_models,PROD)");

// Get model group
String group = model.getModelGroup();
```

### Training Jobs (Lineage)

```java
// Add training jobs
model.addTrainingJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,ml_training_dag,prod),train_model)")
     .addTrainingJob("urn:li:dataProcessInstance:(urn:li:dataFlow:(airflow,ml_training_dag,prod),2025-10-15T08:00:00Z)");

// Remove training job
model.removeTrainingJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,ml_training_dag,prod),train_model)");

// Get training jobs
List<String> jobs = model.getTrainingJobs();
```

### Downstream Jobs (Lineage)

```java
// Add downstream jobs
model.addDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,scoring_dag,prod),score_customers)")
     .addDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,inference_dag,prod),predict)");

// Remove downstream job
model.removeDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,scoring_dag,prod),score_customers)");

// Get downstream jobs
List<String> jobs = model.getDownstreamJobs();
```

### Deployments

```java
// Add deployments
model.addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,model-staging)")
     .addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,model-production)");

// Remove deployment
model.removeDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,model-staging)");

// Get deployments
List<String> deployments = model.getDeployments();
```

## Standard Property Operations

### Display Name and Description

```java
// Set display name
model.setDisplayName("Customer Lifetime Value Model");

// Set description
model.setDescription("Deep learning model predicting CLV based on purchase history");

// Set external URL
model.setExternalUrl("https://mlflow.company.com/experiments/42/runs/abc123");

// Get properties
String name = model.getDisplayName();
String desc = model.getDescription();
String url = model.getExternalUrl();
```

### Custom Properties

```java
// Add individual properties
model.addCustomProperty("framework", "TensorFlow 2.14")
     .addCustomProperty("model_version", "2.1.0")
     .addCustomProperty("training_date", "2025-10-15");

// Set all properties at once
Map<String, String> props = new HashMap<>();
props.put("deployment_date", "2025-10-20");
props.put("inference_latency_ms", "15");
model.setCustomProperties(props);

// Get properties
Map<String, String> customProps = model.getCustomProperties();
```

## Standard Metadata Operations

### Tags

```java
// Add tags (with or without urn:li:tag: prefix)
model.addTag("production")
     .addTag("urn:li:tag:ml-model")
     .addTag("deep-learning");

// Remove tag
model.removeTag("production");
```

### Owners

```java
// Add owners with different types
model.addOwner("urn:li:corpuser:ml_platform_team", OwnershipType.TECHNICAL_OWNER)
     .addOwner("urn:li:corpuser:data_science_team", OwnershipType.DATA_STEWARD);

// Remove owner
model.removeOwner("urn:li:corpuser:ml_platform_team");
```

### Glossary Terms

```java
// Add glossary terms
model.addTerm("urn:li:glossaryTerm:MachineLearning.Model")
     .addTerm("urn:li:glossaryTerm:CustomerAnalytics.Prediction");

// Remove term
model.removeTerm("urn:li:glossaryTerm:MachineLearning.Model");
```

### Domain

```java
// Set domain
model.setDomain("urn:li:domain:MachineLearning");

// Remove specific domain
model.removeDomain("urn:li:domain:MachineLearning");

// Or clear all domains
model.clearDomains();
```

## Common Patterns

### Complete ML Model Workflow

```java
// 1. Create model with basic metadata
MLModel model = MLModel.builder()
    .platform("tensorflow")
    .name("customer_ltv_predictor")
    .env("PROD")
    .displayName("Customer Lifetime Value Prediction Model")
    .description("Deep learning model predicting customer lifetime value")
    .externalUrl("https://mlflow.company.com/experiments/42")
    .build();

// 2. Add comprehensive training metrics
model.addTrainingMetric("accuracy", "0.947")
     .addTrainingMetric("precision", "0.934")
     .addTrainingMetric("recall", "0.921")
     .addTrainingMetric("f1_score", "0.927")
     .addTrainingMetric("auc_roc", "0.965")
     .addTrainingMetric("training_time_minutes", "142.5");

// 3. Add comprehensive hyperparameters
model.addHyperParam("learning_rate", "0.001")
     .addHyperParam("batch_size", "64")
     .addHyperParam("epochs", "100")
     .addHyperParam("optimizer", "adam")
     .addHyperParam("dropout_rate", "0.3")
     .addHyperParam("hidden_layers", "3");

// 4. Set model group for version tracking
model.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,ltv_models,PROD)");

// 5. Add training lineage
model.addTrainingJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,ml_training_dag,prod),train_ltv)")
     .addTrainingJob("urn:li:dataProcessInstance:(urn:li:dataFlow:(airflow,ml_training_dag,prod),2025-10-15T08:00:00Z)");

// 6. Add downstream lineage
model.addDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_scoring,prod),score)")
     .addDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,campaign_targeting,prod),target)");

// 7. Add deployment information
model.addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,ltv-staging)")
     .addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,ltv-production)");

// 8. Add organizational metadata
model.addTag("production")
     .addTag("deep-learning")
     .addTag("business-critical")
     .addOwner("urn:li:corpuser:ml_platform", OwnershipType.TECHNICAL_OWNER)
     .addOwner("urn:li:corpuser:data_science", OwnershipType.DATA_STEWARD)
     .addTerm("urn:li:glossaryTerm:MachineLearning.Model")
     .setDomain("urn:li:domain:MachineLearning");

// 9. Add custom properties
model.addCustomProperty("framework", "TensorFlow 2.14")
     .addCustomProperty("model_version", "2.1.0")
     .addCustomProperty("training_date", "2025-10-15")
     .addCustomProperty("deployment_date", "2025-10-20")
     .addCustomProperty("inference_latency_ms", "15");

// 10. Save to DataHub
client.entities().upsert(model);
```

### Model Training to Deployment Flow

```java
// Step 1: Create model after training
MLModel model = MLModel.builder()
    .platform("pytorch")
    .name("fraud_detector_v2")
    .env("DEV")
    .build();

// Step 2: Add training results
model.addTrainingMetric("accuracy", "0.97")
     .addTrainingMetric("precision", "0.95")
     .addHyperParam("learning_rate", "0.001")
     .addHyperParam("batch_size", "128")
     .setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:pytorch,fraud_models,DEV)");

client.entities().upsert(model);

// Step 3: Promote to staging
MLModel stagingModel = MLModel.builder()
    .platform("pytorch")
    .name("fraud_detector_v2")
    .env("STAGING")
    .build();

stagingModel.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:pytorch,fraud_models,STAGING)")
            .addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,fraud-staging)");

client.entities().upsert(stagingModel);

// Step 4: Deploy to production
MLModel prodModel = MLModel.builder()
    .platform("pytorch")
    .name("fraud_detector_v2")
    .env("PROD")
    .build();

prodModel.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:pytorch,fraud_models,PROD)")
         .addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,fraud-production)")
         .addTag("production")
         .addOwner("urn:li:corpuser:fraud_ml_team", OwnershipType.TECHNICAL_OWNER)
         .setDomain("urn:li:domain:FraudPrevention");

client.entities().upsert(prodModel);
```

### A/B Testing Scenario

```java
// Model A (current champion)
MLModel modelA = MLModel.builder()
    .platform("tensorflow")
    .name("recommendation_model_a")
    .env("PROD")
    .displayName("Recommendation Model A (Champion)")
    .build();

modelA.addTrainingMetric("accuracy", "0.92")
      .setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,recommendation_models,PROD)")
      .addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,recommend-prod-80pct)")
      .addCustomProperty("traffic_percentage", "80");

// Model B (challenger)
MLModel modelB = MLModel.builder()
    .platform("tensorflow")
    .name("recommendation_model_b")
    .env("PROD")
    .displayName("Recommendation Model B (Challenger)")
    .build();

modelB.addTrainingMetric("accuracy", "0.94")
      .setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,recommendation_models,PROD)")
      .addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,recommend-prod-20pct)")
      .addCustomProperty("traffic_percentage", "20")
      .addCustomProperty("experiment_id", "ab_test_2025_10");

client.entities().upsert(modelA);
client.entities().upsert(modelB);
```

## Best Practices

1. **Use descriptive names**: Model names should clearly indicate purpose (e.g., `user_churn_predictor_v2`, `fraud_detection_xgboost`)

2. **Track comprehensive metrics**: Include both training and validation metrics for transparency

3. **Document hyperparameters**: Record all hyperparameters used for reproducibility

4. **Maintain lineage**: Always link training jobs and downstream consumers

5. **Use model groups**: Group related models together for easier versioning

6. **Tag appropriately**: Use tags like `production`, `experimental`, `deprecated`

7. **Set ownership**: Assign technical owners (ML engineers) and data stewards

8. **Add deployment info**: Track where models are deployed for operational monitoring

9. **Use custom properties**: Store framework versions, training dates, performance benchmarks

10. **Link to external systems**: Use `externalUrl` to link to MLflow, SageMaker, or other ML platforms

## See Also

- [Dataset Entity](dataset-entity.md) - For training data lineage
- [DataJob Entity](datajob-entity.md) - For training job metadata
- [SDK V2 Overview](README.md) - General SDK concepts
