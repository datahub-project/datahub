# MLModelGroup Entity

## Overview

The `MLModelGroup` entity represents a collection of versioned ML models in DataHub. ML Model Groups provide a way to organize and track related models that belong to the same model family, enabling version management, A/B testing scenarios, and comprehensive lineage tracking across model training and deployment pipelines.

## URN Structure

MLModelGroup URNs follow this pattern:

```
urn:li:mlModelGroup:(urn:li:dataPlatform:{platform},{group_name},{environment})
```

**Components:**

- `platform`: The ML platform (e.g., mlflow, sagemaker, vertexai, tensorflow)
- `group_name`: Unique identifier for the model group
- `environment`: Fabric type (PROD, DEV, STAGING, TEST, etc.)

**Examples:**

```
urn:li:mlModelGroup:(urn:li:dataPlatform:mlflow,recommendation_models,PROD)
urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,fraud_detection_family,PROD)
urn:li:mlModelGroup:(urn:li:dataPlatform:vertexai,churn_prediction_models,STAGING)
```

## ML Model Group Concepts

### Model Families

A model group represents a family of related models, typically different versions of the same model:

- **Version Evolution**: Track how a model evolves over time (v1.0, v1.1, v2.0)
- **A/B Testing**: Group champion and challenger models for comparison
- **Multi-Environment**: Same model family across DEV, STAGING, PROD environments
- **Framework Variations**: Different implementations of the same business logic

### Relationship with MLModel

- **One-to-Many**: One model group contains many models
- **Versioning**: Individual models represent specific versions within the group
- **Shared Metadata**: Common properties like purpose, business context, and team ownership
- **Lineage Aggregation**: Training and downstream jobs can be tracked at the group level

### Training Jobs

Data processing jobs or pipelines that train models in this group. Training jobs create lineage from training data to model groups, showing the data sources used to build all versions of the model.

### Downstream Jobs

Jobs that consume or use models from this group for inference, scoring, or predictions. Downstream jobs create lineage from model groups to applications, showing where these models are being used.

## Creating an ML Model Group

### Basic Example

```java
MLModelGroup modelGroup = MLModelGroup.builder()
    .platform("mlflow")
    .groupId("recommendation_models")
    .env("PROD")
    .name("Product Recommendation Model Family")
    .description("Collection of product recommendation models trained on user behavior data")
    .build();

// Add standard metadata
modelGroup.addTag("recommendation")
          .addTag("production")
          .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
          .setDomain("urn:li:domain:MachineLearning");

// Add training job references
modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_pipeline_2025_01");

// Save to DataHub
client.entities().upsert(modelGroup);
```

### Builder Options

```java
MLModelGroup modelGroup = MLModelGroup.builder()
    .platform("sagemaker")                  // Required: ML platform
    .groupId("churn_prediction_models")     // Required: Model group identifier
    .env("PROD")                            // Optional: Default is PROD
    .name("Customer Churn Prediction Models") // Optional: Human-readable name
    .description("Models predicting customer churn") // Optional
    .externalUrl("https://mlflow.company.com/groups/123") // Optional
    .build();
```

## ML Model Group Operations

### Display Name and Description

```java
// Name is typically set in builder and read-only after creation
String name = modelGroup.getName();

// Description can be updated using patch operations
modelGroup.setDescription("Updated description for customer churn model family");

// Get properties
String description = modelGroup.getDescription();
```

### External URL

```java
// External URL is typically set in builder
MLModelGroup modelGroup = MLModelGroup.builder()
    .platform("mlflow")
    .groupId("fraud_models")
    .externalUrl("https://mlflow.company.com/experiments/fraud-detection")
    .build();

// Get external URL
String url = modelGroup.getExternalUrl();
```

### Custom Properties

```java
// Custom properties are set in builder
Map<String, String> customProps = new HashMap<>();
customProps.put("model_family", "transformer");
customProps.put("framework", "pytorch");
customProps.put("use_case", "fraud_detection");
customProps.put("model_type", "classification");

MLModelGroup modelGroup = MLModelGroup.builder()
    .platform("vertexai")
    .groupId("fraud_models")
    .customProperties(customProps)
    .build();

// Get custom properties
Map<String, String> props = modelGroup.getCustomProperties();
```

### Timestamps

```java
import com.linkedin.common.TimeStamp;

// Set creation and modification timestamps
TimeStamp created = new TimeStamp().setTime(System.currentTimeMillis());
TimeStamp lastModified = new TimeStamp().setTime(System.currentTimeMillis());

MLModelGroup modelGroup = MLModelGroup.builder()
    .platform("mlflow")
    .groupId("recommendation_models")
    .created(created)
    .lastModified(lastModified)
    .build();

// Get timestamps
TimeStamp createdTime = modelGroup.getCreated();
TimeStamp modifiedTime = modelGroup.getLastModified();
```

### Training Jobs (Lineage)

```java
// Add training jobs one at a time
modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_pipeline_v1_2025_01")
          .addTrainingJob("urn:li:dataProcessInstance:training_pipeline_v2_2025_03");

// Set all training jobs at once (replaces existing)
List<String> trainingJobs = Arrays.asList(
    "urn:li:dataProcessInstance:training_pipeline_v1_2025_01",
    "urn:li:dataProcessInstance:training_pipeline_v2_2025_03",
    "urn:li:dataProcessInstance:training_pipeline_v3_2025_06"
);
modelGroup.setTrainingJobs(trainingJobs);

// Remove a training job
modelGroup.removeTrainingJob("urn:li:dataProcessInstance:training_pipeline_v1_2025_01");

// Get training jobs
List<String> jobs = modelGroup.getTrainingJobs();
```

### Downstream Jobs (Lineage)

```java
// Add downstream jobs one at a time
modelGroup.addDownstreamJob("urn:li:dataProcessInstance:prediction_service")
          .addDownstreamJob("urn:li:dataProcessInstance:batch_scoring_job");

// Set all downstream jobs at once (replaces existing)
List<String> downstreamJobs = Arrays.asList(
    "urn:li:dataProcessInstance:prediction_service",
    "urn:li:dataProcessInstance:batch_scoring_job",
    "urn:li:dataProcessInstance:real_time_inference_api"
);
modelGroup.setDownstreamJobs(downstreamJobs);

// Remove a downstream job
modelGroup.removeDownstreamJob("urn:li:dataProcessInstance:prediction_service");

// Get downstream jobs
List<String> jobs = modelGroup.getDownstreamJobs();
```

## Standard Metadata Operations

### Tags

```java
// Add tags (with or without urn:li:tag: prefix)
modelGroup.addTag("production")
          .addTag("urn:li:tag:ml-model-group")
          .addTag("high-priority");

// Remove tag
modelGroup.removeTag("production");
```

### Owners

```java
// Add owners with different types
modelGroup.addOwner("urn:li:corpuser:ml_platform_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:data_science_lead", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:product_team", OwnershipType.BUSINESS_OWNER);

// Remove owner
modelGroup.removeOwner("urn:li:corpuser:ml_platform_team");
```

### Glossary Terms

```java
// Add glossary terms
modelGroup.addTerm("urn:li:glossaryTerm:MachineLearning.ModelGroup")
          .addTerm("urn:li:glossaryTerm:CustomerAnalytics.Prediction");

// Remove term
modelGroup.removeTerm("urn:li:glossaryTerm:MachineLearning.ModelGroup");
```

### Domain

```java
// Set domain
modelGroup.setDomain("urn:li:domain:MachineLearning");

// Remove domain
modelGroup.removeDomain();
```

## Common Patterns

### Complete Model Group Workflow

```java
// 1. Create model group with comprehensive metadata
Map<String, String> customProperties = new HashMap<>();
customProperties.put("model_family", "transformer");
customProperties.put("framework", "pytorch");
customProperties.put("use_case", "customer_churn_prediction");
customProperties.put("model_type", "classification");
customProperties.put("deployment_status", "production");

MLModelGroup modelGroup = MLModelGroup.builder()
    .platform("sagemaker")
    .groupId("customer_churn_predictor")
    .env("PROD")
    .name("Customer Churn Prediction Model Family")
    .description(
        "Collection of customer churn prediction models trained on historical customer " +
        "behavior, subscription data, and engagement metrics. Models are retrained " +
        "monthly and deployed across all customer touchpoints.")
    .externalUrl("https://ml-platform.example.com/models/churn-predictor")
    .customProperties(customProperties)
    .build();

// 2. Add organizational metadata
modelGroup.addTag("churn-prediction")
          .addTag("classification")
          .addTag("production")
          .addTag("customer-analytics")
          .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
          .addOwner("urn:li:corpuser:product_team", OwnershipType.BUSINESS_OWNER)
          .addTerm("urn:li:glossaryTerm:MachineLearning.Classification")
          .addTerm("urn:li:glossaryTerm:CustomerAnalytics.ChurnPrediction")
          .setDomain("urn:li:domain:MachineLearning");

// 3. Add training job lineage
modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_pipeline_v1_2025_01")
          .addTrainingJob("urn:li:dataProcessInstance:training_pipeline_v2_2025_03")
          .addTrainingJob("urn:li:dataProcessInstance:training_pipeline_v3_2025_06");

// 4. Add downstream job lineage
modelGroup.addDownstreamJob("urn:li:dataProcessInstance:prediction_service_deployment")
          .addDownstreamJob("urn:li:dataProcessInstance:batch_scoring_job")
          .addDownstreamJob("urn:li:dataProcessInstance:real_time_inference_api");

// 5. Save to DataHub
client.entities().upsert(modelGroup);
```

### Versioned Model Family Pattern

```java
// Step 1: Create the model group
MLModelGroup modelGroup = MLModelGroup.builder()
    .platform("tensorflow")
    .groupId("fraud_detection_models")
    .env("PROD")
    .name("Fraud Detection Model Family")
    .description("Collection of fraud detection models across versions and environments")
    .build();

modelGroup.addTag("fraud-detection")
          .addOwner("urn:li:corpuser:fraud_ml_team", OwnershipType.TECHNICAL_OWNER)
          .setDomain("urn:li:domain:FraudPrevention");

client.entities().upsert(modelGroup);

// Step 2: Create individual model versions in the group
MLModel modelV1 = MLModel.builder()
    .platform("tensorflow")
    .name("fraud_detector_v1_0")
    .env("PROD")
    .displayName("Fraud Detector v1.0")
    .build();

modelV1.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,fraud_detection_models,PROD)")
       .addTrainingMetric("accuracy", "0.92")
       .addCustomProperty("version", "1.0")
       .addCustomProperty("release_date", "2025-01-15");

client.entities().upsert(modelV1);

MLModel modelV2 = MLModel.builder()
    .platform("tensorflow")
    .name("fraud_detector_v2_0")
    .env("PROD")
    .displayName("Fraud Detector v2.0")
    .build();

modelV2.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,fraud_detection_models,PROD)")
       .addTrainingMetric("accuracy", "0.95")
       .addCustomProperty("version", "2.0")
       .addCustomProperty("release_date", "2025-06-15")
       .addCustomProperty("improvements", "New feature engineering and ensemble methods");

client.entities().upsert(modelV2);
```

### Multi-Environment Model Group Pattern

```java
// Create model groups for each environment
MLModelGroup devGroup = MLModelGroup.builder()
    .platform("mlflow")
    .groupId("recommendation_models")
    .env("DEV")
    .name("Recommendation Models - Development")
    .description("Development versions of recommendation models")
    .build();

devGroup.addTag("development")
        .addOwner("urn:li:corpuser:ml_dev_team", OwnershipType.TECHNICAL_OWNER);

client.entities().upsert(devGroup);

MLModelGroup stagingGroup = MLModelGroup.builder()
    .platform("mlflow")
    .groupId("recommendation_models")
    .env("STAGING")
    .name("Recommendation Models - Staging")
    .description("Staging versions of recommendation models for testing")
    .build();

stagingGroup.addTag("staging")
            .addOwner("urn:li:corpuser:ml_qa_team", OwnershipType.TECHNICAL_OWNER);

client.entities().upsert(stagingGroup);

MLModelGroup prodGroup = MLModelGroup.builder()
    .platform("mlflow")
    .groupId("recommendation_models")
    .env("PROD")
    .name("Recommendation Models - Production")
    .description("Production recommendation models serving live traffic")
    .build();

prodGroup.addTag("production")
         .addTag("business-critical")
         .addOwner("urn:li:corpuser:ml_prod_team", OwnershipType.TECHNICAL_OWNER)
         .addOwner("urn:li:corpuser:product_team", OwnershipType.BUSINESS_OWNER)
         .setDomain("urn:li:domain:ProductRecommendations");

client.entities().upsert(prodGroup);
```

### A/B Testing Model Group Pattern

```java
// Create a model group for A/B testing
MLModelGroup abTestGroup = MLModelGroup.builder()
    .platform("sagemaker")
    .groupId("product_ranking_ab_test")
    .env("PROD")
    .name("Product Ranking A/B Test Models")
    .description("Model group for A/B testing different product ranking algorithms")
    .build();

Map<String, String> abTestProps = new HashMap<>();
abTestProps.put("test_type", "ab_test");
abTestProps.put("test_id", "ranking_experiment_2025_10");
abTestProps.put("start_date", "2025-10-01");
abTestProps.put("expected_end_date", "2025-11-01");

MLModelGroup groupWithProps = MLModelGroup.builder()
    .platform("sagemaker")
    .groupId("product_ranking_ab_test")
    .env("PROD")
    .customProperties(abTestProps)
    .build();

abTestGroup.addTag("ab-test")
           .addTag("experiment")
           .addOwner("urn:li:corpuser:growth_team", OwnershipType.BUSINESS_OWNER)
           .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER);

client.entities().upsert(abTestGroup);

// Champion model (80% traffic)
MLModel championModel = MLModel.builder()
    .platform("sagemaker")
    .name("ranking_champion_collaborative_filter")
    .env("PROD")
    .displayName("Champion: Collaborative Filtering")
    .build();

championModel.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,product_ranking_ab_test,PROD)")
             .addCustomProperty("traffic_percentage", "80")
             .addCustomProperty("model_role", "champion")
             .addTrainingMetric("ndcg", "0.82");

client.entities().upsert(championModel);

// Challenger model (20% traffic)
MLModel challengerModel = MLModel.builder()
    .platform("sagemaker")
    .name("ranking_challenger_neural_network")
    .env("PROD")
    .displayName("Challenger: Neural Network")
    .build();

challengerModel.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,product_ranking_ab_test,PROD)")
               .addCustomProperty("traffic_percentage", "20")
               .addCustomProperty("model_role", "challenger")
               .addTrainingMetric("ndcg", "0.85");

client.entities().upsert(challengerModel);
```

### Complete Lineage Pattern

```java
// Create a model group with full lineage tracking
MLModelGroup modelGroup = MLModelGroup.builder()
    .platform("vertexai")
    .groupId("customer_ltv_models")
    .env("PROD")
    .name("Customer Lifetime Value Models")
    .description("Models predicting customer lifetime value for marketing campaigns")
    .build();

// Add training job lineage (showing data sources)
modelGroup.addTrainingJob("urn:li:dataProcessInstance:customer_data_etl_2025_01")
          .addTrainingJob("urn:li:dataProcessInstance:feature_engineering_pipeline_2025_01")
          .addTrainingJob("urn:li:dataProcessInstance:model_training_pipeline_2025_01");

// Add downstream job lineage (showing consumers)
modelGroup.addDownstreamJob("urn:li:dataProcessInstance:ltv_scoring_batch_job")
          .addDownstreamJob("urn:li:dataProcessInstance:marketing_campaign_targeting")
          .addDownstreamJob("urn:li:dataProcessInstance:customer_segmentation_pipeline")
          .addDownstreamJob("urn:li:dataProcessInstance:retention_prediction_service");

modelGroup.addTag("ltv-prediction")
          .addTag("marketing-analytics")
          .addOwner("urn:li:corpuser:marketing_analytics_team", OwnershipType.BUSINESS_OWNER)
          .addOwner("urn:li:corpuser:ml_platform_team", OwnershipType.TECHNICAL_OWNER)
          .setDomain("urn:li:domain:Marketing");

client.entities().upsert(modelGroup);
```

## Best Practices

1. **Use descriptive group IDs**: Group IDs should clearly indicate the model family purpose (e.g., `customer_churn_models`, `fraud_detection_family`)

2. **Maintain consistent naming**: Use a consistent naming scheme across environments (e.g., `recommendation_models` in DEV, STAGING, PROD)

3. **Track lineage at group level**: Add training and downstream jobs to show the complete data flow for the model family

4. **Group related versions**: All versions of the same business model should belong to the same group

5. **Use environments appropriately**: Separate model groups by environment (DEV, STAGING, PROD) to track promotion workflow

6. **Document model evolution**: Use custom properties to track important milestones, improvements, and architectural changes

7. **Tag for organization**: Use tags like `production`, `experimental`, `deprecated`, `ab-test` to categorize model groups

8. **Set ownership clearly**: Assign technical owners (ML engineers) and business owners (product managers)

9. **Link to external systems**: Use `externalUrl` to link to MLflow, SageMaker, or other ML platform dashboards

10. **Leverage custom properties**: Store metadata like `framework`, `model_family`, `use_case`, `deployment_status`

## Model Group vs Individual Model

### When to use Model Groups

- **Version management**: Track multiple versions of the same model
- **A/B testing**: Group champion and challenger models together
- **Environment promotion**: Track the same model family across DEV, STAGING, PROD
- **Aggregate lineage**: Show training and downstream jobs at the family level
- **Team organization**: Group all models maintained by a specific team

### When to use Individual Models

- **Specific model versions**: Represent a particular trained model with specific metrics and hyperparameters
- **Deployment tracking**: Track where a specific model version is deployed
- **Performance metrics**: Store training and validation metrics for a particular model
- **Detailed metadata**: Capture hyperparameters, training configuration, and model artifacts

### Relationship Best Practices

```java
// 1. Create the model group first
MLModelGroup group = MLModelGroup.builder()
    .platform("tensorflow")
    .groupId("churn_models")
    .env("PROD")
    .name("Customer Churn Model Family")
    .build();
client.entities().upsert(group);

// 2. Create individual models that reference the group
MLModel modelV1 = MLModel.builder()
    .platform("tensorflow")
    .name("churn_predictor_v1")
    .env("PROD")
    .build();

// Link model to group
modelV1.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,churn_models,PROD)");
client.entities().upsert(modelV1);

// 3. Repeat for other versions
MLModel modelV2 = MLModel.builder()
    .platform("tensorflow")
    .name("churn_predictor_v2")
    .env("PROD")
    .build();

modelV2.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,churn_models,PROD)");
client.entities().upsert(modelV2);
```

## See Also

- [MLModel Entity](mlmodel-entity.md) - For individual model metadata and metrics
- [Dataset Entity](dataset-entity.md) - For training data lineage
- [DataJob Entity](datajob-entity.md) - For training and inference job metadata
- [SDK V2 Overview](README.md) - General SDK concepts
