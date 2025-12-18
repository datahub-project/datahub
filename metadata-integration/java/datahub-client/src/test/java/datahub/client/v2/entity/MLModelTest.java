package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.ml.metadata.MLHyperParam;
import com.linkedin.ml.metadata.MLMetric;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Tests for MLModel entity builder and operations. */
public class MLModelTest {

  @Test
  public void testMLModelBuilderMinimal() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    assertNotNull(model);
    assertNotNull(model.getUrn());
    assertNotNull(model.getMLModelUrn());
    assertEquals(model.getEntityType(), "mlModel");
    assertTrue(model.isNewEntity());
    assertTrue(model.getUrn().toString().contains("tensorflow"));
    assertTrue(model.getUrn().toString().contains("my_model"));
  }

  @Test
  public void testMLModelBuilderWithOptions() {
    MLModel model =
        MLModel.builder()
            .platform("pytorch")
            .name("recommendation_model")
            .env("DEV")
            .displayName("Product Recommendation Model")
            .description("Collaborative filtering model for product recommendations")
            .build();

    assertNotNull(model);
    assertTrue(model.getUrn().toString().contains("pytorch"));
    assertTrue(model.getUrn().toString().contains("DEV"));
  }

  @Test
  public void testMLModelBuilderWithCustomProperties() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("version", "1.0.0");
    customProps.put("framework", "PyTorch");

    MLModel model =
        MLModel.builder()
            .platform("pytorch")
            .name("my_model")
            .customProperties(customProps)
            .build();

    assertNotNull(model);
    List<MetadataChangeProposalWrapper> mcps = model.toMCPs();
    assertFalse(mcps.isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMLModelBuilderMissingPlatform() {
    MLModel.builder().name("my_model").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMLModelBuilderMissingName() {
    MLModel.builder().platform("tensorflow").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMLModelBuilderInvalidEnvironment() {
    MLModel.builder().platform("tensorflow").name("my_model").env("INVALID_ENV").build();
  }

  @Test
  public void testAddTrainingMetric() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addTrainingMetric("accuracy", "0.95");

    // Verify mutation is tracked via patches (reads after mutations throw
    // PendingMutationsException)
    // MLModel uses patch-based mutations
    assertTrue("Should have pending patches", model.hasPendingPatches());
  }

  @Test
  public void testAddMultipleTrainingMetrics() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model
        .addTrainingMetric("accuracy", "0.95")
        .addTrainingMetric("precision", "0.93")
        .addTrainingMetric("recall", "0.92");

    // Verify mutations are tracked (reads after mutations throw PendingMutationsException)
    // MLModel uses patch-based mutations
    assertTrue("Should have pending patches", model.hasPendingPatches());
  }

  @Test
  public void testSetTrainingMetrics() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    MLMetric metric1 = new MLMetric();
    metric1.setName("f1_score");
    metric1.setValue("0.94");

    MLMetric metric2 = new MLMetric();
    metric2.setName("auc_roc");
    metric2.setValue("0.96");

    List<MLMetric> metrics = List.of(metric1, metric2);
    model.setTrainingMetrics(metrics);

    List<MLMetric> retrieved = model.getTrainingMetrics();
    assertNotNull(retrieved);
    assertEquals(2, retrieved.size());
  }

  @Test
  public void testAddHyperParam() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addHyperParam("learning_rate", "0.01");

    // Verify mutation is tracked via patches (reads after mutations throw
    // PendingMutationsException)
    // MLModel uses patch-based mutations
    assertTrue("Should have pending patches", model.hasPendingPatches());
  }

  @Test
  public void testAddMultipleHyperParams() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model
        .addHyperParam("learning_rate", "0.01")
        .addHyperParam("batch_size", "64")
        .addHyperParam("epochs", "100");

    // Verify mutations are tracked (reads after mutations throw PendingMutationsException)
    // MLModel uses patch-based mutations
    assertTrue("Should have pending patches", model.hasPendingPatches());
  }

  @Test
  public void testSetHyperParams() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    MLHyperParam param1 = new MLHyperParam();
    param1.setName("max_depth");
    param1.setValue("6");

    MLHyperParam param2 = new MLHyperParam();
    param2.setName("n_estimators");
    param2.setValue("100");

    List<MLHyperParam> params = List.of(param1, param2);
    model.setHyperParams(params);

    List<MLHyperParam> retrieved = model.getHyperParams();
    assertNotNull(retrieved);
    assertEquals(2, retrieved.size());
  }

  @Test
  public void testSetModelGroup() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,my_group,PROD)");

    String group = model.getModelGroup();
    assertNotNull(group);
    assertTrue(group.contains("my_group"));
  }

  @Test
  public void testAddTrainingJob() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addTrainingJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,ml_dag,prod),train_model)");

    List<String> jobs = model.getTrainingJobs();
    assertNotNull(jobs);
    assertEquals(1, jobs.size());
    assertTrue(jobs.get(0).contains("train_model"));
  }

  @Test
  public void testAddMultipleTrainingJobs() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model
        .addTrainingJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,ml_dag,prod),train_model)")
        .addTrainingJob(
            "urn:li:dataProcessInstance:(urn:li:dataFlow:(airflow,ml_dag,prod),2025-10-01T00:00:00Z)");

    List<String> jobs = model.getTrainingJobs();
    assertNotNull(jobs);
    assertEquals(2, jobs.size());
  }

  @Test
  public void testRemoveTrainingJob() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    String jobUrn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,ml_dag,prod),train_model)";
    model.addTrainingJob(jobUrn);
    model.removeTrainingJob(jobUrn);

    List<String> jobs = model.getTrainingJobs();
    assertTrue(jobs == null || jobs.isEmpty());
  }

  @Test
  public void testAddDownstreamJob() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,scoring_dag,prod),score)");

    List<String> jobs = model.getDownstreamJobs();
    assertNotNull(jobs);
    assertEquals(1, jobs.size());
    assertTrue(jobs.get(0).contains("score"));
  }

  @Test
  public void testAddMultipleDownstreamJobs() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model
        .addDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,scoring_dag,prod),score)")
        .addDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,inference_dag,prod),predict)");

    List<String> jobs = model.getDownstreamJobs();
    assertNotNull(jobs);
    assertEquals(2, jobs.size());
  }

  @Test
  public void testRemoveDownstreamJob() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    String jobUrn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,scoring_dag,prod),score)";
    model.addDownstreamJob(jobUrn);
    model.removeDownstreamJob(jobUrn);

    List<String> jobs = model.getDownstreamJobs();
    assertTrue(jobs == null || jobs.isEmpty());
  }

  @Test
  public void testAddDeployment() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,my_deployment)");

    List<String> deployments = model.getDeployments();
    assertNotNull(deployments);
    assertEquals(1, deployments.size());
    assertTrue(deployments.get(0).contains("my_deployment"));
  }

  @Test
  public void testAddMultipleDeployments() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model
        .addDeployment(
            "urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,staging_deployment)")
        .addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,prod_deployment)");

    List<String> deployments = model.getDeployments();
    assertNotNull(deployments);
    assertEquals(2, deployments.size());
  }

  @Test
  public void testRemoveDeployment() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    String deploymentUrn = "urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,my_deployment)";
    model.addDeployment(deploymentUrn);
    model.removeDeployment(deploymentUrn);

    List<String> deployments = model.getDeployments();
    assertTrue(deployments == null || deployments.isEmpty());
  }

  @Test
  public void testSetDisplayName() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.setDisplayName("My Awesome Model");

    String name = model.getDisplayName();
    assertEquals("My Awesome Model", name);
  }

  @Test
  public void testSetDescription() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.setDescription("This is a test model");

    // Verify mutation is tracked via patches (reads after mutations throw
    // PendingMutationsException)
    assertTrue("Should have pending patches for description mutation", model.hasPendingPatches());
  }

  @Test
  public void testSetExternalUrl() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.setExternalUrl("https://mlflow.company.com/models/123");

    // Verify mutation is tracked via patches (reads after mutations throw
    // PendingMutationsException)
    // MLModel uses patch-based mutations
    assertTrue("Should have pending patches", model.hasPendingPatches());
  }

  @Test
  public void testAddCustomProperty() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addCustomProperty("version", "1.0.0");

    // Verify mutation is tracked via patches (reads after mutations throw
    // PendingMutationsException)
    // MLModel uses patch-based mutations
    assertTrue("Should have pending patches", model.hasPendingPatches());
  }

  @Test
  public void testSetCustomProperties() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    Map<String, String> props = new HashMap<>();
    props.put("key1", "value1");
    props.put("key2", "value2");
    model.setCustomProperties(props);

    Map<String, String> retrieved = model.getCustomProperties();
    assertNotNull(retrieved);
    assertEquals(2, retrieved.size());
    assertEquals("value1", retrieved.get("key1"));
  }

  @Test
  public void testAddOwner() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);

    List<MetadataChangeProposal> patches = model.getPendingPatches();
    assertFalse(patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testRemoveOwner() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
    model.removeOwner("urn:li:corpuser:johndoe");

    List<MetadataChangeProposal> patches = model.getPendingPatches();
    assertFalse(patches.isEmpty());
  }

  @Test
  public void testAddTag() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addTag("production");
    model.addTag("urn:li:tag:ml-model");

    List<MetadataChangeProposal> patches = model.getPendingPatches();
    assertFalse(patches.isEmpty());
    assertEquals("globalTags", patches.get(0).getAspectName());
  }

  @Test
  public void testRemoveTag() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addTag("test");
    model.removeTag("test");

    List<MetadataChangeProposal> patches = model.getPendingPatches();
    assertFalse(patches.isEmpty());
  }

  @Test
  public void testAddTerm() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addTerm("urn:li:glossaryTerm:MachineLearning.Model");

    List<MetadataChangeProposal> patches = model.getPendingPatches();
    assertFalse(patches.isEmpty());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());
  }

  @Test
  public void testRemoveTerm() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addTerm("urn:li:glossaryTerm:MachineLearning.Model");
    model.removeTerm("urn:li:glossaryTerm:MachineLearning.Model");

    List<MetadataChangeProposal> patches = model.getPendingPatches();
    assertFalse(patches.isEmpty());
  }

  @Test
  public void testSetDomain() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.setDomain("urn:li:domain:MachineLearning");

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = model.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testRemoveDomain() throws Exception {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    Urn mlDomain = Urn.createFromString("urn:li:domain:MachineLearning");
    model.setDomain(mlDomain.toString());
    model.removeDomain(mlDomain);

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = model.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testRemoveDomainWithString() throws Exception {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.setDomain("urn:li:domain:MachineLearning");
    model.removeDomain("urn:li:domain:MachineLearning");

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = model.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testClearDomains() throws Exception {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.setDomain("urn:li:domain:MachineLearning");
    model.clearDomains();

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = model.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testFluentAPI() {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("churn_model")
            .displayName("Churn Prediction Model")
            .description("Predicts user churn")
            .build();

    model
        .addTrainingMetric("accuracy", "0.95")
        .addHyperParam("learning_rate", "0.01")
        .addTag("production")
        .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:ML.Model")
        .setDomain("urn:li:domain:MachineLearning")
        .addCustomProperty("version", "1.0.0");

    // Verify mutations are tracked (reads after mutations throw PendingMutationsException)
    // MLModel uses patch-based mutations
    assertTrue(model.hasPendingPatches());
  }

  @Test
  public void testToMCPs() {
    MLModel model =
        MLModel.builder()
            .platform("tensorflow")
            .name("my_model")
            .displayName("Test Model")
            .description("Test description")
            .build();

    List<MetadataChangeProposalWrapper> mcps = model.toMCPs();

    assertNotNull(mcps);
    assertFalse(mcps.isEmpty());

    for (MetadataChangeProposalWrapper mcp : mcps) {
      assertEquals(mcp.getEntityType(), "mlModel");
      assertNotNull(mcp.getEntityUrn());
      assertNotNull(mcp.getAspect());
    }
  }

  @Test
  public void testClearPendingPatches() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    model.addTag("test");
    model.addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER);

    assertTrue(model.hasPendingPatches());

    model.clearPendingPatches();

    assertFalse(model.hasPendingPatches());
  }

  @Test
  public void testEqualsAndHashCode() {
    MLModel model1 = MLModel.builder().platform("tensorflow").name("my_model").build();

    MLModel model2 = MLModel.builder().platform("tensorflow").name("my_model").build();

    assertEquals(model1, model2);
    assertEquals(model1.hashCode(), model2.hashCode());
  }

  @Test
  public void testToString() {
    MLModel model = MLModel.builder().platform("tensorflow").name("my_model").build();

    String str = model.toString();
    assertNotNull(str);
    assertTrue(str.contains("MLModel"));
    assertTrue(str.contains("urn"));
  }

  @Test
  public void testMLWorkflowScenario() {
    MLModel model = MLModel.builder().platform("tensorflow").name("fraud_detector").build();

    model
        .setDisplayName("Fraud Detection Model")
        .setDescription("Real-time fraud detection using deep learning")
        .addTrainingMetric("accuracy", "0.97")
        .addTrainingMetric("precision", "0.95")
        .addTrainingMetric("recall", "0.93")
        .addHyperParam("learning_rate", "0.001")
        .addHyperParam("batch_size", "128")
        .addHyperParam("epochs", "50")
        .setModelGroup("urn:li:mlModelGroup:(urn:li:dataPlatform:tensorflow,fraud_models,PROD)")
        .addTrainingJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,ml_training,prod),train)")
        .addDownstreamJob("urn:li:dataJob:(urn:li:dataFlow:(airflow,fraud_scoring,prod),score)")
        .addDeployment("urn:li:mlModelDeployment:(urn:li:dataPlatform:sagemaker,prod)")
        .addTag("production")
        .addTag("fraud-detection")
        .addOwner("urn:li:corpuser:fraud_ml_team", OwnershipType.TECHNICAL_OWNER)
        .setDomain("urn:li:domain:FraudPrevention")
        .addCustomProperty("model_version", "3.2.1")
        .addCustomProperty("deployment_date", "2025-10-20");

    // Verify mutations are tracked (reads after mutations throw PendingMutationsException)
    // MLModel uses patch-based mutations
    assertTrue(model.hasPendingPatches());
  }

  @Test
  public void testMultipleEnvironments() {
    MLModel prodModel =
        MLModel.builder().platform("tensorflow").name("my_model").env("PROD").build();

    MLModel devModel = MLModel.builder().platform("tensorflow").name("my_model").env("DEV").build();

    assertNotEquals(prodModel.getUrn(), devModel.getUrn());
    assertTrue(prodModel.getUrn().toString().contains("PROD"));
    assertTrue(devModel.getUrn().toString().contains("DEV"));
  }
}
