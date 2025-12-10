package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Tests for MLModelGroup entity builder and patch-based operations. */
public class MLModelGroupTest {

  // ==================== Builder Tests ====================

  @Test
  public void testMLModelGroupBuilderMinimal() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("recommender-model").build();

    assertNotNull(modelGroup);
    assertNotNull(modelGroup.getUrn());
    assertNotNull(modelGroup.getMlModelGroupUrn());
    assertEquals(modelGroup.getEntityType(), "mlModelGroup");
    assertTrue(modelGroup.isNewEntity());
  }

  @Test
  public void testMLModelGroupBuilderWithName() {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("sagemaker")
            .groupId("churn-predictor")
            .name("Customer Churn Prediction Model Family")
            .build();

    assertNotNull(modelGroup);
    List<MetadataChangeProposalWrapper> mcps = modelGroup.toMCPs();
    assertFalse(mcps.isEmpty());

    boolean hasProperties =
        mcps.stream()
            .anyMatch(
                mcp -> mcp.getAspect().getClass().getSimpleName().equals("MLModelGroupProperties"));
    assertTrue("Should have MLModelGroupProperties aspect", hasProperties);
  }

  @Test
  public void testMLModelGroupBuilderWithAllOptions() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("model_family", "transformer");
    customProps.put("framework", "pytorch");

    TimeStamp created = new TimeStamp().setTime(System.currentTimeMillis());
    TimeStamp lastModified = new TimeStamp().setTime(System.currentTimeMillis());

    List<String> trainingJobs =
        Arrays.asList(
            "urn:li:dataProcessInstance:training_job_1",
            "urn:li:dataProcessInstance:training_job_2");

    List<String> downstreamJobs =
        Arrays.asList(
            "urn:li:dataProcessInstance:inference_job_1",
            "urn:li:dataProcessInstance:inference_job_2");

    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("vertexai")
            .groupId("recommendation-models")
            .env("DEV")
            .name("Recommendation Model Group")
            .description("Models for product recommendations")
            .externalUrl("https://example.com/models")
            .customProperties(customProps)
            .created(created)
            .lastModified(lastModified)
            .trainingJobs(trainingJobs)
            .downstreamJobs(downstreamJobs)
            .build();

    assertNotNull(modelGroup);
    assertTrue(modelGroup.getUrn().toString().contains("vertexai"));
    assertTrue(modelGroup.getUrn().toString().contains("DEV"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMLModelGroupBuilderMissingPlatform() {
    MLModelGroup.builder().groupId("model-group").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMLModelGroupBuilderMissingGroupId() {
    MLModelGroup.builder().platform("mlflow").build();
  }

  // ==================== Ownership Tests ====================

  @Test
  public void testMLModelGroupAddOwner() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER);

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testMLModelGroupAddMultipleOwners() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER);
    modelGroup.addOwner("urn:li:corpuser:data_science", OwnershipType.TECHNICAL_OWNER);

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testMLModelGroupRemoveOwner() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER);
    modelGroup.removeOwner("urn:li:corpuser:ml_team");

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Tag Tests ====================

  @Test
  public void testMLModelGroupAddTag() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addTag("recommendation");
    modelGroup.addTag("urn:li:tag:production");

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("globalTags", patches.get(0).getAspectName());
  }

  @Test
  public void testMLModelGroupRemoveTag() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addTag("tag1");
    modelGroup.removeTag("tag1");

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Glossary Term Tests ====================

  @Test
  public void testMLModelGroupAddTerm() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addTerm("urn:li:glossaryTerm:MachineLearning");
    modelGroup.addTerm("urn:li:glossaryTerm:ProductionModel");

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());
  }

  @Test
  public void testMLModelGroupRemoveTerm() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addTerm("urn:li:glossaryTerm:term1");
    modelGroup.removeTerm("urn:li:glossaryTerm:term1");

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Domain Tests ====================

  @Test
  public void testMLModelGroupSetDomain() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.setDomain("urn:li:domain:MachineLearning");

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = modelGroup.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testMLModelGroupRemoveDomain() throws Exception {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    Urn mlDomain = Urn.createFromString("urn:li:domain:MachineLearning");
    modelGroup.setDomain(mlDomain.toString());
    modelGroup.removeDomain(mlDomain);

    // Verify domain aspect was cached (domains use aspect cache, not patches)
    List<MetadataChangeProposalWrapper> mcps = modelGroup.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  // ==================== Description Tests ====================

  @Test
  public void testMLModelGroupSetDescription() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.setDescription("Model group for recommendation models");

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    assertFalse("Should have description patch", patches.isEmpty());
    assertEquals("editableMlModelGroupProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testMLModelGroupGetDescription() {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("mlflow")
            .groupId("model-group")
            .description("Test description")
            .build();

    String description = modelGroup.getDescription();
    assertEquals("Test description", description);
  }

  // ==================== Name Tests ====================

  @Test
  public void testMLModelGroupGetName() {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("mlflow")
            .groupId("model-group")
            .name("Recommendation Models")
            .build();

    String name = modelGroup.getName();
    assertEquals("Recommendation Models", name);
  }

  // ==================== Training Jobs Tests ====================

  @Test
  public void testMLModelGroupSetTrainingJobs() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    List<String> jobs =
        Arrays.asList("urn:li:dataProcessInstance:job1", "urn:li:dataProcessInstance:job2");
    modelGroup.setTrainingJobs(jobs);

    List<String> retrievedJobs = modelGroup.getTrainingJobs();
    assertNotNull(retrievedJobs);
    assertEquals(2, retrievedJobs.size());
    assertTrue(retrievedJobs.contains("urn:li:dataProcessInstance:job1"));
  }

  @Test
  public void testMLModelGroupAddTrainingJob() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_job_1");
    modelGroup.addTrainingJob("urn:li:dataProcessInstance:training_job_2");

    List<String> jobs = modelGroup.getTrainingJobs();
    assertNotNull(jobs);
    assertEquals(2, jobs.size());
  }

  @Test
  public void testMLModelGroupRemoveTrainingJob() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addTrainingJob("urn:li:dataProcessInstance:job1");
    modelGroup.addTrainingJob("urn:li:dataProcessInstance:job2");
    modelGroup.removeTrainingJob("urn:li:dataProcessInstance:job1");

    List<String> jobs = modelGroup.getTrainingJobs();
    assertNotNull(jobs);
    assertEquals(1, jobs.size());
    assertFalse(jobs.contains("urn:li:dataProcessInstance:job1"));
  }

  @Test
  public void testMLModelGroupAddDuplicateTrainingJob() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addTrainingJob("urn:li:dataProcessInstance:job1");
    modelGroup.addTrainingJob("urn:li:dataProcessInstance:job1");

    List<String> jobs = modelGroup.getTrainingJobs();
    assertNotNull(jobs);
    assertEquals("Should not add duplicate job", 1, jobs.size());
  }

  @Test
  public void testMLModelGroupTrainingJobsFromBuilder() {
    List<String> jobs =
        Arrays.asList("urn:li:dataProcessInstance:job1", "urn:li:dataProcessInstance:job2");

    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").trainingJobs(jobs).build();

    List<String> retrievedJobs = modelGroup.getTrainingJobs();
    assertNotNull(retrievedJobs);
    assertEquals(2, retrievedJobs.size());
  }

  // ==================== Downstream Jobs Tests ====================

  @Test
  public void testMLModelGroupSetDownstreamJobs() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    List<String> jobs =
        Arrays.asList(
            "urn:li:dataProcessInstance:inference1", "urn:li:dataProcessInstance:inference2");
    modelGroup.setDownstreamJobs(jobs);

    List<String> retrievedJobs = modelGroup.getDownstreamJobs();
    assertNotNull(retrievedJobs);
    assertEquals(2, retrievedJobs.size());
  }

  @Test
  public void testMLModelGroupAddDownstreamJob() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addDownstreamJob("urn:li:dataProcessInstance:inference_job_1");
    modelGroup.addDownstreamJob("urn:li:dataProcessInstance:inference_job_2");

    List<String> jobs = modelGroup.getDownstreamJobs();
    assertNotNull(jobs);
    assertEquals(2, jobs.size());
  }

  @Test
  public void testMLModelGroupRemoveDownstreamJob() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addDownstreamJob("urn:li:dataProcessInstance:job1");
    modelGroup.addDownstreamJob("urn:li:dataProcessInstance:job2");
    modelGroup.removeDownstreamJob("urn:li:dataProcessInstance:job1");

    List<String> jobs = modelGroup.getDownstreamJobs();
    assertNotNull(jobs);
    assertEquals(1, jobs.size());
    assertFalse(jobs.contains("urn:li:dataProcessInstance:job1"));
  }

  @Test
  public void testMLModelGroupDownstreamJobsFromBuilder() {
    List<String> jobs =
        Arrays.asList(
            "urn:li:dataProcessInstance:inference1", "urn:li:dataProcessInstance:inference2");

    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("mlflow")
            .groupId("model-group")
            .downstreamJobs(jobs)
            .build();

    List<String> retrievedJobs = modelGroup.getDownstreamJobs();
    assertNotNull(retrievedJobs);
    assertEquals(2, retrievedJobs.size());
  }

  // ==================== Custom Properties Tests ====================

  @Test
  public void testMLModelGroupCustomPropertiesFromBuilder() {
    Map<String, String> props = new HashMap<>();
    props.put("model_family", "transformer");
    props.put("framework", "pytorch");

    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("mlflow")
            .groupId("model-group")
            .customProperties(props)
            .build();

    Map<String, String> retrievedProps = modelGroup.getCustomProperties();
    assertNotNull(retrievedProps);
    assertEquals(2, retrievedProps.size());
    assertEquals("transformer", retrievedProps.get("model_family"));
  }

  // ==================== External URL Tests ====================

  @Test
  public void testMLModelGroupExternalUrl() {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("mlflow")
            .groupId("model-group")
            .externalUrl("https://mlflow.example.com/models/group")
            .build();

    String url = modelGroup.getExternalUrl();
    assertEquals("https://mlflow.example.com/models/group", url);
  }

  // ==================== Timestamp Tests ====================

  @Test
  public void testMLModelGroupTimestamps() {
    TimeStamp created = new TimeStamp().setTime(System.currentTimeMillis());
    TimeStamp lastModified = new TimeStamp().setTime(System.currentTimeMillis());

    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("mlflow")
            .groupId("model-group")
            .created(created)
            .lastModified(lastModified)
            .build();

    assertNotNull(modelGroup.getCreated());
    assertNotNull(modelGroup.getLastModified());
    assertEquals(created.getTime(), modelGroup.getCreated().getTime());
  }

  // ==================== Fluent API Tests ====================

  @Test
  public void testMLModelGroupFluentAPI() {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("sagemaker")
            .groupId("churn-model")
            .name("Churn Prediction Models")
            .build();

    modelGroup
        .addTag("churn")
        .addTag("classification")
        .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:CustomerAnalytics")
        .setDomain("urn:li:domain:MachineLearning")
        .setDescription("Customer churn prediction model family")
        .addTrainingJob("urn:li:dataProcessInstance:training_job")
        .addDownstreamJob("urn:li:dataProcessInstance:inference_job");

    List<MetadataChangeProposal> patches = modelGroup.getPendingPatches();
    // Accumulated patches combine operations per aspect (5 aspects modified)
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Utility Tests ====================

  @Test
  public void testMLModelGroupToMCPs() {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("mlflow")
            .groupId("model-group")
            .name("Test Model Group")
            .build();

    List<MetadataChangeProposalWrapper> mcps = modelGroup.toMCPs();

    assertNotNull(mcps);
    assertFalse(mcps.isEmpty());

    for (MetadataChangeProposalWrapper mcp : mcps) {
      assertEquals(mcp.getEntityType(), "mlModelGroup");
      assertNotNull(mcp.getEntityUrn());
      assertNotNull(mcp.getAspect());
    }
  }

  @Test
  public void testMLModelGroupClearPendingPatches() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    modelGroup.addTag("tag1");
    modelGroup.addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER);

    assertTrue("Should have pending patches", modelGroup.hasPendingPatches());

    modelGroup.clearPendingPatches();

    assertFalse("Should not have pending patches", modelGroup.hasPendingPatches());
  }

  @Test
  public void testMLModelGroupEqualsAndHashCode() {
    MLModelGroup modelGroup1 =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    MLModelGroup modelGroup2 =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    assertEquals(modelGroup1, modelGroup2);
    assertEquals(modelGroup1.hashCode(), modelGroup2.hashCode());
  }

  @Test
  public void testMLModelGroupToString() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    String str = modelGroup.toString();
    assertNotNull(str);
    assertTrue(str.contains("MLModelGroup"));
    assertTrue(str.contains("urn"));
  }

  @Test
  public void testMLModelGroupUrnStructure() {
    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("mlflow")
            .groupId("recommendation-model")
            .env("PROD")
            .build();

    String urnString = modelGroup.getUrn().toString();
    assertTrue(urnString.contains("urn:li:mlModelGroup:"));
    assertTrue(urnString.contains("mlflow"));
    assertTrue(urnString.contains("recommendation-model"));
    assertTrue(urnString.contains("PROD"));
  }

  // ==================== Workflow Tests ====================

  @Test
  public void testMLModelGroupCompleteWorkflow() {
    Map<String, String> props = new HashMap<>();
    props.put("model_type", "classification");
    props.put("framework", "tensorflow");

    MLModelGroup modelGroup =
        MLModelGroup.builder()
            .platform("vertexai")
            .groupId("fraud-detector")
            .env("PROD")
            .name("Fraud Detection Model Family")
            .description("Models for detecting fraudulent transactions")
            .externalUrl("https://example.com/models/fraud")
            .customProperties(props)
            .build();

    modelGroup
        .addTag("fraud-detection")
        .addTag("production")
        .addOwner("urn:li:corpuser:security_team", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:FraudDetection")
        .setDomain("urn:li:domain:Security")
        .addTrainingJob("urn:li:dataProcessInstance:fraud_training")
        .addDownstreamJob("urn:li:dataProcessInstance:fraud_scoring");

    assertFalse(
        "Model group with builder properties has cached aspects, so not new",
        modelGroup.isNewEntity());
    assertTrue("Should have pending patches", modelGroup.hasPendingPatches());
    assertNotNull("Should have URN", modelGroup.getUrn());
    assertEquals("Should have correct entity type", "mlModelGroup", modelGroup.getEntityType());
  }

  @Test
  public void testMLModelGroupPatchVsFullAspect() {
    MLModelGroup modelGroup1 =
        MLModelGroup.builder().platform("mlflow").groupId("group1").name("Model Group 1").build();

    List<MetadataChangeProposalWrapper> mcps1 = modelGroup1.toMCPs();
    assertFalse("Should have cached aspects", mcps1.isEmpty());

    MLModelGroup modelGroup2 = MLModelGroup.builder().platform("mlflow").groupId("group2").build();

    modelGroup2.setDescription("Description from setter");
    List<MetadataChangeProposal> patches2 = modelGroup2.getPendingPatches();
    assertFalse("Should have pending patches", patches2.isEmpty());
  }

  @Test
  public void testMLModelGroupModeAwareBehavior() {
    MLModelGroup modelGroup =
        MLModelGroup.builder().platform("mlflow").groupId("model-group").build();

    assertTrue("Default should be SDK mode", modelGroup.isSdkMode());

    modelGroup.setMode(datahub.client.v2.config.DataHubClientConfigV2.OperationMode.INGESTION);
    assertTrue("Should be in ingestion mode", modelGroup.isIngestionMode());
  }
}
