package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class DataFlowTest {

  // ==================== Builder Tests ====================

  @Test
  public void testDataFlowBuilderMinimal() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    assertNotNull(dataflow);
    assertNotNull(dataflow.getUrn());
    assertNotNull(dataflow.getDataFlowUrn());
    assertEquals("dataFlow", dataflow.getEntityType());
    assertTrue(dataflow.isNewEntity());
    assertEquals("airflow", dataflow.getDataFlowUrn().getOrchestratorEntity());
    assertEquals("my_dag", dataflow.getDataFlowUrn().getFlowIdEntity());
    assertEquals("prod", dataflow.getDataFlowUrn().getClusterEntity());
  }

  @Test
  public void testDataFlowBuilderWithDisplayName() {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("spark")
            .flowId("my_job")
            .cluster("prod")
            .displayName("My Spark Job")
            .build();

    assertNotNull(dataflow);
    List<MetadataChangeProposalWrapper> mcps = dataflow.toMCPs();
    assertFalse(mcps.isEmpty());

    boolean hasDataFlowInfo =
        mcps.stream()
            .anyMatch(mcp -> mcp.getAspect().getClass().getSimpleName().equals("DataFlowInfo"));
    assertTrue("Should have DataFlowInfo aspect", hasDataFlowInfo);
  }

  @Test
  public void testDataFlowBuilderWithDescription() {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("dbt")
            .flowId("my_dbt_project")
            .cluster("prod")
            .description("Test dataflow description")
            .build();

    assertNotNull(dataflow);
    List<MetadataChangeProposalWrapper> mcps = dataflow.toMCPs();
    assertFalse(mcps.isEmpty());
  }

  @Test
  public void testDataFlowBuilderWithAllOptions() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("schedule", "0 2 * * *");
    customProps.put("team", "data-engineering");

    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("full_dag")
            .cluster("prod-us-west-2")
            .displayName("Full DAG Example")
            .description("Comprehensive dataflow test")
            .customProperties(customProps)
            .build();

    assertNotNull(dataflow);
    assertTrue(dataflow.getUrn().toString().contains("airflow"));
    assertTrue(dataflow.getUrn().toString().contains("full_dag"));
    assertTrue(dataflow.getUrn().toString().contains("prod-us-west-2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataFlowBuilderMissingOrchestrator() {
    DataFlow.builder().flowId("my_dag").cluster("prod").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataFlowBuilderMissingFlowId() {
    DataFlow.builder().orchestrator("airflow").cluster("prod").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataFlowBuilderMissingCluster() {
    DataFlow.builder().orchestrator("airflow").flowId("my_dag").build();
  }

  // ==================== Ownership Tests ====================

  @Test
  public void testDataFlowAddOwner() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testDataFlowAddMultipleOwners() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow
        .addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER)
        .addOwner("urn:li:corpuser:janedoe", OwnershipType.BUSINESS_OWNER)
        .addOwner("urn:li:corpuser:datasteward", OwnershipType.DATA_STEWARD);

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataFlowRemoveOwner() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow
        .addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER)
        .removeOwner("urn:li:corpuser:johndoe");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Tag Tests ====================

  @Test
  public void testDataFlowAddTag() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.addTag("etl").addTag("urn:li:tag:production");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("globalTags", patches.get(0).getAspectName());
  }

  @Test
  public void testDataFlowAddTagWithoutPrefix() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.addTag("pii");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataFlowRemoveTag() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.addTag("tag1").removeTag("tag1");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Glossary Term Tests ====================

  @Test
  public void testDataFlowAddTerm() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.addTerm("urn:li:glossaryTerm:ETL").addTerm("urn:li:glossaryTerm:DataPipeline");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());
  }

  @Test
  public void testDataFlowRemoveTerm() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.addTerm("urn:li:glossaryTerm:term1").removeTerm("urn:li:glossaryTerm:term1");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Domain Tests ====================

  @Test
  public void testDataFlowSetDomain() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.setDomain("urn:li:domain:DataEngineering");

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dataflow.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDataFlowRemoveDomain() throws Exception {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    Urn engineeringDomain = Urn.createFromString("urn:li:domain:DataEngineering");
    dataflow.setDomain(engineeringDomain.toString()).removeDomain(engineeringDomain);

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dataflow.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDataFlowRemoveDomainByString() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow
        .setDomain("urn:li:domain:DataEngineering")
        .removeDomain("urn:li:domain:DataEngineering");

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dataflow.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDataFlowClearDomains() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.clearDomains();

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dataflow.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  // ==================== Description Tests ====================

  @Test
  public void testDataFlowSetDescription() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.setDescription("Test description");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have description patch", patches.isEmpty());
    assertEquals("dataFlowInfo", patches.get(0).getAspectName());
  }

  // ==================== Display Name Tests ====================

  @Test
  public void testDataFlowSetDisplayName() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.setDisplayName("My Display Name");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have display name patch", patches.isEmpty());
    assertEquals("dataFlowInfo", patches.get(0).getAspectName());
  }

  // ==================== Custom Properties Tests ====================

  @Test
  public void testDataFlowAddCustomProperty() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow
        .addCustomProperty("schedule", "0 2 * * *")
        .addCustomProperty("team", "data-engineering");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("dataFlowInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDataFlowRemoveCustomProperty() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.addCustomProperty("env", "production").removeCustomProperty("env");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataFlowSetCustomProperties() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    Map<String, String> props = new HashMap<>();
    props.put("key1", "value1");
    props.put("key2", "value2");

    dataflow.setCustomProperties(props);

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== DataFlow-Specific Properties Tests ====================

  @Test
  public void testDataFlowSetExternalUrl() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.setExternalUrl("https://airflow.example.com/dags/my_dag");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertTrue("Should not have patches (uses direct mutation)", patches.isEmpty());
    assertEquals("https://airflow.example.com/dags/my_dag", dataflow.getExternalUrl());
  }

  @Test
  public void testDataFlowSetProject() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.setProject("marketing");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertTrue("Should not have patches (uses direct mutation)", patches.isEmpty());
    assertEquals("marketing", dataflow.getProject());
  }

  @Test
  public void testDataFlowSetCreated() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    long createdTime = System.currentTimeMillis();
    dataflow.setCreated(createdTime);

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have created timestamp patch", patches.isEmpty());
    assertEquals("dataFlowInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDataFlowSetLastModified() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    long lastModifiedTime = System.currentTimeMillis();
    dataflow.setLastModified(lastModifiedTime);

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have last modified timestamp patch", patches.isEmpty());
    assertEquals("dataFlowInfo", patches.get(0).getAspectName());
  }

  // ==================== Fluent API Tests ====================

  @Test
  public void testDataFlowFluentAPI() {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("sales_pipeline")
            .cluster("prod")
            .displayName("Sales Data Pipeline")
            .description("Daily sales ETL")
            .build();

    dataflow
        .addTag("etl")
        .addTag("production")
        .addTag("daily")
        .addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER)
        .addOwner("urn:li:corpuser:owner2", OwnershipType.BUSINESS_OWNER)
        .addTerm("urn:li:glossaryTerm:Sales")
        .addTerm("urn:li:glossaryTerm:ETL")
        .setDomain("urn:li:domain:Sales")
        .setProject("sales")
        .addCustomProperty("schedule", "0 2 * * *")
        .addCustomProperty("team", "sales-analytics");

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== toMCPs Tests ====================

  @Test
  public void testDataFlowToMCPs() {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("spark")
            .flowId("my_job")
            .cluster("prod")
            .description("Test description")
            .build();

    List<MetadataChangeProposalWrapper> mcps = dataflow.toMCPs();

    assertNotNull(mcps);
    assertFalse(mcps.isEmpty());

    for (MetadataChangeProposalWrapper mcp : mcps) {
      assertEquals("dataFlow", mcp.getEntityType());
      assertNotNull(mcp.getEntityUrn());
      assertNotNull(mcp.getAspect());
    }
  }

  // ==================== Patch Management Tests ====================

  @Test
  public void testDataFlowClearPendingPatches() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    dataflow.addTag("tag1").addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER);

    assertTrue("Should have pending patches", dataflow.hasPendingPatches());

    dataflow.clearPendingPatches();

    assertFalse("Should not have pending patches", dataflow.hasPendingPatches());
  }

  @Test
  public void testDataFlowHasPendingPatches() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    assertFalse("Should not have pending patches initially", dataflow.hasPendingPatches());

    dataflow.addTag("tag1");

    assertTrue("Should have pending patches after adding tag", dataflow.hasPendingPatches());
  }

  // ==================== Utility Tests ====================

  @Test
  public void testDataFlowEqualsAndHashCode() {
    DataFlow dataflow1 =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    DataFlow dataflow2 =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    assertEquals(dataflow1, dataflow2);
    assertEquals(dataflow1.hashCode(), dataflow2.hashCode());
  }

  @Test
  public void testDataFlowToString() {
    DataFlow dataflow =
        DataFlow.builder().orchestrator("airflow").flowId("my_dag").cluster("prod").build();

    String str = dataflow.toString();
    assertNotNull(str);
    assertTrue(str.contains("DataFlow"));
    assertTrue(str.contains("urn"));
  }

  // ==================== URN Structure Tests ====================

  @Test
  public void testDataFlowUrnStructure() {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("customer_etl")
            .cluster("prod-us-west-2")
            .build();

    String urnString = dataflow.getUrn().toString();
    assertTrue("URN should contain orchestrator", urnString.contains("airflow"));
    assertTrue("URN should contain flowId", urnString.contains("customer_etl"));
    assertTrue("URN should contain cluster", urnString.contains("prod-us-west-2"));
    assertTrue("URN should start with urn:li:dataFlow:", urnString.startsWith("urn:li:dataFlow:"));
  }

  @Test
  public void testDataFlowDifferentOrchestrators() {
    DataFlow airflow =
        DataFlow.builder().orchestrator("airflow").flowId("dag1").cluster("prod").build();
    DataFlow spark =
        DataFlow.builder().orchestrator("spark").flowId("job1").cluster("prod").build();
    DataFlow dbt = DataFlow.builder().orchestrator("dbt").flowId("proj1").cluster("prod").build();

    assertNotEquals(airflow.getUrn(), spark.getUrn());
    assertNotEquals(spark.getUrn(), dbt.getUrn());
    assertNotEquals(airflow.getUrn(), dbt.getUrn());
  }

  // ==================== Builder vs Setter Tests ====================

  @Test
  public void testDataFlowBuilderVsSetterForDescription() {
    DataFlow dataflow1 =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("dag1")
            .cluster("prod")
            .description("Description from builder")
            .build();

    List<MetadataChangeProposalWrapper> mcps1 = dataflow1.toMCPs();
    assertFalse("Should have cached aspects", mcps1.isEmpty());

    DataFlow dataflow2 =
        DataFlow.builder().orchestrator("airflow").flowId("dag2").cluster("prod").build();

    dataflow2.setDescription("Description from setter");
    List<MetadataChangeProposal> patches2 = dataflow2.getPendingPatches();
    assertFalse("Should have pending patches", patches2.isEmpty());
  }

  // ==================== Realistic Workflow Tests ====================

  @Test
  public void testDataFlowCompleteWorkflow() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("schedule", "0 2 * * *");
    customProps.put("team", "data-engineering");
    customProps.put("sla_hours", "4");

    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("production_etl_pipeline")
            .cluster("prod-us-east-1")
            .displayName("Production ETL Pipeline")
            .description("Main ETL pipeline for customer data")
            .customProperties(customProps)
            .build();

    dataflow
        .addTag("etl")
        .addTag("production")
        .addTag("pii")
        .addOwner("urn:li:corpuser:data_eng_team", OwnershipType.TECHNICAL_OWNER)
        .addOwner("urn:li:corpuser:product_owner", OwnershipType.BUSINESS_OWNER)
        .addTerm("urn:li:glossaryTerm:ETL")
        .addTerm("urn:li:glossaryTerm:CustomerData")
        .setDomain("urn:li:domain:DataEngineering")
        .setProject("customer_analytics")
        .setCreated(System.currentTimeMillis() - 86400000L)
        .setLastModified(System.currentTimeMillis());

    assertNotNull(dataflow);
    assertTrue(dataflow.hasPendingPatches());

    List<MetadataChangeProposalWrapper> mcps = dataflow.toMCPs();
    assertFalse(mcps.isEmpty());

    List<MetadataChangeProposal> patches = dataflow.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataFlowSparkWorkflow() {
    DataFlow sparkFlow =
        DataFlow.builder()
            .orchestrator("spark")
            .flowId("ml_feature_generation")
            .cluster("emr-prod-cluster")
            .displayName("ML Feature Generation")
            .build();

    sparkFlow
        .setDescription("Large-scale Spark job for ML feature generation")
        .addTag("spark")
        .addTag("machine-learning")
        .addCustomProperty("spark.executor.memory", "8g")
        .addCustomProperty("spark.driver.memory", "4g")
        .addOwner("urn:li:corpuser:ml_team", OwnershipType.TECHNICAL_OWNER)
        .setDomain("urn:li:domain:MachineLearning");

    assertTrue(sparkFlow.hasPendingPatches());
    assertEquals("spark", sparkFlow.getDataFlowUrn().getOrchestratorEntity());
  }

  @Test
  public void testDataFlowDbtWorkflow() {
    DataFlow dbtFlow =
        DataFlow.builder()
            .orchestrator("dbt")
            .flowId("marketing_analytics")
            .cluster("prod")
            .displayName("Marketing Analytics Models")
            .description("dbt transformations for marketing data")
            .build();

    dbtFlow
        .addTag("dbt")
        .addTag("transformation")
        .addTag("analytics")
        .addCustomProperty("dbt_version", "1.5.0")
        .addCustomProperty("target", "production")
        .setProject("marketing")
        .setExternalUrl("https://github.com/company/dbt-marketing");

    assertNotNull(dbtFlow);
    assertEquals("dbt", dbtFlow.getDataFlowUrn().getOrchestratorEntity());
  }
}
