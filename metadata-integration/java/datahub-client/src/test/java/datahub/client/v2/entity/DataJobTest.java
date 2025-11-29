package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Tests for DataJob entity builder and patch-based operations. */
public class DataJobTest {

  // ==================== Builder Tests ====================

  @Test
  public void testDataJobBuilderMinimal() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    assertNotNull(dataJob);
    assertNotNull(dataJob.getUrn());
    assertNotNull(dataJob.getDataJobUrn());
    assertEquals(dataJob.getEntityType(), "dataJob");
    assertTrue(dataJob.isNewEntity());
  }

  @Test
  public void testDataJobBuilderWithDescription() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .name("Extract Customer Data")
            .type("COMMAND")
            .description("Extracts customer data from MySQL")
            .build();

    assertNotNull(dataJob);
    List<MetadataChangeProposalWrapper> mcps = dataJob.toMCPs();
    assertFalse(mcps.isEmpty());

    // Should have DataJobInfo aspect cached from builder
    boolean hasInfo =
        mcps.stream()
            .anyMatch(mcp -> mcp.getAspect().getClass().getSimpleName().equals("DataJobInfo"));
    assertTrue("Should have DataJobInfo aspect", hasInfo);
  }

  @Test
  public void testDataJobBuilderWithAllOptions() throws Exception {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("retries", "3");
    customProps.put("timeout", "300");

    DataJob dataJob =
        DataJob.builder()
            .orchestrator("dagster")
            .flowId("sales_pipeline")
            .cluster("staging")
            .jobId("transform_sales")
            .description("Transforms sales data")
            .name("Transform Sales Data")
            .type("COMMAND")
            .customProperties(customProps)
            .build();

    assertNotNull(dataJob);
    assertTrue(dataJob.getUrn().toString().contains("dagster"));
    assertTrue(dataJob.getUrn().toString().contains("sales_pipeline"));
    assertTrue(dataJob.getUrn().toString().contains("staging"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataJobBuilderMissingOrchestrator() throws Exception {
    DataJob.builder().flowId("etl_pipeline").jobId("extract_task").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataJobBuilderMissingFlowId() throws Exception {
    DataJob.builder().orchestrator("airflow").jobId("extract_task").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDataJobBuilderMissingJobId() throws Exception {
    DataJob.builder().orchestrator("airflow").flowId("etl_pipeline").build();
  }

  // ==================== Input Dataset Operation Tests (Input Lineage) ====================

  @Test
  public void testDataJobAddInputDataset() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,source.table,PROD)");
    dataJob.addInputDataset(dataset);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("dataJobInputOutput", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobAddInputDatasetString() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addInputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,source.table,PROD)");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("dataJobInputOutput", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobSetInputDatasets() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    List<String> inputDatasets =
        Arrays.asList(
            "urn:li:dataset:(urn:li:dataPlatform:mysql,orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,customers,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:mysql,products,PROD)");

    dataJob.setInputDatasets(inputDatasets);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    for (MetadataChangeProposal patch : patches) {
      assertEquals("dataJobInputOutput", patch.getAspectName());
    }
  }

  @Test
  public void testDataJobRemoveInputDataset() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,source.table,PROD)");
    dataJob.addInputDataset(dataset);
    dataJob.removeInputDataset(dataset);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataJobRemoveInputDatasetString() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    String datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,source.table,PROD)";
    dataJob.addInputDataset(datasetUrn);
    dataJob.removeInputDataset(datasetUrn);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataJobGetInputDatasets() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    // Initially empty
    List<DatasetUrn> inputDatasets = dataJob.getInputDatasets();
    assertNotNull(inputDatasets);
    assertTrue(inputDatasets.isEmpty());
  }

  @Test
  public void testDataJobMultipleInputDatasets() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    DatasetUrn dataset1 =
        DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,orders,PROD)");
    DatasetUrn dataset2 =
        DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,customers,PROD)");
    DatasetUrn dataset3 =
        DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,products,PROD)");

    dataJob.addInputDataset(dataset1).addInputDataset(dataset2).addInputDataset(dataset3);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    for (MetadataChangeProposal patch : patches) {
      assertEquals("dataJobInputOutput", patch.getAspectName());
    }
  }

  // ==================== Output Dataset Operation Tests (Output Lineage) ====================

  @Test
  public void testDataJobAddOutputDataset() throws Exception {
    DataJob dataJob =
        DataJob.builder().orchestrator("airflow").flowId("etl_pipeline").jobId("load_task").build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,target.table,PROD)");
    dataJob.addOutputDataset(dataset);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("dataJobInputOutput", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobAddOutputDatasetString() throws Exception {
    DataJob dataJob =
        DataJob.builder().orchestrator("airflow").flowId("etl_pipeline").jobId("load_task").build();

    dataJob.addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,target.table,PROD)");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("dataJobInputOutput", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobSetOutputDatasets() throws Exception {
    DataJob dataJob =
        DataJob.builder().orchestrator("airflow").flowId("etl_pipeline").jobId("load_task").build();

    List<String> outputDatasets =
        Arrays.asList(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.enriched,PROD)");

    dataJob.setOutputDatasets(outputDatasets);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    for (MetadataChangeProposal patch : patches) {
      assertEquals("dataJobInputOutput", patch.getAspectName());
    }
  }

  @Test
  public void testDataJobRemoveOutputDataset() throws Exception {
    DataJob dataJob =
        DataJob.builder().orchestrator("airflow").flowId("etl_pipeline").jobId("load_task").build();

    DatasetUrn dataset =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,target.table,PROD)");
    dataJob.addOutputDataset(dataset);
    dataJob.removeOutputDataset(dataset);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataJobRemoveOutputDatasetString() throws Exception {
    DataJob dataJob =
        DataJob.builder().orchestrator("airflow").flowId("etl_pipeline").jobId("load_task").build();

    String datasetUrn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,target.table,PROD)";
    dataJob.addOutputDataset(datasetUrn);
    dataJob.removeOutputDataset(datasetUrn);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataJobGetOutputDatasets() throws Exception {
    DataJob dataJob =
        DataJob.builder().orchestrator("airflow").flowId("etl_pipeline").jobId("load_task").build();

    // Initially empty
    List<DatasetUrn> outputDatasets = dataJob.getOutputDatasets();
    assertNotNull(outputDatasets);
    assertTrue(outputDatasets.isEmpty());
  }

  @Test
  public void testDataJobMultipleOutputDatasets() throws Exception {
    DataJob dataJob =
        DataJob.builder().orchestrator("airflow").flowId("etl_pipeline").jobId("load_task").build();

    DatasetUrn dataset1 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)");
    DatasetUrn dataset2 =
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.enriched,PROD)");

    dataJob.addOutputDataset(dataset1).addOutputDataset(dataset2);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    for (MetadataChangeProposal patch : patches) {
      assertEquals("dataJobInputOutput", patch.getAspectName());
    }
  }

  // ==================== Complete Lineage Tests ====================

  @Test
  public void testDataJobWithInputAndOutputDatasets() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("transform_task")
            .build();

    // Add inlets
    dataJob.addInputDataset(
        DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,orders,PROD)"));
    dataJob.addInputDataset(
        DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,customers,PROD)"));

    // Add outlets
    dataJob.addOutputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)"));
    dataJob.addOutputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.enriched,PROD)"));

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    for (MetadataChangeProposal patch : patches) {
      assertEquals("dataJobInputOutput", patch.getAspectName());
    }
  }

  @Test
  public void testDataJobETLPattern() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("transform_task")
            .build();

    // Add 3 inlets
    dataJob
        .addInputDataset(
            DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,orders,PROD)"))
        .addInputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,customers,PROD)"))
        .addInputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,products,PROD)"));

    // Add 2 outlets
    dataJob
        .addOutputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)"))
        .addOutputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.enriched,PROD)"));

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Standard Metadata Tests ====================

  @Test
  public void testDataJobAddTag() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addTag("critical");
    dataJob.addTag("urn:li:tag:priority");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("globalTags", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobAddOwner() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobAddTerm() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addTerm("urn:li:glossaryTerm:DataProcessing");
    dataJob.addTerm("urn:li:glossaryTerm:ETL");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobSetDomain() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.setDomain("urn:li:domain:Engineering");

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dataJob.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDataJobAddCustomProperty() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addCustomProperty("retries", "3");
    dataJob.addCustomProperty("timeout", "300");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("dataJobInfo", patches.get(0).getAspectName());
  }

  // ==================== DataJob-Specific Property Tests ====================

  @Test
  public void testDataJobSetDescription() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.setDescription("Extracts customer data from MySQL source");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have description patch", patches.isEmpty());
    assertEquals("dataJobInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobSetDisplayName() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.setName("Extract Customer Data");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have name patch", patches.isEmpty());
    assertEquals("dataJobInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobRemoveCustomProperty() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addCustomProperty("temp", "value");
    dataJob.removeCustomProperty("temp");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Fluent API Tests ====================

  @Test
  public void testDataJobFluentAPI() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("sales_pipeline")
            .jobId("transform_sales")
            .name("Transform Sales")
            .type("COMMAND")
            .description("Transforms sales data")
            .build();

    // Test fluent method chaining with inlets and outlets
    dataJob
        .addTag("critical")
        .addTag("production")
        .addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:Sales")
        .setDomain("urn:li:domain:Finance")
        .addInputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,raw_sales,PROD)"))
        .addInputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,raw_customers,PROD)"))
        .addOutputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.sales,PROD)"))
        .addCustomProperty("sla", "1hour");

    // Verify patches were accumulated
    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataJobFluentAPIComplexLineage() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("dagster")
            .flowId("analytics_pipeline")
            .jobId("aggregate_metrics")
            .build();

    dataJob
        .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:mysql,events,PROD)")
        .addInputDataset("urn:li:dataset:(urn:li:dataPlatform:mysql,users,PROD)")
        .addOutputDataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,metrics.daily,PROD)")
        .setDescription("Aggregates daily metrics from events")
        .setName("Aggregate Daily Metrics")
        .addTag("metrics");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Utility Tests ====================

  @Test
  public void testDataJobToMCPs() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .name("Extract Customer Data")
            .type("COMMAND")
            .description("Extracts customer data")
            .build();

    // toMCPs returns cached aspects (from builder), not patches
    List<MetadataChangeProposalWrapper> mcps = dataJob.toMCPs();

    assertNotNull(mcps);
    assertFalse(mcps.isEmpty());

    // Verify all MCPs have correct entity type and URN
    for (MetadataChangeProposalWrapper mcp : mcps) {
      assertEquals(mcp.getEntityType(), "dataJob");
      assertNotNull(mcp.getEntityUrn());
      assertNotNull(mcp.getAspect());
    }
  }

  @Test
  public void testDataJobClearPendingPatches() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    // Add some patches
    dataJob.addTag("tag1");
    dataJob.addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER);
    dataJob.addInputDataset(
        DatasetUrn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,source,PROD)"));

    assertTrue("Should have pending patches", dataJob.hasPendingPatches());

    // Clear patches
    dataJob.clearPendingPatches();

    assertFalse("Should not have pending patches", dataJob.hasPendingPatches());
  }

  @Test
  public void testDataJobEqualsAndHashCode() throws Exception {
    DataJob dataJob1 =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    DataJob dataJob2 =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    // Should be equal based on URN
    assertEquals(dataJob1, dataJob2);
    assertEquals(dataJob1.hashCode(), dataJob2.hashCode());
  }

  @Test
  public void testDataJobUrn() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .cluster("staging")
            .jobId("extract_task")
            .build();

    String urnString = dataJob.getUrn().toString();
    assertNotNull(urnString);

    // Verify URN structure includes DataFlow
    assertTrue("URN should contain orchestrator", urnString.contains("airflow"));
    assertTrue("URN should contain flowId", urnString.contains("etl_pipeline"));
    assertTrue("URN should contain cluster", urnString.contains("staging"));
    assertTrue("URN should contain jobId", urnString.contains("extract_task"));
  }

  @Test
  public void testDataJobToString() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    String str = dataJob.toString();
    assertNotNull(str);
    assertTrue(str.contains("DataJob"));
    assertTrue(str.contains("urn"));
  }

  @Test
  public void testDataJobRemoveOwner() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER);
    dataJob.removeOwner("urn:li:corpuser:data_team");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataJobRemoveTag() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addTag("tag1");
    dataJob.removeTag("tag1");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataJobRemoveTerm() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    dataJob.addTerm("urn:li:glossaryTerm:term1");
    dataJob.removeTerm("urn:li:glossaryTerm:term1");

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testDataJobRemoveDomain() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    Urn engineeringDomain = Urn.createFromString("urn:li:domain:Engineering");
    dataJob.setDomain(engineeringDomain.toString());
    dataJob.removeDomain(engineeringDomain);

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = dataJob.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testDataJobSetCustomProperties() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("etl_pipeline")
            .jobId("extract_task")
            .build();

    Map<String, String> customProps = new HashMap<>();
    customProps.put("env", "production");
    customProps.put("team", "data-engineering");

    dataJob.setCustomProperties(customProps);

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have custom properties patch", patches.isEmpty());
    assertEquals("dataJobInfo", patches.get(0).getAspectName());
  }

  @Test
  public void testDataJobPatchVsFullAspect() throws Exception {
    DataJob dataJob1 =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("pipeline1")
            .jobId("task1")
            .name("Task 1")
            .type("COMMAND")
            .description("Description from builder")
            .build();

    // Builder-provided description goes into cached aspects
    List<MetadataChangeProposalWrapper> mcps1 = dataJob1.toMCPs();
    assertFalse("Should have cached aspects", mcps1.isEmpty());

    DataJob dataJob2 =
        DataJob.builder().orchestrator("airflow").flowId("pipeline2").jobId("task2").build();

    // Setter creates a patch
    dataJob2.setDescription("Description from setter");
    List<MetadataChangeProposal> patches2 = dataJob2.getPendingPatches();
    assertFalse("Should have pending patches", patches2.isEmpty());
  }

  @Test
  public void testDataJobRealisticWorkflow() throws Exception {
    // Build a realistic ETL job with complete metadata
    Map<String, String> customProps = new HashMap<>();
    customProps.put("retries", "3");
    customProps.put("timeout", "3600");
    customProps.put("owner_email", "data-team@company.com");

    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("customer_analytics_pipeline")
            .cluster("production")
            .jobId("enrich_customer_profiles")
            .description("Enriches customer profiles with transaction history")
            .name("Enrich Customer Profiles")
            .type("COMMAND")
            .customProperties(customProps)
            .build();

    // Add complete metadata
    dataJob
        .addTag("critical")
        .addTag("pii")
        .addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:CustomerData")
        .addTerm("urn:li:glossaryTerm:PII")
        .setDomain("urn:li:domain:CustomerAnalytics");

    // Add input datasets (inputDatasets)
    dataJob
        .addInputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,customers,PROD)"))
        .addInputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,transactions,PROD)"))
        .addInputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:mysql,user_activity,PROD)"));

    // Add output datasets (outputDatasets)
    dataJob
        .addOutputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_profiles,PROD)"))
        .addOutputDataset(
            DatasetUrn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_metrics,PROD)"));

    // Verify everything was created
    assertNotNull(dataJob.getUrn());
    assertTrue(dataJob.hasPendingPatches());

    List<MetadataChangeProposal> patches = dataJob.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());

    // Verify cached aspects from builder
    List<MetadataChangeProposalWrapper> mcps = dataJob.toMCPs();
    assertFalse("Should have cached aspects from builder", mcps.isEmpty());
  }
}
