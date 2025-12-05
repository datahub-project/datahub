package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.DatasetUrn;
import datahub.client.v2.entity.DataJob;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Integration tests for DataJob entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class DataJobIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testDataJobCreateMinimal() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId("test_job_minimal_" + System.currentTimeMillis())
            .build();

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());
  }

  @Test
  public void testDataJobCreateWithMetadata() throws Exception {
    String jobId = "test_job_with_metadata_" + System.currentTimeMillis();
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId(jobId)
            .name(jobId) // Required for DataJobInfo aspect
            .type("BATCH") // Required for DataJobInfo aspect
            .description("This is a test data job created by Java SDK V2")
            .build();

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());
  }

  @Test
  public void testDataJobWithTags() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId("test_job_with_tags_" + System.currentTimeMillis())
            .build();

    dataJob.addTag("test-tag-1");
    dataJob.addTag("test-tag-2");
    dataJob.addTag("critical");

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());
  }

  @Test
  public void testDataJobWithOwners() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId("test_job_with_owners_" + System.currentTimeMillis())
            .build();

    dataJob.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dataJob.addOwner("urn:li:corpuser:admin", OwnershipType.BUSINESS_OWNER);

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());
  }

  @Test
  public void testDataJobWithDomain() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId("test_job_with_domain_" + System.currentTimeMillis())
            .build();

    dataJob.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());
  }

  @Test
  public void testDataJobWithCustomProperties() throws Exception {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("dag", "test_flow");
    customProps.put("schedule", "daily");
    customProps.put("created_by", "java_sdk_v2");

    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId("test_job_with_custom_props_" + System.currentTimeMillis())
            .name("Test Job with Custom Properties")
            .type("BATCH")
            .customProperties(customProps)
            .build();

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());
  }

  @Test
  public void testDataJobWithInlets() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId("test_job_with_inlets_" + System.currentTimeMillis())
            .build();

    // Add input datasets
    dataJob.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source1,PROD)"));
    dataJob.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source2,PROD)"));
    dataJob.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source3,PROD)"));

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());
  }

  @Test
  public void testDataJobWithOutlets() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId("test_job_with_outlets_" + System.currentTimeMillis())
            .build();

    // Add output datasets
    dataJob.addOutputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target1,PROD)"));
    dataJob.addOutputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target2,PROD)"));
    dataJob.addOutputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target3,PROD)"));

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());
  }

  @Test
  public void testDataJobWithInletsAndOutlets() throws Exception {
    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId("test_job_with_inlets_outlets_" + System.currentTimeMillis())
            .build();

    // Add input datasets
    dataJob.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.input1,PROD)"));
    dataJob.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.input2,PROD)"));

    // Add output datasets
    dataJob.addOutputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.output1,PROD)"));
    dataJob.addOutputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.output2,PROD)"));

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());

    // Validate lineage was written correctly
    DataJob fetched = client.entities().get(dataJob.getUrn().toString(), DataJob.class);
    assertNotNull(fetched);

    // Verify input datasets are present
    com.linkedin.datajob.DataJobInputOutput inputOutput =
        fetched.getAspectLazy(com.linkedin.datajob.DataJobInputOutput.class);

    if (inputOutput != null) {
      // Verify input datasets if they exist
      if (inputOutput.hasInputDatasets()
          && inputOutput.getInputDatasets() != null
          && !inputOutput.getInputDatasets().isEmpty()) {
        assertEquals("Should have 2 input datasets", 2, inputOutput.getInputDatasets().size());

        // Verify the specific input dataset URNs
        java.util.List<String> inputUrns = new java.util.ArrayList<>();
        for (com.linkedin.common.urn.Urn urn : inputOutput.getInputDatasets()) {
          inputUrns.add(urn.toString());
        }
        assertTrue(
            "Should contain input1 dataset",
            inputUrns.contains(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.input1,PROD)"));
        assertTrue(
            "Should contain input2 dataset",
            inputUrns.contains(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.input2,PROD)"));
      } else if (inputOutput.hasInputDatasetEdges()
          && inputOutput.getInputDatasetEdges() != null
          && !inputOutput.getInputDatasetEdges().isEmpty()) {
        assertEquals("Should have 2 input datasets", 2, inputOutput.getInputDatasetEdges().size());

        // Verify the specific input dataset URNs from edges
        java.util.List<String> inputUrns = new java.util.ArrayList<>();
        for (com.linkedin.common.Edge edge : inputOutput.getInputDatasetEdges()) {
          inputUrns.add(edge.getDestinationUrn().toString());
        }
        assertTrue(
            "Should contain input1 dataset",
            inputUrns.contains(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.input1,PROD)"));
        assertTrue(
            "Should contain input2 dataset",
            inputUrns.contains(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.input2,PROD)"));
      } else {
        fail("DataJob should have either inputDatasets or inputDatasetEdges populated");
      }

      // Verify output datasets if they exist
      if (inputOutput.hasOutputDatasets()
          && inputOutput.getOutputDatasets() != null
          && !inputOutput.getOutputDatasets().isEmpty()) {
        assertEquals("Should have 2 output datasets", 2, inputOutput.getOutputDatasets().size());

        // Verify the specific output dataset URNs
        java.util.List<String> outputUrns = new java.util.ArrayList<>();
        for (com.linkedin.common.urn.Urn urn : inputOutput.getOutputDatasets()) {
          outputUrns.add(urn.toString());
        }
        assertTrue(
            "Should contain output1 dataset",
            outputUrns.contains(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.output1,PROD)"));
        assertTrue(
            "Should contain output2 dataset",
            outputUrns.contains(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.output2,PROD)"));
      } else if (inputOutput.hasOutputDatasetEdges()
          && inputOutput.getOutputDatasetEdges() != null
          && !inputOutput.getOutputDatasetEdges().isEmpty()) {
        assertEquals(
            "Should have 2 output datasets", 2, inputOutput.getOutputDatasetEdges().size());

        // Verify the specific output dataset URNs from edges
        java.util.List<String> outputUrns = new java.util.ArrayList<>();
        for (com.linkedin.common.Edge edge : inputOutput.getOutputDatasetEdges()) {
          outputUrns.add(edge.getDestinationUrn().toString());
        }
        assertTrue(
            "Should contain output1 dataset",
            outputUrns.contains(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.output1,PROD)"));
        assertTrue(
            "Should contain output2 dataset",
            outputUrns.contains(
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.output2,PROD)"));
      } else {
        fail("DataJob should have either outputDatasets or outputDatasetEdges populated");
      }
    } else {
      fail("DataJobInputOutput aspect should exist");
    }
  }

  @Test
  public void testDataJobFullMetadata() throws Exception {
    String jobId = "test_job_full_metadata_" + System.currentTimeMillis();
    String testRunValue = String.valueOf(System.currentTimeMillis());

    Map<String, String> customProps = new HashMap<>();
    customProps.put("created_by", "java_sdk_v2");
    customProps.put("test_run", testRunValue);

    DataJob dataJob =
        DataJob.builder()
            .orchestrator("airflow")
            .flowId("test_flow")
            .cluster("prod")
            .jobId(jobId)
            .name(jobId) // Required for DataJobInfo aspect
            .type("BATCH") // Required for DataJobInfo aspect
            .description("Complete data job with all metadata from Java SDK V2")
            .customProperties(customProps)
            .build();

    // Add all types of metadata
    dataJob.addTag("java-sdk-v2");
    dataJob.addTag("integration-test");
    dataJob.addTag("data-processing");

    dataJob.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dataJob.setDomain("urn:li:domain:Engineering");

    // Add lineage - inlets
    dataJob.addInputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.input,PROD)"));

    // Add lineage - outlets
    dataJob.addOutputDataset(
        DatasetUrn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.output,PROD)"));

    client.entities().upsert(dataJob);

    assertNotNull(dataJob.getUrn());

    // Validate all metadata was written correctly
    validateEntityDescription(
        dataJob.getUrn().toString(),
        DataJob.class,
        "Complete data job with all metadata from Java SDK V2");

    validateEntityHasTags(
        dataJob.getUrn().toString(),
        DataJob.class,
        Arrays.asList("java-sdk-v2", "integration-test", "data-processing"));

    validateEntityHasOwners(
        dataJob.getUrn().toString(), DataJob.class, Arrays.asList("urn:li:corpuser:datahub"));

    Map<String, String> expectedCustomProps = new HashMap<>();
    expectedCustomProps.put("created_by", "java_sdk_v2");
    expectedCustomProps.put("test_run", testRunValue);
    validateEntityCustomProperties(dataJob.getUrn().toString(), DataJob.class, expectedCustomProps);
  }

  @Test
  public void testMultipleDataJobCreation() throws Exception {
    // Create multiple data jobs in sequence
    for (int i = 0; i < 5; i++) {
      Map<String, String> customProps = new HashMap<>();
      customProps.put("index", String.valueOf(i));

      DataJob dataJob =
          DataJob.builder()
              .orchestrator("airflow")
              .flowId("test_flow")
              .cluster("prod")
              .jobId("test_job_multi_" + i + "_" + System.currentTimeMillis())
              .name("Data job number " + i)
              .type("BATCH")
              .description("Data job number " + i)
              .customProperties(customProps)
              .build();

      dataJob.addTag("batch-test");

      client.entities().upsert(dataJob);
    }

    // If we get here, all data jobs were created successfully
    assertTrue(true);
  }
}
