package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.entity.DataFlow;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Integration tests for DataFlow entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class DataFlowIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testDataFlowCreateMinimal() throws Exception {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("test_flow_minimal_" + System.currentTimeMillis())
            .cluster("prod")
            .build();

    client.entities().upsert(dataflow);

    assertNotNull(dataflow.getUrn());
  }

  @Test
  public void testDataFlowCreateWithMetadata() throws Exception {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("test_flow_with_metadata_" + System.currentTimeMillis())
            .cluster("prod")
            .displayName("Test ETL Pipeline")
            .description("This is a test dataflow created by Java SDK V2")
            .build();

    client.entities().upsert(dataflow);

    assertNotNull(dataflow.getUrn());
  }

  @Test
  public void testDataFlowWithTags() throws Exception {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("test_flow_with_tags_" + System.currentTimeMillis())
            .cluster("prod")
            .displayName("DataFlow with tags")
            .build();

    dataflow.addTag("test-tag-1");
    dataflow.addTag("test-tag-2");
    dataflow.addTag("etl");

    client.entities().upsert(dataflow);

    assertNotNull(dataflow.getUrn());
  }

  @Test
  public void testDataFlowWithOwners() throws Exception {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("test_flow_with_owners_" + System.currentTimeMillis())
            .cluster("prod")
            .displayName("DataFlow with owners")
            .build();

    dataflow.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dataflow.addOwner("urn:li:corpuser:admin", OwnershipType.BUSINESS_OWNER);

    client.entities().upsert(dataflow);

    assertNotNull(dataflow.getUrn());
  }

  @Test
  public void testDataFlowWithDomain() throws Exception {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("test_flow_with_domain_" + System.currentTimeMillis())
            .cluster("prod")
            .displayName("DataFlow with domain")
            .build();

    dataflow.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(dataflow);

    assertNotNull(dataflow.getUrn());
  }

  @Test
  public void testDataFlowWithCustomProperties() throws Exception {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("environment", "production");
    customProps.put("schedule", "daily");
    customProps.put("created_by", "java_sdk_v2");

    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("test_flow_with_custom_props_" + System.currentTimeMillis())
            .cluster("prod")
            .displayName("DataFlow with custom properties")
            .customProperties(customProps)
            .build();

    client.entities().upsert(dataflow);

    assertNotNull(dataflow.getUrn());
  }

  @Test
  public void testDataFlowWithProperties() throws Exception {
    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("test_flow_with_properties_" + System.currentTimeMillis())
            .cluster("prod")
            .displayName("DataFlow with dataflow-specific properties")
            .build();

    dataflow.setProject("data-engineering");
    dataflow.setExternalUrl("https://airflow.example.com/dags/test_flow");

    client.entities().upsert(dataflow);

    assertNotNull(dataflow.getUrn());
  }

  @Test
  public void testDataFlowFullMetadata() throws Exception {
    long testRun = System.currentTimeMillis();
    Map<String, String> customProps = new HashMap<>();
    customProps.put("created_by", "java_sdk_v2");
    customProps.put("test_run", String.valueOf(testRun));

    DataFlow dataflow =
        DataFlow.builder()
            .orchestrator("airflow")
            .flowId("test_flow_full_metadata_" + testRun)
            .cluster("prod")
            .displayName("Complete ETL Pipeline")
            .description("Complete dataflow with all metadata from Java SDK V2")
            .customProperties(customProps)
            .build();

    // Add all types of metadata
    dataflow.addTag("java-sdk-v2");
    dataflow.addTag("integration-test");
    dataflow.addTag("etl");

    dataflow.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    dataflow.setDomain("urn:li:domain:Engineering");

    dataflow.setProject("data-engineering");
    dataflow.setExternalUrl("https://airflow.example.com/dags/complete_pipeline");

    client.entities().upsert(dataflow);

    assertNotNull(dataflow.getUrn());

    // Validate all metadata was written correctly
    DataFlow fetched = client.entities().get(dataflow.getUrn().toString(), DataFlow.class);
    assertNotNull(fetched);

    // Validate description
    validateEntityDescription(
        dataflow.getUrn().toString(),
        DataFlow.class,
        "Complete dataflow with all metadata from Java SDK V2");

    // Validate tags
    validateEntityHasTags(
        dataflow.getUrn().toString(),
        DataFlow.class,
        java.util.Arrays.asList("java-sdk-v2", "integration-test", "etl"));

    // Validate owners
    validateEntityHasOwners(
        dataflow.getUrn().toString(),
        DataFlow.class,
        java.util.Arrays.asList("urn:li:corpuser:datahub"));

    // Validate custom properties
    validateEntityCustomProperties(dataflow.getUrn().toString(), DataFlow.class, customProps);

    // Verify dataflow-specific properties
    com.linkedin.datajob.DataFlowInfo info =
        fetched.getAspectLazy(com.linkedin.datajob.DataFlowInfo.class);
    assertNotNull("DataFlowInfo aspect should exist", info);
    assertNotNull("Project should exist", info.getProject());
    assertEquals("data-engineering", info.getProject());
    assertNotNull("External URL should exist", info.getExternalUrl());
    assertEquals(
        "https://airflow.example.com/dags/complete_pipeline", info.getExternalUrl().toString());
  }

  @Test
  public void testMultipleDataFlowCreation() throws Exception {
    // Create multiple dataflows in sequence
    for (int i = 0; i < 5; i++) {
      Map<String, String> customProps = new HashMap<>();
      customProps.put("index", String.valueOf(i));

      DataFlow dataflow =
          DataFlow.builder()
              .orchestrator("airflow")
              .flowId("test_flow_multi_" + i + "_" + System.currentTimeMillis())
              .cluster("prod")
              .displayName("DataFlow number " + i)
              .customProperties(customProps)
              .build();

      dataflow.addTag("batch-test");

      client.entities().upsert(dataflow);
    }

    // If we get here, all dataflows were created successfully
    assertTrue(true);
  }
}
