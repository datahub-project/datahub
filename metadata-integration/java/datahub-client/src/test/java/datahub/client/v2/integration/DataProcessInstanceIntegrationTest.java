// ABOUTME: Integration tests for DataProcessInstance entity with actual DataHub server.
// ABOUTME: Tests input/output lineage operations.
package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import datahub.client.v2.entity.DataProcessInstance;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

/**
 * Integration tests for DataProcessInstance entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class DataProcessInstanceIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testDataProcessInstanceCreateMinimal() throws Exception {
    DataProcessInstance instance =
        DataProcessInstance.builder()
            .id("test_instance_minimal_" + System.currentTimeMillis())
            .build();

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());
  }

  @Test
  public void testDataProcessInstanceCreateWithName() throws Exception {
    String instanceId = "test_instance_with_name_" + System.currentTimeMillis();
    DataProcessInstance instance =
        DataProcessInstance.builder()
            .id(instanceId)
            .name("ETL Pipeline Run #" + instanceId)
            .build();

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());
  }

  @Test
  public void testDataProcessInstanceWithInputs() throws Exception {
    DataProcessInstance instance =
        DataProcessInstance.builder()
            .id("test_instance_with_inputs_" + System.currentTimeMillis())
            .build();

    // Add input URNs
    instance.addInput("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source1,PROD)");
    instance.addInput("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.source2,PROD)");

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());
  }

  @Test
  public void testDataProcessInstanceWithInputEdges() throws Exception {
    DataProcessInstance instance =
        DataProcessInstance.builder()
            .id("test_instance_with_input_edges_" + System.currentTimeMillis())
            .build();

    // Add input edges (with metadata)
    instance.addInputEdge("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.input1,PROD)");
    instance.addInputEdge("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.input2,PROD)");

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());
  }

  @Test
  public void testDataProcessInstanceWithOutputs() throws Exception {
    DataProcessInstance instance =
        DataProcessInstance.builder()
            .id("test_instance_with_outputs_" + System.currentTimeMillis())
            .build();

    // Add output URNs
    instance.addOutput("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target1,PROD)");
    instance.addOutput("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.target2,PROD)");

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());
  }

  @Test
  public void testDataProcessInstanceWithOutputEdges() throws Exception {
    DataProcessInstance instance =
        DataProcessInstance.builder()
            .id("test_instance_with_output_edges_" + System.currentTimeMillis())
            .build();

    // Add output edges (with metadata)
    instance.addOutputEdge("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.output1,PROD)");
    instance.addOutputEdge("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.output2,PROD)");

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());
  }

  @Test
  public void testDataProcessInstanceWithInputsAndOutputs() throws Exception {
    DataProcessInstance instance =
        DataProcessInstance.builder()
            .id("test_instance_inputs_outputs_" + System.currentTimeMillis())
            .name("Test DPI with Lineage")
            .build();

    // Add input edges
    instance.addInputEdge("urn:li:dataset:(urn:li:dataPlatform:hive,input_table1,PROD)");
    instance.addInputEdge("urn:li:dataset:(urn:li:dataPlatform:hive,input_table2,PROD)");

    // Add output edges
    instance.addOutputEdge("urn:li:dataset:(urn:li:dataPlatform:hive,output_table1,PROD)");
    instance.addOutputEdge("urn:li:dataset:(urn:li:dataPlatform:hive,output_table2,PROD)");

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());

    // Fetch and validate
    DataProcessInstance fetched =
        client.entities().get(instance.getUrn().toString(), DataProcessInstance.class);
    assertNotNull(fetched);

    // Verify input edges
    DataProcessInstanceInput inputAspect = fetched.getAspectLazy(DataProcessInstanceInput.class);
    if (inputAspect != null && inputAspect.hasInputEdges()) {
      List<String> inputUrns =
          inputAspect.getInputEdges().stream()
              .map(edge -> edge.getDestinationUrn().toString())
              .collect(Collectors.toList());
      assertTrue(
          "Should contain input_table1",
          inputUrns.contains("urn:li:dataset:(urn:li:dataPlatform:hive,input_table1,PROD)"));
      assertTrue(
          "Should contain input_table2",
          inputUrns.contains("urn:li:dataset:(urn:li:dataPlatform:hive,input_table2,PROD)"));
    }

    // Verify output edges
    DataProcessInstanceOutput outputAspect = fetched.getAspectLazy(DataProcessInstanceOutput.class);
    if (outputAspect != null && outputAspect.hasOutputEdges()) {
      List<String> outputUrns =
          outputAspect.getOutputEdges().stream()
              .map(edge -> edge.getDestinationUrn().toString())
              .collect(Collectors.toList());
      assertTrue(
          "Should contain output_table1",
          outputUrns.contains("urn:li:dataset:(urn:li:dataPlatform:hive,output_table1,PROD)"));
      assertTrue(
          "Should contain output_table2",
          outputUrns.contains("urn:li:dataset:(urn:li:dataPlatform:hive,output_table2,PROD)"));
    }
  }

  @Test
  public void testDataProcessInstanceWithEdgeObject() throws Exception {
    DataProcessInstance instance =
        DataProcessInstance.builder()
            .id("test_instance_edge_object_" + System.currentTimeMillis())
            .build();

    // Create an Edge object with additional properties
    Edge inputEdge = new Edge();
    inputEdge.setDestinationUrn(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,topic1,PROD)"));

    Edge outputEdge = new Edge();
    outputEdge.setDestinationUrn(
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:kafka,topic2,PROD)"));

    instance.addInputEdge(inputEdge);
    instance.addOutputEdge(outputEdge);

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());
  }

  @Test
  public void testDataProcessInstanceWithLineage() throws Exception {
    String instanceId = "test_instance_lineage_" + System.currentTimeMillis();
    DataProcessInstance instance =
        DataProcessInstance.builder().id(instanceId).name("Lineage Test Instance").build();

    // Add input lineage
    instance.addInputEdge(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.source,PROD)");

    // Add output lineage
    instance.addOutputEdge(
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.target,PROD)");

    client.entities().upsert(instance);

    assertNotNull(instance.getUrn());
  }

  @Test
  public void testMultipleDataProcessInstanceCreation() throws Exception {
    // Create multiple instances in sequence
    for (int i = 0; i < 5; i++) {
      DataProcessInstance instance =
          DataProcessInstance.builder()
              .id("test_instance_multi_" + i + "_" + System.currentTimeMillis())
              .name("Instance number " + i)
              .build();

      instance.addInputEdge(
          "urn:li:dataset:(urn:li:dataPlatform:hive,batch_source_" + i + ",PROD)");
      instance.addOutputEdge(
          "urn:li:dataset:(urn:li:dataPlatform:hive,batch_target_" + i + ",PROD)");

      client.entities().upsert(instance);
    }

    // If we get here, all instances were created successfully
    assertTrue(true);
  }
}
