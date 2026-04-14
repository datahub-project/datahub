package io.datahubproject.openlineage.converter;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class OpenLineageOrchestratorTest {

  /**
   * Tests that the getFlowUrn method (which calls getOrchestrator internally) works with custom
   * producer URIs that don't match the hardcoded OpenLineage/Airflow/Trino patterns.
   *
   * <p>Regression test for https://github.com/datahub-project/datahub/issues/16961
   */
  @Test
  public void testCustomProducerExtractsOrchestratorFromPath() {
    // Custom producer URI should extract "client" from the path
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create(
                "https://github.com/myorganization/myproducer/blob/v1-0-0/client"),
            new io.datahubproject.openlineage.config.DatahubOpenlineageConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "client");
  }

  @Test
  public void testCustomProducerWithSimplePath() {
    // A producer URI with a simple path should use the last segment
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create("https://example.com/my-custom-producer"),
            new io.datahubproject.openlineage.config.DatahubOpenlineageConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "my-custom-producer");
  }

  @Test
  public void testCustomProducerWithTrailingSlash() {
    // Trailing slash should be handled gracefully
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create("https://example.com/my-producer/"),
            new io.datahubproject.openlineage.config.DatahubOpenlineageConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "my-producer");
  }

  @Test
  public void testKnownOpenLineageProducerStillWorks() {
    // Standard OpenLineage producer should still work
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create(
                "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"),
            new io.datahubproject.openlineage.config.DatahubOpenlineageConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "client");
  }

  @Test
  public void testAirflowProducerStillWorks() {
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create(
                "https://github.com/apache/airflow/tree/providers-openlineage/1.0.0"),
            new io.datahubproject.openlineage.config.DatahubOpenlineageConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "airflow");
  }

  @Test
  public void testTrinoProducerStillWorks() {
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create("https://github.com/trinodb/trino/blob/v435/core"),
            new io.datahubproject.openlineage.config.DatahubOpenlineageConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "trino");
  }

  @Test
  public void testProcessingEngineOverridesProducer() {
    // When processingEngine is provided, it should take precedence
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            "spark",
            java.net.URI.create("https://example.com/whatever"),
            new io.datahubproject.openlineage.config.DatahubOpenlineageConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "spark");
  }
}
