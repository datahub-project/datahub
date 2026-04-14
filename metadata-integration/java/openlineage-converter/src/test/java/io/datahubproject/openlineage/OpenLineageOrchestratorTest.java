package io.datahubproject.openlineage.converter;

import static org.testng.Assert.assertEquals;

import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import org.testng.annotations.Test;

public class OpenLineageOrchestratorTest {

  private static DatahubOpenlineageConfig defaultConfig() {
    return DatahubOpenlineageConfig.builder().build();
  }

  /**
   * Tests that the getFlowUrn method (which calls getOrchestrator internally) works with custom
   * producer URIs that don't match the hardcoded OpenLineage/Airflow/Trino patterns.
   *
   * <p>Regression test for https://github.com/datahub-project/datahub/issues/16961
   */
  @Test
  public void testCustomProducerExtractsOrchestratorFromPath() {
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create(
                "https://github.com/myorganization/myproducer/blob/v1-0-0/client"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "client");
  }

  @Test
  public void testCustomProducerWithSimplePath() {
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create("https://example.com/my-custom-producer"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "my-custom-producer");
  }

  @Test
  public void testCustomProducerWithTrailingSlash() {
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create("https://example.com/my-producer/"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "my-producer");
  }

  @Test
  public void testKnownOpenLineageProducerStillWorks() {
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            java.net.URI.create(
                "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"),
            defaultConfig());
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
            defaultConfig());
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
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "trino");
  }

  @Test
  public void testProcessingEngineOverridesProducer() {
    var flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            "spark",
            java.net.URI.create("https://example.com/whatever"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "spark");
  }
}
