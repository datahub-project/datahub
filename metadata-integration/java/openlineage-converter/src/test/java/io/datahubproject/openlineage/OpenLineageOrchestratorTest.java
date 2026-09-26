package io.datahubproject.openlineage;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataFlowUrn;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import java.net.URI;
import org.testng.annotations.Test;

/**
 * Tests for custom OpenLineage producer support.
 *
 * <p>Regression tests for https://github.com/datahub-project/datahub/issues/16961
 */
public class OpenLineageOrchestratorTest {

  private static DatahubOpenlineageConfig defaultConfig() {
    return DatahubOpenlineageConfig.builder().fabricType(FabricType.PROD).build();
  }

  @Test
  public void testCustomProducerExtractsOrchestratorFromPath() {
    DataFlowUrn flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            URI.create("https://github.com/myorganization/myproducer/blob/v1-0-0/client"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "client");
  }

  @Test
  public void testCustomProducerWithSimplePath() {
    DataFlowUrn flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            URI.create("https://example.com/my-custom-producer"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "my-custom-producer");
  }

  @Test
  public void testCustomProducerWithTrailingSlash() {
    DataFlowUrn flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            URI.create("https://example.com/my-producer/"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "my-producer");
  }

  @Test
  public void testKnownOpenLineageProducerStillWorks() {
    DataFlowUrn flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            URI.create("https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "client");
  }

  @Test
  public void testAirflowProducerStillWorks() {
    DataFlowUrn flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            URI.create("https://github.com/apache/airflow/tree/providers-openlineage/1.0.0"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "airflow");
  }

  @Test
  public void testTrinoProducerStillWorks() {
    DataFlowUrn flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            null,
            URI.create("https://github.com/trinodb/trino/blob/v435/core"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "trino");
  }

  @Test
  public void testProcessingEngineOverridesProducer() {
    DataFlowUrn flowUrn =
        OpenLineageToDataHub.getFlowUrn(
            "my_namespace",
            "my_job",
            "spark",
            URI.create("https://example.com/whatever"),
            defaultConfig());
    assertEquals(flowUrn.getOrchestratorEntity(), "spark");
  }
}
