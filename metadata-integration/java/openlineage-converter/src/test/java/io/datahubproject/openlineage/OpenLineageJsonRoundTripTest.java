package io.datahubproject.openlineage;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.converter.OpenLineageToDataHub;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import org.testng.annotations.Test;

/**
 * The facet tests build OpenLineage objects programmatically, which skips deserialization entirely.
 * This one starts from the JSON a producer actually sends, which is where the facet accessors are
 * populated (or not).
 */
public class OpenLineageJsonRoundTripTest {

  private static String load(String resource) {
    try (InputStream in =
        OpenLineageJsonRoundTripTest.class.getClassLoader().getResourceAsStream(resource)) {
      assertNotNull(in, "missing test resource " + resource);
      return new Scanner(in, StandardCharsets.UTF_8.name()).useDelimiter("\\A").next();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void timeseriesFacetsSurviveJsonDeserialization() throws Exception {
    OpenLineage.RunEvent event = OpenLineageClientUtils.runEventFromJson(load("ts_event.json"));

    // The facet accessors must be populated by deserialization, not just by the builders.
    OpenLineage.OutputDataset output = event.getOutputs().get(0);
    assertNotNull(output.getFacets().getLifecycleStateChange(), "lifecycleStateChange");
    assertNotNull(output.getFacets().getDataQualityMetrics(), "dataQualityMetrics");
    assertNotNull(output.getOutputFacets().getOutputStatistics(), "outputStatistics");
    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .orchestrator("airflow")
            .materializeDataset(true)
            .includeSchemaMetadata(true)
            .build();

    List<MetadataChangeProposal> mcps =
        OpenLineageToDataHub.convertRunEventToJob(event, config).toMcps(config);

    assertTrue(
        mcps.stream().anyMatch(m -> "operation".equals(m.getAspectName())),
        "expected an operation aspect from outputStatistics/lifecycleStateChange");
    assertTrue(
        mcps.stream().anyMatch(m -> "datasetProfile".equals(m.getAspectName())),
        "expected a datasetProfile aspect from dataQualityMetrics");
  }
}
