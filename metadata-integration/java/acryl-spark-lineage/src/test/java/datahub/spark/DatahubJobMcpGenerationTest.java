package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.dataprocess.DataProcessInstanceRelationships;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for DatahubJob.toMcps() — verifying that the dataJobInputOutput aspect is correctly
 * generated (or skipped) based on inSet/outSet contents. These tests cover the customer-reported
 * issue where early coalesced emissions with empty edges clobbered later emissions that had output
 * dataset edges.
 */
class DatahubJobMcpGenerationTest {

  private static DatahubJob createMinimalJob() throws URISyntaxException {
    DataFlowUrn flowUrn = new DataFlowUrn("spark", "test-pipeline", "default");
    DataJobUrn jobUrn = new DataJobUrn(flowUrn, "test-pipeline");

    DataFlowInfo flowInfo = new DataFlowInfo();
    flowInfo.setName("test-pipeline");

    DataJobInfo jobInfo = new DataJobInfo();
    jobInfo.setName("test-pipeline");
    jobInfo.setFlowUrn(flowUrn);
    jobInfo.setType(DataJobInfo.Type.create("spark"));

    DataProcessInstanceRelationships dpiRelationships = new DataProcessInstanceRelationships();
    dpiRelationships.setParentTemplate(jobUrn);
    dpiRelationships.setUpstreamInstances(new com.linkedin.common.UrnArray());

    DatahubJob job = DatahubJob.builder().build();
    job.setFlowUrn(flowUrn);
    job.setJobUrn(jobUrn);
    job.setDataFlowInfo(flowInfo);
    job.setJobInfo(jobInfo);
    job.setEventTime(System.currentTimeMillis());
    job.setDataProcessInstanceUrn(Urn.createFromString("urn:li:dataProcessInstance:test-run-123"));
    job.setDataProcessInstanceRelationships(dpiRelationships);
    return job;
  }

  private static DatahubDataset createDataset(String name) throws URISyntaxException {
    DatasetUrn urn = new DatasetUrn(new DataPlatformUrn("s3"), name, FabricType.PROD);
    return DatahubDataset.builder().urn(urn).build();
  }

  private static DatahubOpenlineageConfig createConfig(boolean usePatch) {
    return DatahubOpenlineageConfig.builder()
        .fabricType(FabricType.PROD)
        .usePatch(usePatch)
        .materializeDataset(false)
        .includeSchemaMetadata(false)
        .build();
  }

  private static boolean hasDataJobInputOutputMcp(List<MetadataChangeProposal> mcps) {
    return mcps.stream().anyMatch(mcp -> "dataJobInputOutput".equals(mcp.getAspectName()));
  }

  /**
   * Simulates the START event scenario: empty inSet and outSet. The dataJobInputOutput aspect
   * should NOT be emitted, preventing it from clobbering later emissions with actual data.
   */
  @Test
  void testEmptyEdgesSkipsDataJobInputOutputMcp() throws URISyntaxException, IOException {
    DatahubJob job = createMinimalJob();

    assertTrue(job.getInSet().isEmpty());
    assertTrue(job.getOutSet().isEmpty());

    List<MetadataChangeProposal> mcps = job.toMcps(createConfig(false));

    assertFalse(
        hasDataJobInputOutputMcp(mcps),
        "Empty inSet/outSet should NOT produce a dataJobInputOutput MCP");
  }

  /**
   * Simulates the final emission: both inputs and outputs populated. The dataJobInputOutput aspect
   * MUST be emitted with both inputDatasetEdges and outputDatasetEdges.
   */
  @Test
  void testPopulatedOutSetProducesDataJobInputOutputMcp() throws URISyntaxException, IOException {
    DatahubJob job = createMinimalJob();
    job.getInSet().add(createDataset("bucket/input/data"));
    job.getOutSet().add(createDataset("bucket/output/data"));

    List<MetadataChangeProposal> mcps = job.toMcps(createConfig(false));

    assertTrue(
        hasDataJobInputOutputMcp(mcps),
        "Populated inSet/outSet SHOULD produce a dataJobInputOutput MCP");
  }

  /** Verifies the PATCH path includes both input and output edge operations. */
  @Test
  void testPatchPathIncludesOutputEdges() throws URISyntaxException, IOException {
    DatahubJob job = createMinimalJob();
    job.getInSet().add(createDataset("bucket/input/data"));
    job.getOutSet().add(createDataset("bucket/output/data"));

    List<MetadataChangeProposal> mcps = job.toMcps(createConfig(true));

    assertTrue(
        hasDataJobInputOutputMcp(mcps),
        "PATCH path with populated sets SHOULD produce a dataJobInputOutput MCP");

    MetadataChangeProposal patchMcp =
        mcps.stream()
            .filter(mcp -> "dataJobInputOutput".equals(mcp.getAspectName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Expected dataJobInputOutput MCP"));
    String patchContent = patchMcp.getAspect().getValue().asString(Charset.defaultCharset());
    assertTrue(
        patchContent.contains("outputDatasetEdges"),
        "Patch MCP should contain outputDatasetEdges operations");
    assertTrue(
        patchContent.contains("inputDatasetEdges"),
        "Patch MCP should contain inputDatasetEdges operations");
  }

  /**
   * Simulates an intermediate coalesced emission where reads have been captured but the write
   * hasn't happened yet. The dataJobInputOutput should still be emitted (with input edges only).
   */
  @Test
  void testInputOnlyProducesDataJobInputOutputMcp() throws URISyntaxException, IOException {
    DatahubJob job = createMinimalJob();
    job.getInSet().add(createDataset("bucket/input/data"));

    List<MetadataChangeProposal> mcps = job.toMcps(createConfig(false));

    assertTrue(
        hasDataJobInputOutputMcp(mcps), "Input-only job SHOULD produce a dataJobInputOutput MCP");
  }
}
