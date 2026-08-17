package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LegacyDatasetLineageCleanupTest {
  @Test
  void patchRemovesOnlyLineageRepresentedByTheSparkEvent() throws Exception {
    List<MetadataChangeProposal> mcps =
        LegacyDatasetLineageCleanup.toMcps(Set.of(outputDataset()), true);

    assertEquals(1, mcps.size());
    MetadataChangeProposal mcp = mcps.get(0);
    assertEquals(ChangeType.PATCH, mcp.getChangeType());
    assertEquals("upstreamLineage", mcp.getAspectName());
    String patch = mcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
    assertTrue(patch.contains("source"));
    assertTrue(patch.contains("output_col"));
  }

  @Test
  void upsertClearsLegacyDatasetLineage() throws Exception {
    List<MetadataChangeProposal> mcps =
        LegacyDatasetLineageCleanup.toMcps(Set.of(outputDataset()), false);

    assertEquals(1, mcps.size());
    MetadataChangeProposal mcp = mcps.get(0);
    assertEquals(ChangeType.UPSERT, mcp.getChangeType());
    assertEquals("upstreamLineage", mcp.getAspectName());
    String aspect = mcp.getAspect().getValue().asString(StandardCharsets.UTF_8);
    assertTrue(aspect.contains("\"upstreams\":[]"));
    assertTrue(aspect.contains("\"fineGrainedLineages\":[]"));
  }

  @Test
  void skipsDatasetsWithoutColumnLineage() throws Exception {
    DatasetUrn outputUrn = datasetUrn("output");

    assertTrue(
        LegacyDatasetLineageCleanup.toMcps(
                Set.of(DatahubDataset.builder().urn(outputUrn).build()), true)
            .isEmpty());
  }

  private static DatahubDataset outputDataset() throws Exception {
    DatasetUrn sourceUrn = datasetUrn("source");
    DatasetUrn outputUrn = datasetUrn("output");

    UpstreamArray upstreams = new UpstreamArray();
    upstreams.add(new Upstream().setDataset(sourceUrn).setType(DatasetLineageType.TRANSFORMED));

    FineGrainedLineage fineGrainedLineage = new FineGrainedLineage();
    fineGrainedLineage.setUpstreams(new UrnArray(List.of(sourceUrn)));
    fineGrainedLineage.setDownstreams(new UrnArray(List.of(outputUrn, schemaField(outputUrn))));
    fineGrainedLineage.setTransformOperation("TRANSFORM");
    FineGrainedLineageArray fineGrainedLineages = new FineGrainedLineageArray();
    fineGrainedLineages.add(fineGrainedLineage);

    UpstreamLineage lineage =
        new UpstreamLineage().setUpstreams(upstreams).setFineGrainedLineages(fineGrainedLineages);
    return DatahubDataset.builder().urn(outputUrn).lineage(lineage).build();
  }

  private static DatasetUrn datasetUrn(String name) throws Exception {
    return new DatasetUrn(new DataPlatformUrn("snowflake"), name, FabricType.PROD);
  }

  private static com.linkedin.common.urn.Urn schemaField(DatasetUrn datasetUrn) {
    return com.linkedin.common.urn.UrnUtils.getUrn(
        "urn:li:schemaField:(" + datasetUrn + ",output_col)");
  }
}
