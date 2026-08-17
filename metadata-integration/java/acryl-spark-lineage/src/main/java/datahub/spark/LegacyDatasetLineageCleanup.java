package datahub.spark;

import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageArray;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.aspect.patch.builder.UpstreamLineagePatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.EventFormatter;
import datahub.event.MetadataChangeProposalWrapper;
import io.datahubproject.openlineage.dataset.DatahubDataset;
import io.datahubproject.openlineage.dataset.DatahubJob;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

final class LegacyDatasetLineageCleanup {
  private LegacyDatasetLineageCleanup() {}

  static List<MetadataChangeProposal> toMcps(Set<DatahubDataset> outputDatasets, boolean usePatch)
      throws IOException {
    List<MetadataChangeProposal> mcps = new ArrayList<>();
    for (DatahubDataset dataset : outputDatasets) {
      UpstreamLineage lineage = dataset.getLineage();
      if (lineage == null || lineage.getUpstreams() == null || lineage.getUpstreams().isEmpty()) {
        continue;
      }
      if (usePatch) {
        mcps.add(toPatchMcp(dataset, lineage));
      } else {
        mcps.add(toUpsertMcp(dataset));
      }
    }
    return mcps;
  }

  private static MetadataChangeProposal toPatchMcp(
      DatahubDataset dataset, UpstreamLineage lineage) {
    UpstreamLineagePatchBuilder patchBuilder =
        new UpstreamLineagePatchBuilder().urn(dataset.getUrn());
    for (Upstream upstream : lineage.getUpstreams()) {
      patchBuilder.removeUpstream(upstream.getDataset());
    }
    if (lineage.getFineGrainedLineages() != null) {
      for (FineGrainedLineage fineGrainedLineage : lineage.getFineGrainedLineages()) {
        if (fineGrainedLineage == null
            || fineGrainedLineage.getUpstreams() == null
            || fineGrainedLineage.getDownstreams() == null) {
          continue;
        }
        for (Urn upstream : fineGrainedLineage.getUpstreams()) {
          for (Urn downstream : fineGrainedLineage.getDownstreams()) {
            patchBuilder.removeFineGrainedUpstreamField(
                upstream,
                StringUtils.defaultIfEmpty(fineGrainedLineage.getTransformOperation(), "TRANSFORM"),
                downstream,
                null);
          }
        }
      }
    }
    return patchBuilder.build();
  }

  private static MetadataChangeProposal toUpsertMcp(DatahubDataset dataset) throws IOException {
    UpstreamLineage emptyLineage =
        new UpstreamLineage()
            .setUpstreams(new UpstreamArray())
            .setFineGrainedLineages(new FineGrainedLineageArray());
    return new EventFormatter()
        .convert(
            MetadataChangeProposalWrapper.create(
                builder ->
                    builder
                        .entityType(DatahubJob.DATASET_ENTITY_TYPE)
                        .entityUrn(dataset.getUrn())
                        .upsert()
                        .aspect(emptyLineage)));
  }
}
