package com.linkedin.datahub.graphql.resolvers.lineage;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.getAspectFromEntity;

public class LineageUtils {

  private LineageUtils() { }

  /**
   * Validates that a given list of urns are all datasets and all exist. Throws error if either condition is false for any urn.
   */
  public static void validateDatasetUrns(@Nonnull final List<Urn> urns, @Nonnull final EntityService entityService) {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
        throw new IllegalArgumentException(String.format("Tried to add lineage edge with non-dataset node when we expect a dataset. Upstream urn: %s", urn));
      }
      validateUrnExists(urn, entityService);
    }
  }

  /**
   * Validates that a given urn exists using the entityService
   */
  public static void validateUrnExists(@Nonnull final Urn urn, @Nonnull final EntityService entityService) {
    if (!entityService.exists(urn)) {
      throw new IllegalArgumentException(String.format("Error: urn does not exist: %s", urn));
    }
  }

  /**
   * Builds an MCP of UpstreamLineage for dataset entities.
   */
  @Nonnull
  public static MetadataChangeProposal buildDatasetLineageProposal(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final EntityService entityService
  ) throws Exception {
    UpstreamLineage upstreamLineage = (UpstreamLineage) getAspectFromEntity(
        downstreamUrn.toString(), Constants.UPSTREAM_LINEAGE_ASPECT_NAME, entityService, new UpstreamLineage()
    );
    if (upstreamLineage == null) {
      upstreamLineage = new UpstreamLineage();
    }

    if (!upstreamLineage.hasUpstreams()) {
      upstreamLineage.setUpstreams(new UpstreamArray());
    }

    final UpstreamArray upstreams = upstreamLineage.getUpstreams();
    final List<Urn> upstreamsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamUrnsToAdd) {
      if (upstreams.stream().anyMatch(upstream -> upstream.getDataset().equals(upstreamUrn))) {
        continue;
      }
      upstreamsToAdd.add(upstreamUrn);
    }

    for (final Urn upstreamUrn : upstreamsToAdd) {
      final Upstream newUpstream = new Upstream();
      newUpstream.setDataset(DatasetUrn.createFromUrn(upstreamUrn));
      newUpstream.setAuditStamp(MutationUtils.getAuditStamp(actor));
      newUpstream.setCreatedAuditStamp(MutationUtils.getAuditStamp(actor));
      newUpstream.setType(DatasetLineageType.TRANSFORMED);
      upstreams.add(newUpstream);
    }

    upstreams.removeIf(upstream -> upstreamUrnsToRemove.contains(upstream.getDataset()));

    upstreamLineage.setUpstreams(upstreams);

    return MutationUtils.buildMetadataChangeProposal(
        downstreamUrn, Constants.UPSTREAM_LINEAGE_ASPECT_NAME, upstreamLineage, actor, entityService
    );
  }
}
