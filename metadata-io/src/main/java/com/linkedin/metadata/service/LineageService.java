package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.linkedin.metadata.entity.AspectUtils.*;

@Slf4j
@RequiredArgsConstructor
public class LineageService {
  private final EntityClient _entityClient;

  /**
   * Validates that a given list of urns are all datasets and all exist. Throws error if either condition is false for any urn.
   */
  public void validateDatasetUrns(@Nonnull final List<Urn> urns, @Nonnull final Authentication authentication) throws Exception {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
        throw new IllegalArgumentException(String.format("Tried to add lineage edge with non-dataset node when we expect a dataset. Upstream urn: %s", urn));
      }
      validateUrnExists(urn, authentication);
    }
  }

  /**
   * Validates that a given urn exists using the entityService
   */
  public void validateUrnExists(@Nonnull final Urn urn, @Nonnull final Authentication authentication) throws Exception {
    if (!_entityClient.exists(urn, authentication)) {
      throw new IllegalArgumentException(String.format("Error: urn does not exist: %s", urn));
    }
  }

  /**
   * Updates dataset lineage by taking in a list of upstreams to add and to remove and updating the existing
   * upstreamLineage aspect.
   */
  public void updateDatasetLineage(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final Authentication authentication
  ) throws Exception {
    validateDatasetUrns(upstreamUrnsToAdd, authentication);
    // TODO: add permissions check here for entity type - or have one overall permissions check above
    try {
      MetadataChangeProposal changeProposal = buildDatasetLineageProposal(
          downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor, authentication);
      _entityClient.ingestProposal(changeProposal, authentication, false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update dataset lineage for urn %s", downstreamUrn), e);
    }
  }

  /**
   * Builds an MCP of UpstreamLineage for dataset entities.
   */
  @Nonnull
  public MetadataChangeProposal buildDatasetLineageProposal(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final Authentication authentication
  ) throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(Constants.DATASET_ENTITY_NAME, downstreamUrn, ImmutableSet.of(Constants.UPSTREAM_LINEAGE_ASPECT_NAME), authentication);

    UpstreamLineage upstreamLineage = new UpstreamLineage();
    if (entityResponse != null && entityResponse.getAspects().containsKey(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)) {
      DataMap dataMap = entityResponse.getAspects().get(Constants.UPSTREAM_LINEAGE_ASPECT_NAME).getValue().data();
      upstreamLineage = new UpstreamLineage(dataMap);
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
      newUpstream.setAuditStamp(getAuditStamp(actor));
      newUpstream.setCreatedAuditStamp(getAuditStamp(actor));
      newUpstream.setType(DatasetLineageType.TRANSFORMED);
      upstreams.add(newUpstream);
    }

    upstreams.removeIf(upstream -> upstreamUrnsToRemove.contains(upstream.getDataset()));

    upstreamLineage.setUpstreams(upstreams);

    return buildMetadataChangeProposal(
        downstreamUrn, Constants.UPSTREAM_LINEAGE_ASPECT_NAME, upstreamLineage
    );
  }
}
