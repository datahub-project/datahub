package com.linkedin.datahub.graphql.resolvers.lineage;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LineageEdge;
import com.linkedin.datahub.graphql.generated.UpsertLineageInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.getAspectFromEntity;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.getAuditStamp;

@Slf4j
@RequiredArgsConstructor
public class UpsertLineageResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpsertLineageInput input = bindArgument(environment.getArgument("input"), UpsertLineageInput.class);
    final List<LineageEdge> edges = input.getEdges();
    final Urn actor = UrnUtils.getUrn(context.getActorUrn());

    return CompletableFuture.supplyAsync(() -> {
      // group edges to get a list of upstream urns for each downstream urn
      Map<Urn, List<Urn>> downstreamToUpstreams = getDownstreamToUpstreamsMap(edges);

      for (Map.Entry<Urn, List<Urn>> entry: downstreamToUpstreams.entrySet()) {
        final Urn downstreamUrn = entry.getKey();
        final List<Urn> upstreamUrns = entry.getValue();

        if (!_entityService.exists(downstreamUrn)) {
          throw new IllegalArgumentException(String.format("Cannot upsert lineage as downstream urn %s doesn't exist", downstreamUrn));
        }

        // make this a switch statement when we deal with more than one entity type
        if (downstreamUrn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
          validateDatasetEdges(upstreamUrns);
          // add permissions check here? or have one overall permissions check above
          try {
            MetadataChangeProposal changeProposal = buildUpsertDatasetLineageProposal(downstreamUrn, upstreamUrns, actor);
            _entityService.ingestProposal(changeProposal, getAuditStamp(actor), false);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to upsert dataset lineage with downstreamUrn %s and upstreamUrns %s", downstreamUrn, upstreamUrns), e);
          }
        }
      }

      return true;
    });
  }

  private Map<Urn, List<Urn>> getDownstreamToUpstreamsMap(@Nonnull List<LineageEdge> edges) {
    final Map<Urn, List<Urn>> downstreamToUpstreams = new HashMap<>();

    for (LineageEdge edge : edges) {
      final Urn downstream = UrnUtils.getUrn(edge.getDownstreamUrn());
      final Urn upstream = UrnUtils.getUrn(edge.getUpstreamUrn());
      final List<Urn> upstreams = downstreamToUpstreams.getOrDefault(downstream, new ArrayList<>());
      upstreams.add(upstream);
      downstreamToUpstreams.put(downstream, upstreams);
    }
    return downstreamToUpstreams;
  }

  private void validateDatasetEdges(@Nonnull final List<Urn> upstreamUrns) {
    for (final Urn destinationUrn : upstreamUrns) {
      if (!destinationUrn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
        throw new IllegalArgumentException(String.format("Tried to add lineage edge with non-dataset node to dataset. Destination urn: %s", destinationUrn));
      }
      validateUrnExists(destinationUrn);
    }
  }

  private void validateUrnExists(@Nonnull Urn destinationUrn) {
    if (!_entityService.exists(destinationUrn)) {
      throw new IllegalArgumentException(String.format("Cannot add lineage edge as urn %s doesn't exist", destinationUrn));
    }
  }

  private MetadataChangeProposal buildUpsertDatasetLineageProposal(@Nonnull final Urn downstreamUrn, @Nonnull List<Urn> upstreamUrns, @Nonnull final Urn actor) throws Exception {
    final UpstreamLineage upstreamLineage = (UpstreamLineage) getAspectFromEntity(downstreamUrn.toString(), Constants.UPSTREAM_LINEAGE_ASPECT_NAME, _entityService, new UpstreamLineage());
    if (!upstreamLineage.hasUpstreams()) {
      upstreamLineage.setUpstreams(new UpstreamArray());
    }

    final UpstreamArray upstreams = upstreamLineage.getUpstreams();
    final List<Urn> upstreamsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamUrns) {
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
      newUpstream.setType(DatasetLineageType.TRANSFORMED); // figure this out later
      upstreams.add(newUpstream);
    }

    upstreamLineage.setUpstreams(upstreams);

    return MutationUtils.buildMetadataChangeProposal(downstreamUrn, Constants.UPSTREAM_LINEAGE_ASPECT_NAME, upstreamLineage, actor, _entityService);
  }
}
