package com.linkedin.datahub.graphql.resolvers.lineage;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LineageEdge;
import com.linkedin.datahub.graphql.generated.UpdateLineageInput;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.getAuditStamp;

@Slf4j
@RequiredArgsConstructor
public class UpdateLineageResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn actor = UrnUtils.getUrn(context.getActorUrn());
    final UpdateLineageInput input = bindArgument(environment.getArgument("input"), UpdateLineageInput.class);
    final List<LineageEdge> edgesToAdd = input.getEdgesToAdd();
    final List<LineageEdge> edgesToRemove = input.getEdgesToRemove();

    // organize data to make updating lineage cleaner
    Map<Urn, List<Urn>> downstreamToUpstreamsToAdd = getDownstreamToUpstreamsMap(edgesToAdd);
    Map<Urn, List<Urn>> downstreamToUpstreamsToRemove = getDownstreamToUpstreamsMap(edgesToRemove);
    Set<Urn> downstreamUrns = new HashSet<>();
    downstreamUrns.addAll(downstreamToUpstreamsToAdd.keySet());
    downstreamUrns.addAll(downstreamToUpstreamsToRemove.keySet());

    return CompletableFuture.supplyAsync(() -> {
      // build MCP for every downstreamUrn
      for (Urn downstreamUrn : downstreamUrns) {
        if (!_entityService.exists(downstreamUrn)) {
          throw new IllegalArgumentException(String.format("Cannot upsert lineage as downstream urn %s doesn't exist", downstreamUrn));
        }

        final List<Urn> upstreamUrnsToAdd = downstreamToUpstreamsToAdd.getOrDefault(downstreamUrn, new ArrayList<>());
        final List<Urn> upstreamUrnsToRemove = downstreamToUpstreamsToRemove.getOrDefault(downstreamUrn, new ArrayList<>());

        if (downstreamUrn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
          // need to filter out upstream dataJob entities as we take care of outputDatasets for DataJobInputOutput separately
          final List<Urn> filteredUpstreamUrnsToAdd = upstreamUrnsToAdd.stream().filter(
              upstreamUrn -> !upstreamUrn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME)
          ).collect(Collectors.toList());
          final List<Urn> filteredUpstreamUrnsToRemove = upstreamUrnsToRemove.stream().filter(
              upstreamUrn -> !upstreamUrn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME)
          ).collect(Collectors.toList());

          LineageUtils.validateDatasetUrns(filteredUpstreamUrnsToAdd, _entityService);
          // TODO: add permissions check here for entity type - or have one overall permissions check above
          try {
            MetadataChangeProposal changeProposal = LineageUtils.buildDatasetLineageProposal(
                downstreamUrn, filteredUpstreamUrnsToAdd, filteredUpstreamUrnsToRemove, actor, _entityService);
            _entityService.ingestProposal(changeProposal, getAuditStamp(actor), false);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to update dataset lineage for urn %s", downstreamUrn), e);
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
}
