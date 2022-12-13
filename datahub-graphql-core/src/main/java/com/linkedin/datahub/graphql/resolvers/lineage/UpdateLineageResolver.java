package com.linkedin.datahub.graphql.resolvers.lineage;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LineageEdge;
import com.linkedin.datahub.graphql.generated.UpdateLineageInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.LineageService;
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

@Slf4j
@RequiredArgsConstructor
public class UpdateLineageResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;
  private final LineageService _lineageService;

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
        try {
          switch (downstreamUrn.getEntityType()) {
            case Constants.DATASET_ENTITY_NAME:
              final List<Urn> filteredUpstreamUrnsToAdd = filterUrnsForDatasetLineage(upstreamUrnsToAdd);
              final List<Urn> filteredUpstreamUrnsToRemove = filterUrnsForDatasetLineage(upstreamUrnsToRemove);

              _lineageService.updateDatasetLineage(downstreamUrn, filteredUpstreamUrnsToAdd, filteredUpstreamUrnsToRemove, actor, context.getAuthentication());
              break;
            case Constants.CHART_ENTITY_NAME:
              _lineageService.updateChartLineage(downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor, context.getAuthentication());
              break;
            case Constants.DASHBOARD_ENTITY_NAME:
              _lineageService.updateDashboardLineage(downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor, context.getAuthentication());
              break;
          }
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to update lineage for urn %s", downstreamUrn), e);
        }
      }

      return true;
    });
  }

  /**
   * Filter out upstream dataJob entities as we take care of outputDatasets for DataJobInputOutput separately.
   * This is necessary since a downstream dataset and upstream data job is valid for DataJobInputOutput.
   * If the upstream is anything else, it is invalid (check in the LineageService).
   */
  private List<Urn> filterUrnsForDatasetLineage(List<Urn> urns) {
    return urns.stream().filter(
        upstreamUrn -> !upstreamUrn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME)
    ).collect(Collectors.toList());
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
