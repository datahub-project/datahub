package com.linkedin.datahub.graphql.resolvers.lineage;

import static com.datahub.authorization.AuthUtil.buildDisjunctivePrivilegeGroup;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.authorization.ApiGroup.LINEAGE;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;

import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.LineageEdge;
import com.linkedin.datahub.graphql.generated.UpdateLineageInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.LineageService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateLineageResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> _entityService;
  private final LineageService _lineageService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Urn actor = UrnUtils.getUrn(context.getActorUrn());
    final UpdateLineageInput input =
        bindArgument(environment.getArgument("input"), UpdateLineageInput.class);
    final List<LineageEdge> edgesToAdd = input.getEdgesToAdd();
    final List<LineageEdge> edgesToRemove = input.getEdgesToRemove();

    // loop over edgesToAdd and edgesToRemove and ensure the actor has privileges to edit lineage
    // for each entity
    checkPrivileges(context, edgesToAdd, edgesToRemove);

    // organize data to make updating lineage cleaner
    Map<Urn, List<Urn>> downstreamToUpstreamsToAdd = getDownstreamToUpstreamsMap(edgesToAdd);
    Map<Urn, List<Urn>> downstreamToUpstreamsToRemove = getDownstreamToUpstreamsMap(edgesToRemove);
    Set<Urn> downstreamUrns = new HashSet<>();
    downstreamUrns.addAll(downstreamToUpstreamsToAdd.keySet());
    downstreamUrns.addAll(downstreamToUpstreamsToRemove.keySet());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Set<Urn> existingDownstreamUrns =
              _entityService.exists(context.getOperationContext(), downstreamUrns, true);

          // build MCP for every downstreamUrn
          for (Urn downstreamUrn : downstreamUrns) {
            if (!existingDownstreamUrns.contains(downstreamUrn)) {
              throw new IllegalArgumentException(
                  String.format(
                      "Cannot upsert lineage as downstream urn %s doesn't exist", downstreamUrn));
            }

            final List<Urn> upstreamUrnsToAdd =
                downstreamToUpstreamsToAdd.getOrDefault(downstreamUrn, new ArrayList<>());
            final List<Urn> upstreamUrnsToRemove =
                downstreamToUpstreamsToRemove.getOrDefault(downstreamUrn, new ArrayList<>());
            try {
              switch (downstreamUrn.getEntityType()) {
                case Constants.DATASET_ENTITY_NAME:
                  // need to filter out dataJobs since this is a valid lineage edge, but will be
                  // handled in the downstream direction for DataJobInputOutputs
                  final List<Urn> filteredUpstreamUrnsToAdd =
                      filterOutDataJobUrns(upstreamUrnsToAdd);
                  final List<Urn> filteredUpstreamUrnsToRemove =
                      filterOutDataJobUrns(upstreamUrnsToRemove);

                  _lineageService.updateDatasetLineage(
                      context.getOperationContext(),
                      downstreamUrn,
                      filteredUpstreamUrnsToAdd,
                      filteredUpstreamUrnsToRemove,
                      actor);
                  break;
                case Constants.CHART_ENTITY_NAME:
                  _lineageService.updateChartLineage(
                      context.getOperationContext(),
                      downstreamUrn,
                      upstreamUrnsToAdd,
                      upstreamUrnsToRemove,
                      actor);
                  break;
                case Constants.DASHBOARD_ENTITY_NAME:
                  _lineageService.updateDashboardLineage(
                      context.getOperationContext(),
                      downstreamUrn,
                      upstreamUrnsToAdd,
                      upstreamUrnsToRemove,
                      actor);
                  break;
                case Constants.DATA_JOB_ENTITY_NAME:
                  _lineageService.updateDataJobUpstreamLineage(
                      context.getOperationContext(),
                      downstreamUrn,
                      upstreamUrnsToAdd,
                      upstreamUrnsToRemove,
                      actor);
                  break;
                default:
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update lineage for urn %s", downstreamUrn), e);
            }
          }

          Map<Urn, List<Urn>> upstreamToDownstreamsToAdd = getUpstreamToDownstreamMap(edgesToAdd);
          Map<Urn, List<Urn>> upstreamToDownstreamsToRemove =
              getUpstreamToDownstreamMap(edgesToRemove);
          Set<Urn> upstreamUrns = new HashSet<>();
          upstreamUrns.addAll(upstreamToDownstreamsToAdd.keySet());
          upstreamUrns.addAll(upstreamToDownstreamsToRemove.keySet());

          final Set<Urn> existingUpstreamUrns =
              _entityService.exists(context.getOperationContext(), upstreamUrns, true);

          // build MCP for upstreamUrn if necessary
          for (Urn upstreamUrn : upstreamUrns) {
            if (!existingUpstreamUrns.contains(upstreamUrn)) {
              throw new IllegalArgumentException(
                  String.format(
                      "Cannot upsert lineage as downstream urn %s doesn't exist", upstreamUrn));
            }

            final List<Urn> downstreamUrnsToAdd =
                upstreamToDownstreamsToAdd.getOrDefault(upstreamUrn, new ArrayList<>());
            final List<Urn> downstreamUrnsToRemove =
                upstreamToDownstreamsToRemove.getOrDefault(upstreamUrn, new ArrayList<>());
            try {
              if (upstreamUrn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME)) {
                // need to filter out dataJobs since this is a valid lineage edge, but is handled in
                // the upstream direction for DataJobs
                final List<Urn> filteredDownstreamUrnsToAdd =
                    filterOutDataJobUrns(downstreamUrnsToAdd);
                final List<Urn> filteredDownstreamUrnsToRemove =
                    filterOutDataJobUrns(downstreamUrnsToRemove);

                _lineageService.updateDataJobDownstreamLineage(
                    context.getOperationContext(),
                    upstreamUrn,
                    filteredDownstreamUrnsToAdd,
                    filteredDownstreamUrnsToRemove,
                    actor);
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update lineage for urn %s", upstreamUrn), e);
            }
          }

          return true;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private List<Urn> filterOutDataJobUrns(@Nonnull final List<Urn> urns) {
    return urns.stream()
        .filter(upstreamUrn -> !upstreamUrn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME))
        .collect(Collectors.toList());
  }

  private Map<Urn, List<Urn>> getDownstreamToUpstreamsMap(@Nonnull final List<LineageEdge> edges) {
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

  private Map<Urn, List<Urn>> getUpstreamToDownstreamMap(@Nonnull final List<LineageEdge> edges) {
    final Map<Urn, List<Urn>> upstreamToDownstreams = new HashMap<>();

    for (LineageEdge edge : edges) {
      final Urn downstream = UrnUtils.getUrn(edge.getDownstreamUrn());
      final Urn upstream = UrnUtils.getUrn(edge.getUpstreamUrn());
      final List<Urn> downstreams = upstreamToDownstreams.getOrDefault(upstream, new ArrayList<>());
      downstreams.add(downstream);
      upstreamToDownstreams.put(upstream, downstreams);
    }
    return upstreamToDownstreams;
  }

  private boolean isAuthorized(
      @Nonnull final QueryContext context,
      @Nonnull final Urn urn,
      @Nonnull final DisjunctivePrivilegeGroup orPrivilegesGroup) {
    return AuthorizationUtils.isAuthorized(
        context, urn.getEntityType(), urn.toString(), orPrivilegesGroup);
  }

  private void checkLineageEdgePrivileges(
      @Nonnull final QueryContext context,
      @Nonnull final LineageEdge lineageEdge,
      @Nonnull final DisjunctivePrivilegeGroup editLineagePrivileges) {
    Urn upstreamUrn = UrnUtils.getUrn(lineageEdge.getUpstreamUrn());
    if (!isAuthorized(context, upstreamUrn, editLineagePrivileges)) {
      throw new AuthorizationException(
          String.format(
              "Unauthorized to edit %s lineage. Please contact your DataHub administrator.",
              upstreamUrn.getEntityType()));
    }

    Urn downstreamUrn = UrnUtils.getUrn(lineageEdge.getDownstreamUrn());
    if (!isAuthorized(context, downstreamUrn, editLineagePrivileges)) {
      throw new AuthorizationException(
          String.format(
              "Unauthorized to edit %s lineage. Please contact your DataHub administrator.",
              downstreamUrn.getEntityType()));
    }
  }

  /**
   * Loop over each edge to add and each edge to remove and ensure that the user has edit lineage
   * privilege or edit entity privilege for every upstream and downstream urn. Throws an
   * AuthorizationException if the actor doesn't have permissions.
   */
  private void checkPrivileges(
      @Nonnull final QueryContext context,
      @Nonnull final List<LineageEdge> edgesToAdd,
      @Nonnull final List<LineageEdge> edgesToRemove) {

    DisjunctivePrivilegeGroup editLineagePrivileges =
        buildDisjunctivePrivilegeGroup(LINEAGE, UPDATE, null);

    for (LineageEdge edgeToAdd : edgesToAdd) {
      checkLineageEdgePrivileges(context, edgeToAdd, editLineagePrivileges);
    }
    for (LineageEdge edgeToRemove : edgesToRemove) {
      checkLineageEdgePrivileges(context, edgeToRemove, editLineagePrivileges);
    }
  }
}
