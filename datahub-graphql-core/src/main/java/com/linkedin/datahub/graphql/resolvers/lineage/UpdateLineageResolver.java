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
import java.util.LinkedHashMap;
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

  private static final Set<String> ML_MODEL_ENTITY_TYPES =
      Set.of(Constants.ML_MODEL_ENTITY_NAME, Constants.ML_MODEL_GROUP_ENTITY_NAME);
  private static final Set<String> ML_MODEL_JOB_ENTITY_TYPES =
      Set.of(Constants.DATA_JOB_ENTITY_NAME, Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME);
  private static final Set<String> DATA_JOB_AND_ML_MODEL_ENTITY_TYPES =
      Set.of(
          Constants.DATA_JOB_ENTITY_NAME,
          Constants.ML_MODEL_ENTITY_NAME,
          Constants.ML_MODEL_GROUP_ENTITY_NAME);

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
    validateMlModelEdges(edgesToAdd);
    validateMlModelEdges(edgesToRemove);

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
          for (Urn downstreamUrn : downstreamUrns) {
            if (!existingDownstreamUrns.contains(downstreamUrn)
                && !isOnlyRemovedFromMlModels(
                    downstreamUrn, downstreamToUpstreamsToAdd, downstreamToUpstreamsToRemove)) {
              throw new IllegalArgumentException(
                  String.format(
                      "Cannot upsert lineage as downstream urn %s doesn't exist", downstreamUrn));
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
          // Only an upstream being added must exist. One that appears only in removals may have
          // been hard-deleted; its edge is still removed from the downstream below.
          for (Urn upstreamUrn : upstreamToDownstreamsToAdd.keySet()) {
            if (!existingUpstreamUrns.contains(upstreamUrn)) {
              throw new IllegalArgumentException(
                  String.format(
                      "Cannot upsert lineage as upstream urn %s doesn't exist", upstreamUrn));
            }
          }

          // Both directions of an ML model's lineage live in one properties aspect, so they are
          // collected here and written together once.
          final Map<Urn, MlModelJobChanges> mlModelChanges = new LinkedHashMap<>();

          for (Urn downstreamUrn : downstreamUrns) {
            if (!existingDownstreamUrns.contains(downstreamUrn)) {
              // A hard-deleted job whose ML model upstreams are only being removed: those edges
              // are removed from the model below.
              continue;
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
                      filterOutEntityTypes(
                          upstreamUrnsToAdd, Set.of(Constants.DATA_JOB_ENTITY_NAME));
                  final List<Urn> filteredUpstreamUrnsToRemove =
                      filterOutEntityTypes(
                          upstreamUrnsToRemove, Set.of(Constants.DATA_JOB_ENTITY_NAME));

                  if (!filteredUpstreamUrnsToAdd.isEmpty()
                      || !filteredUpstreamUrnsToRemove.isEmpty()) {
                    _lineageService.updateDatasetLineage(
                        context.getOperationContext(),
                        downstreamUrn,
                        filteredUpstreamUrnsToAdd,
                        filteredUpstreamUrnsToRemove,
                        actor);
                  }
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
                  // ML model upstreams are written on the model, as its downstreamJobEdges, below
                  final List<Urn> jobUpstreamUrnsToAdd =
                      filterOutEntityTypes(upstreamUrnsToAdd, ML_MODEL_ENTITY_TYPES);
                  final List<Urn> jobUpstreamUrnsToRemove =
                      filterOutEntityTypes(upstreamUrnsToRemove, ML_MODEL_ENTITY_TYPES);
                  if (!jobUpstreamUrnsToAdd.isEmpty() || !jobUpstreamUrnsToRemove.isEmpty()) {
                    _lineageService.updateDataJobUpstreamLineage(
                        context.getOperationContext(),
                        downstreamUrn,
                        jobUpstreamUrnsToAdd,
                        jobUpstreamUrnsToRemove,
                        actor);
                  }
                  break;
                case Constants.DATA_FLOW_ENTITY_NAME:
                  // dataFlow has no dataset/dataJob-shaped input/output aspect -- its only
                  // upstream lineage is a linked source-code Repository via repositoryLineage.
                  _lineageService.updateDataFlowUpstreamLineage(
                      context.getOperationContext(),
                      downstreamUrn,
                      upstreamUrnsToAdd,
                      upstreamUrnsToRemove,
                      actor);
                  break;
                case Constants.ML_MODEL_ENTITY_NAME:
                case Constants.ML_MODEL_GROUP_ENTITY_NAME:
                  final MlModelJobChanges changes =
                      mlModelChanges.computeIfAbsent(downstreamUrn, urn -> new MlModelJobChanges());
                  changes.trainingJobsToAdd.addAll(upstreamUrnsToAdd);
                  changes.trainingJobsToRemove.addAll(upstreamUrnsToRemove);
                  break;
                case Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME:
                  // Only ML model upstreams pass validateMlModelEdges; they are written on the
                  // model, as its downstreamJobEdges, below.
                  break;
                default:
                  throw new IllegalArgumentException(
                      String.format(
                          "Unsupported lineage update for entity type %s (urn %s)",
                          downstreamUrn.getEntityType(), downstreamUrn));
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update lineage for urn %s", downstreamUrn), e);
            }
          }

          for (Urn upstreamUrn : upstreamUrns) {
            if (!existingUpstreamUrns.contains(upstreamUrn)) {
              // A hard-deleted upstream that is only being removed has nothing left to update.
              continue;
            }

            final List<Urn> downstreamUrnsToAdd =
                upstreamToDownstreamsToAdd.getOrDefault(upstreamUrn, new ArrayList<>());
            final List<Urn> downstreamUrnsToRemove =
                upstreamToDownstreamsToRemove.getOrDefault(upstreamUrn, new ArrayList<>());
            try {
              if (upstreamUrn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME)) {
                // dataJob and ML model downstreams are written on the downstream, above
                final List<Urn> filteredDownstreamUrnsToAdd =
                    filterOutEntityTypes(downstreamUrnsToAdd, DATA_JOB_AND_ML_MODEL_ENTITY_TYPES);
                final List<Urn> filteredDownstreamUrnsToRemove =
                    filterOutEntityTypes(
                        downstreamUrnsToRemove, DATA_JOB_AND_ML_MODEL_ENTITY_TYPES);

                if (!filteredDownstreamUrnsToAdd.isEmpty()
                    || !filteredDownstreamUrnsToRemove.isEmpty()) {
                  _lineageService.updateDataJobDownstreamLineage(
                      context.getOperationContext(),
                      upstreamUrn,
                      filteredDownstreamUrnsToAdd,
                      filteredDownstreamUrnsToRemove,
                      actor);
                }
              } else if (ML_MODEL_ENTITY_TYPES.contains(upstreamUrn.getEntityType())) {
                final MlModelJobChanges changes =
                    mlModelChanges.computeIfAbsent(upstreamUrn, urn -> new MlModelJobChanges());
                changes.downstreamJobsToAdd.addAll(downstreamUrnsToAdd);
                changes.downstreamJobsToRemove.addAll(downstreamUrnsToRemove);
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update lineage for urn %s", upstreamUrn), e);
            }
          }

          for (Map.Entry<Urn, MlModelJobChanges> entry : mlModelChanges.entrySet()) {
            try {
              updateMlModelJobs(context, entry.getKey(), entry.getValue(), actor);
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update lineage for urn %s", entry.getKey()), e);
            }
          }

          return true;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void updateMlModelJobs(
      @Nonnull final QueryContext context,
      @Nonnull final Urn modelUrn,
      @Nonnull final MlModelJobChanges changes,
      @Nonnull final Urn actor)
      throws Exception {
    if (Constants.ML_MODEL_ENTITY_NAME.equals(modelUrn.getEntityType())) {
      _lineageService.updateMlModelLineage(
          context.getOperationContext(),
          modelUrn,
          changes.trainingJobsToAdd,
          changes.trainingJobsToRemove,
          changes.downstreamJobsToAdd,
          changes.downstreamJobsToRemove,
          actor);
    } else {
      _lineageService.updateMlModelGroupLineage(
          context.getOperationContext(),
          modelUrn,
          changes.trainingJobsToAdd,
          changes.trainingJobsToRemove,
          changes.downstreamJobsToAdd,
          changes.downstreamJobsToRemove,
          actor);
    }
  }

  /**
   * Whether a downstream's only edges in this request are removals of ML model upstreams. Those
   * edges are stored on the model, as its downstreamJobEdges, so they can still be removed after
   * the downstream job has been hard-deleted.
   */
  private static boolean isOnlyRemovedFromMlModels(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final Map<Urn, List<Urn>> downstreamToUpstreamsToAdd,
      @Nonnull final Map<Urn, List<Urn>> downstreamToUpstreamsToRemove) {
    return !downstreamToUpstreamsToAdd.containsKey(downstreamUrn)
        && downstreamToUpstreamsToRemove.getOrDefault(downstreamUrn, List.of()).stream()
            .allMatch(upstream -> ML_MODEL_ENTITY_TYPES.contains(upstream.getEntityType()));
  }

  private static final class MlModelJobChanges {
    private final List<Urn> trainingJobsToAdd = new ArrayList<>();
    private final List<Urn> trainingJobsToRemove = new ArrayList<>();
    private final List<Urn> downstreamJobsToAdd = new ArrayList<>();
    private final List<Urn> downstreamJobsToRemove = new ArrayList<>();
  }

  private static List<Urn> filterOutEntityTypes(
      @Nonnull final List<Urn> urns, @Nonnull final Set<String> entityTypes) {
    return urns.stream()
        .filter(urn -> !entityTypes.contains(urn.getEntityType()))
        .collect(Collectors.toList());
  }

  /**
   * The resolver writes one aspect per entity with no rollback, so unsupported ML model edges are
   * rejected before any write. The other end of an ML model edge must be a dataJob or
   * dataProcessInstance, and a dataProcessInstance downstream is only editable from an ML model.
   */
  private static void validateMlModelEdges(@Nonnull final List<LineageEdge> edges) {
    for (LineageEdge edge : edges) {
      final String upstreamType = UrnUtils.getUrn(edge.getUpstreamUrn()).getEntityType();
      final String downstreamType = UrnUtils.getUrn(edge.getDownstreamUrn()).getEntityType();
      final boolean upstreamIsModel = ML_MODEL_ENTITY_TYPES.contains(upstreamType);
      final boolean downstreamIsModel = ML_MODEL_ENTITY_TYPES.contains(downstreamType);
      final boolean valid =
          upstreamIsModel || downstreamIsModel
              ? ML_MODEL_JOB_ENTITY_TYPES.contains(upstreamIsModel ? downstreamType : upstreamType)
              : !Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME.equals(downstreamType);
      if (!valid) {
        throw new IllegalArgumentException(
            String.format(
                "Unsupported lineage edge from %s to %s",
                edge.getUpstreamUrn(), edge.getDownstreamUrn()));
      }
    }
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
              "Unauthorized to edit %s lineage for %s", upstreamUrn, upstreamUrn.getEntityType()));
    }

    Urn downstreamUrn = UrnUtils.getUrn(lineageEdge.getDownstreamUrn());
    if (!isAuthorized(context, downstreamUrn, editLineagePrivileges)) {
      throw new AuthorizationException(
          String.format(
              "Unauthorized to edit %s lineage for %s",
              downstreamUrn, downstreamUrn.getEntityType()));
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
