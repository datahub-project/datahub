package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateLogicalModelSchemaInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.logical.LogicalModelUtils;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Replaces a logical model's columns. On a breaking change (a removed, renamed, or retyped column)
 * only the child column mappings that reference an affected parent column are torn down; unaffected
 * column edges keep their propagated metadata. A child's dataset-level link is cleared only when
 * the change leaves it with no surviving column mappings. Children are never automatically
 * re-linked.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateLogicalModelSchemaResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private static final int CHILD_PAGE_SIZE = 1000;

  private final EntityClient _entityClient;
  private final GraphClient _graphClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateLogicalModelSchemaInput input =
        bindArgument(environment.getArgument("input"), UpdateLogicalModelSchemaInput.class);
    final Urn urn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canCreateLogicalModels(context)) {
            throw new AuthorizationException(
                "Unauthorized to edit logical models. Please contact your DataHub administrator.");
          }
          try {
            final OperationContext opContext = context.getOperationContext();
            SchemaMetadataUtils.validateColumns(input.getColumns());

            final SchemaMetadata existing =
                SchemaMetadataUtils.readSchemaMetadata(_entityClient, opContext, urn);
            if (existing == null) {
              throw new IllegalArgumentException(
                  String.format("Logical model %s has no schema to update.", urn));
            }

            final SchemaMetadata updated =
                SchemaMetadataUtils.buildSchemaMetadata(
                    existing.getSchemaName(), existing.getPlatform(), input.getColumns());
            final Set<String> affectedParentPaths =
                SchemaMetadataUtils.breakingParentPaths(existing, updated);
            final boolean breaking = !affectedParentPaths.isEmpty();

            // On a breaking change only the child column mappings that reference an affected parent
            // column are torn down; unaffected column edges keep their propagated metadata (tags,
            // terms, structured properties). A child's dataset-level link is cleared only when the
            // change leaves it with no surviving column mappings. Build the teardown proposals
            // BEFORE writing the new schema and ingest them in a single batch ordered
            // teardown-first, schema-last: if enumeration or any teardown fails the schema write
            // never lands, so a retry still detects the breaking change and re-runs the
            // (idempotent)
            // teardown instead of leaving children partially unlinked against an already-changed
            // schema. Tearing down a child edge here is an intentional system cascade authorized by
            // edit access to the parent model alone (the actor need not have edit rights on each
            // child).
            final List<MetadataChangeProposal> proposals = new ArrayList<>();
            if (breaking) {
              proposals.addAll(
                  buildTeardownProposals(urn, affectedParentPaths, context, opContext));
            }
            proposals.add(
                buildMetadataChangeProposalWithUrn(urn, SCHEMA_METADATA_ASPECT_NAME, updated));

            // NOTE: batchIngestProposals is not transactional across very large child sets
            // (proposals
            // are partitioned internally), so an extremely large teardown can still apply
            // partially;
            // the retry-safe ordering above keeps the model and its children eventually consistent.
            _entityClient.batchIngestProposals(opContext, proposals, false);

            if (breaking) {
              log.info(
                  "Breaking schema change on {} tore down child mappings for affected columns {}.",
                  urn,
                  affectedParentPaths);
            }
            return true;
          } catch (IllegalArgumentException e) {
            throw e;
          } catch (Exception e) {
            log.error("Failed to update logical model schema {}", urn, e);
            throw new RuntimeException(
                String.format("Failed to update logical model schema %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Streams physical children in pages via PhysicalInstanceOf and, per page, builds the surgical
   * teardown proposals: for each child, clear only the column-level logicalParent edges whose
   * mapped parent field path is in {@code affectedParentPaths}, and clear the child's dataset-level
   * link only when no column mapping survives. Column mappings for a whole page are read in two
   * batched fetches (no per-child reads).
   */
  private List<MetadataChangeProposal> buildTeardownProposals(
      final Urn parentUrn,
      final Set<String> affectedParentPaths,
      final QueryContext context,
      final OperationContext opContext)
      throws Exception {
    final List<MetadataChangeProposal> proposals = new ArrayList<>();
    int start = 0;
    while (true) {
      final EntityRelationships page =
          _graphClient.getRelatedEntities(
              parentUrn.toString(),
              ImmutableSet.of(PHYSICAL_INSTANCE_OF_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              start,
              CHILD_PAGE_SIZE,
              context.getActorUrn());
      final List<Urn> pageChildren =
          page.getRelationships().stream()
              .map(EntityRelationship::getEntity)
              .collect(Collectors.toList());
      if (!pageChildren.isEmpty()) {
        final Map<Urn, Map<String, String>> pageMappings =
            SchemaMetadataUtils.childColumnMappings(
                _entityClient, opContext, parentUrn, pageChildren);
        for (Urn childUrn : pageChildren) {
          final Map<String, String> childMappings = pageMappings.getOrDefault(childUrn, Map.of());
          final List<String> childFieldsToClear = new ArrayList<>();
          for (Map.Entry<String, String> mapping : childMappings.entrySet()) {
            if (affectedParentPaths.contains(mapping.getValue())) {
              childFieldsToClear.add(mapping.getKey());
            }
          }
          // Clear the dataset-level link only when every mapping the child had is being torn down
          // (i.e. no column edge survives). A child that mapped columns unaffected by this change
          // keeps both its surviving edges and its dataset-level link.
          final boolean clearDatasetLevel = childFieldsToClear.size() == childMappings.size();
          proposals.addAll(
              LogicalModelUtils.buildPartialUnlinkProposals(
                  childUrn, childFieldsToClear, clearDatasetLevel, opContext));
        }
      }
      start += page.getRelationships().size();
      // Terminate on an under-full page rather than on the reported total: a child linked mid
      // operation can make a stale total terminate the loop early and miss children.
      if (page.getRelationships().size() < CHILD_PAGE_SIZE) {
        break;
      }
    }
    return proposals;
  }
}
