package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchEntityResult;
import com.linkedin.datahub.graphql.resolvers.mutate.util.PatchResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.GenericAspect;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PatchEntityResolver implements DataFetcher<CompletableFuture<PatchEntityResult>> {


  private final EntityService _entityService;
  private final EntityClient _entityClient;
  private final EntityRegistry _entityRegistry;

  @Override
  public CompletableFuture<PatchEntityResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final PatchEntityInput input =
        bindArgument(environment.getArgument("input"), PatchEntityInput.class);
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return patchEntity(input, context);
          } catch (Exception e) {
            log.error("Failed to patch entity: {}", e.getMessage(), e);
            // Use a placeholder URN if none was provided
            String urn = input.getUrn() != null ? input.getUrn() : "urn:li:unknown:error";
            return new PatchEntityResult(urn, null, false, e.getMessage());
          }
        },
        this.getClass().getSimpleName(),
        "patchEntity");
  }

  private PatchEntityResult patchEntity(
      @Nonnull PatchEntityInput input, @Nonnull QueryContext context) throws Exception {
    
    // Handle URN resolution - either provided or auto-generated
    final Urn entityUrn = PatchResolverUtils.resolveEntityUrn(input.getUrn(), input.getEntityType());
    final Authentication authentication = context.getAuthentication();

    // Check authorization using GraphQL authorization pattern
    if (!isAuthorized(input, context)) {
      throw new AuthorizationException(
          authentication.getActor().toUrnStr() + " is unauthorized to update entities.");
    }

    // Validate aspect exists
    final AspectSpec aspectSpec =
        _entityRegistry
            .getEntitySpec(entityUrn.getEntityType())
            .getAspectSpec(input.getAspectName());
    if (aspectSpec == null) {
      throw new IllegalArgumentException(
          "Aspect "
              + input.getAspectName()
              + " not found for entity type "
              + entityUrn.getEntityType());
    }

    // Create patch aspect using the same pattern as OpenAPI
    final GenericAspect patchAspect = PatchResolverUtils.createPatchAspect(input.getPatch());

    // Create MetadataChangeProposal
    final com.linkedin.mxe.MetadataChangeProposal mcp =
        PatchResolverUtils.createMetadataChangeProposal(
            entityUrn,
            input.getAspectName(),
            patchAspect,
            input.getSystemMetadata(),
            input.getHeaders());

    // Apply the patch
    try {
      Urn actor = UrnUtils.getUrn(authentication.getActor().toUrnStr());
      _entityService.ingestProposal(
          context.getOperationContext(),
          mcp,
          AuditStampUtils.createAuditStamp(actor.toString()),
          false); // synchronous for GraphQL

      // Extract entity name from the patch operations
      String entityName = PatchResolverUtils.extractEntityName(input.getPatch());
      
      return new PatchEntityResult(input.getUrn(), entityName, true, null);
    } catch (Exception e) {
      return new PatchEntityResult(
          input.getUrn(), null, false, "Failed to apply patch: " + e.getMessage());
    }
  }

  private boolean isAuthorized(@Nonnull PatchEntityInput input, @Nonnull QueryContext context) {
    // For patch operations, we need EDIT_ENTITY_PRIVILEGE
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(
        ImmutableList.of(
            new ConjunctivePrivilegeGroup(
                ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()))));
    
    return AuthorizationUtils.isAuthorized(
        context,
        input.getEntityType(),
        input.getUrn() != null ? input.getUrn() : "urn:li:" + input.getEntityType() + ":new",
        orPrivilegeGroups);
  }

}
