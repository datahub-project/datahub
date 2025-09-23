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
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchEntityResult;
import com.linkedin.datahub.graphql.resolvers.mutate.util.PatchResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.GenericAspect;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PatchEntitiesResolver
    implements DataFetcher<CompletableFuture<List<PatchEntityResult>>> {


  private final EntityService _entityService;
  private final EntityClient _entityClient;
  private final EntityRegistry _entityRegistry;

  @Override
  public CompletableFuture<List<PatchEntityResult>> get(DataFetchingEnvironment environment)
      throws Exception {
    final PatchEntityInput[] inputArray =
        bindArgument(environment.getArgument("input"), PatchEntityInput[].class);
    final List<PatchEntityInput> inputs = Arrays.asList(inputArray);
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return patchEntities(inputs, context);
          } catch (Exception e) {
            log.error("Failed to patch entities: {}", e.getMessage(), e);
            return inputs.stream()
                .map(input -> {
                  String urn = input.getUrn() != null ? input.getUrn() : "urn:li:unknown:error";
                  return new PatchEntityResult(urn, null, false, e.getMessage());
                })
                .collect(Collectors.toList());
          }
        },
        this.getClass().getSimpleName(),
        "patchEntities");
  }

  private List<PatchEntityResult> patchEntities(
      @Nonnull List<PatchEntityInput> inputs, @Nonnull QueryContext context) throws Exception {

    final Authentication authentication = context.getAuthentication();
    log.info("Processing batch patch for {} entities", inputs.size());
    
    // Resolve URNs for all inputs (either provided or auto-generated)
    final List<Urn> entityUrns = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      PatchEntityInput input = inputs.get(i);
      try {
        Urn entityUrn = PatchResolverUtils.resolveEntityUrn(input.getUrn(), input.getEntityType());
        entityUrns.add(entityUrn);
        log.debug("Resolved URN for input {}: {}", i, entityUrn);
      } catch (Exception e) {
        log.error("Failed to resolve URN for input {}: {}", i, e.getMessage(), e);
        throw new IllegalArgumentException("Failed to resolve URN for input " + i + ": " + e.getMessage(), e);
      }
    }

    // Check authorization for all entities using GraphQL authorization pattern
    for (int i = 0; i < inputs.size(); i++) {
      PatchEntityInput input = inputs.get(i);
      
      if (!isAuthorized(input, context)) {
        throw new com.linkedin.datahub.graphql.exception.AuthorizationException(
            authentication.getActor().toUrnStr() + " is unauthorized to update entity " + input.getUrn());
      }
    }

    // Create batch of MetadataChangeProposals
    final List<com.linkedin.mxe.MetadataChangeProposal> mcps = new ArrayList<>();

    for (int i = 0; i < inputs.size(); i++) {
      final PatchEntityInput input = inputs.get(i);
      final Urn entityUrn = entityUrns.get(i);

      try {
        // Validate aspect exists
        final var aspectSpec =
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

        // Always use patch aspect - same as single patchEntity resolver
        GenericAspect aspect = PatchResolverUtils.createPatchAspect(input.getPatch());

        // Create MetadataChangeProposal
        final com.linkedin.mxe.MetadataChangeProposal mcp =
            PatchResolverUtils.createMetadataChangeProposal(
                entityUrn,
                input.getAspectName(),
                aspect,
                input.getSystemMetadata(),
                input.getHeaders());

        mcps.add(mcp);
        
        log.debug("Created MCP for input {}: {}", i, entityUrn);
      } catch (Exception e) {
        log.error("Failed to create MCP for input {} ({}): {}", i, entityUrn, e.getMessage(), e);
        throw new RuntimeException("Failed to create MCP for input " + i + ": " + e.getMessage(), e);
      }
    }

    // Apply all patches using GraphQL standard batch approach
    try {
      Urn actor = UrnUtils.getUrn(authentication.getActor().toUrnStr());
      EntityUtils.ingestChangeProposals(
          context.getOperationContext(), 
          mcps, 
          _entityService, 
          actor, 
          false); // synchronous for GraphQL
      log.debug("Successfully applied batch patch for {} entities", mcps.size());
    } catch (Exception e) {
      log.error("Failed to apply batch patch for {} entities: {}", mcps.size(), e.getMessage(), e);
      throw new RuntimeException("Failed to apply batch patch: " + e.getMessage(), e);
    }

    // Map results back to input order
    final List<PatchEntityResult> patchResults = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      final PatchEntityInput input = inputs.get(i);
      
      // Extract entity name from the patch operations
      String entityName = PatchResolverUtils.extractEntityName(input.getPatch());
      
      // Validate name for glossary entities
      PatchResolverUtils.validateNameForEntityType(input.getEntityType(), entityName);

      // Since we used batch processing, all entities succeeded if we reached this point
      patchResults.add(
          new PatchEntityResult(
              input.getUrn(), entityName, true, null));
    }

    return patchResults;
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
