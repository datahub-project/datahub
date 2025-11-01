package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
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
import com.linkedin.mxe.MetadataChangeProposal;
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

  private final EntityService entityService;
  private final EntityClient entityClient;
  private final EntityRegistry entityRegistry;

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
                .map(
                    input -> {
                      // Try to resolve URN for error response, fallback to placeholder
                      String urn;
                      try {
                        urn =
                            PatchResolverUtils.resolveEntityUrn(
                                    input.getUrn(), input.getEntityType())
                                .toString();
                      } catch (Exception urnException) {
                        urn = input.getUrn() != null ? input.getUrn() : "urn:li:unknown:error";
                      }
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

    // Check authorization for all entities using common utility method
    PatchResolverUtils.checkBatchAuthorization(inputs, context);

    // Create batch of MetadataChangeProposals using common utility method
    final List<MetadataChangeProposal> mcps =
        PatchResolverUtils.createPatchEntitiesMcps(inputs, context, entityRegistry);

    // Apply all patches using GraphQL standard batch approach
    try {
      Urn actor = UrnUtils.getUrn(authentication.getActor().toUrnStr());
      EntityUtils.ingestChangeProposals(
          context.getOperationContext(),
          mcps,
          entityService,
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
      patchResults.add(new PatchEntityResult(input.getUrn(), entityName, true, null));
    }

    return patchResults;
  }
}
