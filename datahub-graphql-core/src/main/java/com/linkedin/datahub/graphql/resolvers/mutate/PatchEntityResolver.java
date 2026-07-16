package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.PatchEntityInput;
import com.linkedin.datahub.graphql.generated.PatchEntityResult;
import com.linkedin.datahub.graphql.resolvers.mutate.util.PatchResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.MetadataChangeProposal;
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

  private final EntityService entityService;
  private final EntityClient entityClient;
  private final EntityRegistry entityRegistry;

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

    final Authentication authentication = context.getAuthentication();

    // Check authorization using common utility method
    if (!AuthorizationUtils.isAuthorizedForPatch(input, context)) {
      throw new AuthorizationException(
          authentication.getActor().toUrnStr() + " is unauthorized to update entities.");
    }

    // Create MCPs using common utility method
    final List<PatchEntityInput> inputs = List.of(input);
    final List<MetadataChangeProposal> mcps =
        PatchResolverUtils.createPatchEntitiesMcps(inputs, context, entityRegistry);

    // Apply the patch
    try {
      Urn actor = UrnUtils.getUrn(authentication.getActor().toUrnStr());
      IngestResult result =
          entityService.ingestProposal(
              context.getOperationContext(),
              mcps.get(0),
              AuditStampUtils.createAuditStamp(actor.toString()),
              false); // synchronous for GraphQL

      if (result == null) {
        return new PatchEntityResult(
            input.getUrn(), null, false, "Failed to apply patch: ingestProposal returned null");
      }

      // Extract entity name from the patch operations
      String entityName = PatchResolverUtils.extractEntityName(input.getPatch());

      return new PatchEntityResult(input.getUrn(), entityName, true, null);
    } catch (Exception e) {
      return new PatchEntityResult(
          input.getUrn(), null, false, "Failed to apply patch: " + e.getMessage());
    }
  }
}
