package com.linkedin.datahub.graphql.resolvers.proposal;

import com.datahub.authentication.proposal.ProposalService;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DescriptionUpdateInput;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class ProposeUpdateDescriptionResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final ProposalService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final DescriptionUpdateInput input = bindArgument(environment.getArgument("input"), DescriptionUpdateInput.class);
    final QueryContext context = environment.getContext();
    Urn resourceUrn = Urn.createFromString(input.getResourceUrn());
    String description = input.getDescription();
    String subresource = input.getSubResource();

    if (subresource != null) {
      throw new IllegalArgumentException("Proposing an update to a column description is currently not supported");
    }

    Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
    String entityType = resourceUrn.getEntityType();

    log.info("Proposing a description update. input: {}", input);
    return CompletableFuture.supplyAsync(() -> {
      try {
        switch (entityType) {
          case Constants.GLOSSARY_TERM_ENTITY_NAME:
          case Constants.GLOSSARY_NODE_ENTITY_NAME:
            return _proposalService.proposeUpdateResourceDescription(actor, resourceUrn, description,
                context.getAuthorizer());
          default:
            log.warn(String.format("Proposing an update to a description is currently not supported for entity type %s",
                entityType));
            return false;
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to update description", e);
      }
    });
  }
}
