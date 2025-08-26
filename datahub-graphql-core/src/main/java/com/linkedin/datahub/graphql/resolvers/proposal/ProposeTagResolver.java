package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.TagAssociationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ProposeTagResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService<?> _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final TagAssociationInput input =
        bindArgument(environment.getArgument("input"), TagAssociationInput.class);
    Urn tagUrn = Urn.createFromString(input.getTagUrn());
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    if (!ProposalUtils.isAuthorizedToProposeTags(context, targetUrn, input.getSubResource())) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          LabelUtils.validateResourceAndLabel(
              context.getOperationContext(),
              tagUrn,
              targetUrn,
              input.getSubResource(),
              input.getSubResourceType(),
              Constants.TAG_ENTITY_NAME,
              _entityService,
              false);

          if (ProposalUtils.isTagAlreadyAttachedToTarget(
              context.getOperationContext(),
              tagUrn,
              targetUrn,
              input.getSubResource(),
              _entityService)) {
            throw new DataHubGraphQLException(
                "Tag has already been applied to target", DataHubGraphQLErrorCode.BAD_REQUEST);
          }

          if (ProposalUtils.isTagAlreadyProposedToTarget(
              context.getOperationContext(),
              tagUrn,
              targetUrn,
              input.getSubResource(),
              _entityClient)) {
            throw new DataHubGraphQLException(
                "Tag has already been proposed to target", DataHubGraphQLErrorCode.BAD_REQUEST);
          }

          try {
            log.info("Proposing Tag. input: {}", input.toString());
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());

            ProposalUtils.proposeTag(
                context.getOperationContext(),
                actor,
                tagUrn,
                targetUrn,
                input.getSubResource(),
                input.getSubResourceType(),
                _entityService);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input.toString()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
