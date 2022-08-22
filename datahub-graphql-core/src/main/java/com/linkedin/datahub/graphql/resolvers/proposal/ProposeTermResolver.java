package com.linkedin.datahub.graphql.resolvers.proposal;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.TermAssociationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class ProposeTermResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final TermAssociationInput input = bindArgument(environment.getArgument("input"), TermAssociationInput.class);
    Urn termUrn = Urn.createFromString(input.getTermUrn());
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    if (!ProposalUtils.isAuthorizedToProposeTerms(environment.getContext(), targetUrn, input.getSubResource())) {
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(() -> {
      LabelUtils.validateResourceAndLabel(
          termUrn,
          targetUrn,
          input.getSubResource(),
          input.getSubResourceType(),
          Constants.GLOSSARY_TERM_ENTITY_NAME,
          _entityService,
          false
      );

      if (ProposalUtils.isTermAlreadyAttachedToTarget(termUrn, targetUrn, input.getSubResource(), _entityService)) {
        throw new DataHubGraphQLException("Term has already been applied to target", DataHubGraphQLErrorCode.BAD_REQUEST);
      }

      if (ProposalUtils.isTermAlreadyProposedToTarget(termUrn, targetUrn, input.getSubResource(), _entityClient,
          ((QueryContext) environment.getContext()).getAuthentication()
      )) {
        throw new DataHubGraphQLException("Term has already been proposed to target", DataHubGraphQLErrorCode.BAD_REQUEST);
      }

      log.info("Proposing Term. input: {}", input.toString());
      try {
        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActorUrn());
        return ProposalUtils.proposeTerm(
            actor,
            termUrn,
            targetUrn,
            input.getSubResource(),
            input.getSubResourceType(),
            _entityService,
            ((QueryContext) environment.getContext()).getAuthorizer()
        );
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }
}
