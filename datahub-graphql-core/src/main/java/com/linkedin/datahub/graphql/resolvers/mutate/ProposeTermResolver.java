package com.linkedin.datahub.graphql.resolvers.mutate;

import com.datahub.metadata.authorization.AuthorizationManager;


import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.TermAssociationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
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

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final TermAssociationInput input = bindArgument(environment.getArgument("input"), TermAssociationInput.class);
    Urn termUrn = Urn.createFromString(input.getTermUrn());
    Urn targetUrn = Urn.createFromString(input.getResourceUrn());

    if (!ProposalUtils.isAuthorizedToProposeTerms(environment.getContext(), targetUrn, input.getSubResource())) {
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    return CompletableFuture.supplyAsync(() -> {
      LabelUtils.validateInput(
          termUrn,
          targetUrn,
          input.getSubResource(),
          input.getSubResourceType(),
          "glossaryTerm",
          _entityService,
          false
      );

      log.info("Proposing Term. input: {}", input.toString());
      try {
        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActor());
        return ProposalUtils.proposeTerm(
            actor,
            termUrn,
            targetUrn,
            input.getSubResource(),
            input.getSubResourceType(),
            _entityService,
            (AuthorizationManager) ((QueryContext) environment.getContext()).getAuthorizer()
        );
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input.toString()), e);
      }
    });
  }
}
