package com.linkedin.datahub.graphql.resolvers.proposal;

import com.datahub.authentication.proposal.ProposalService;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityInput;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class ProposeCreateGlossaryNodeResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final ProposalService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final CreateGlossaryEntityInput input =
        bindArgument(environment.getArgument("input"), CreateGlossaryEntityInput.class);
    final QueryContext context = environment.getContext();
    String name = input.getName();
    String parentNodeUrnStr = input.getParentNode();

    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Name cannot be null or the empty string");
    }

    Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
    Optional<Urn> parentNode =
        parentNodeUrnStr == null ? Optional.empty() : Optional.of(GlossaryNodeUrn.createFromString(parentNodeUrnStr));

    return CompletableFuture.supplyAsync(() -> {
      try {
        log.info("Proposing Creation of Glossary Node. input: {}", input);

        return _proposalService.proposeCreateGlossaryNode(actor, name, parentNode, context.getAuthorizer());
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", input, e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", input), e);
      }
    });
  }
}
