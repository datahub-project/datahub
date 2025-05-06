package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RejectProposalResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;
  private final ActionRequestService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final Urn proposalUrn =
        Urn.createFromString(bindArgument(environment.getArgument("urn"), String.class));
    final String maybeNote = environment.getArgument("note");
    QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!proposalUrn.getEntityType().equals("actionRequest")) {
            throw new RuntimeException(
                String.format(
                    "Failed to reject proposal, Urn provided (%s) is not a valid actionRequest urn",
                    proposalUrn));
          }
          if (!_entityService.exists(context.getOperationContext(), proposalUrn, true)) {
            throw new RuntimeException(
                String.format(
                    "Failed to reject proposal, proposal provided (%s) does not exist",
                    proposalUrn));
          }

          Entity proposalEntity =
              _entityService.getEntity(
                  context.getOperationContext(), proposalUrn, new HashSet<>(), true);
          ActionRequestSnapshot actionRequestSnapshot =
              proposalEntity.getValue().getActionRequestSnapshot();
          ActionRequest proposal =
              ActionRequestUtils.mapActionRequest(
                  context, proposalEntity.getValue().getActionRequestSnapshot());
          ActionRequestType actionRequestType = proposal.getType();
          boolean isTagOrTermProposal =
              actionRequestType.equals(ActionRequestType.TAG_ASSOCIATION)
                  || actionRequestType.equals(ActionRequestType.TERM_ASSOCIATION);

          try {
            log.info("Rejecting term proposal. Proposal urn: {}", proposalUrn);
            Urn actor =
                CorpuserUrn.createFromString(
                    ((QueryContext) environment.getContext()).getActorUrn());
            String subResource = proposal.getSubResource();

            if (isTagOrTermProposal
                && !ProposalUtils.isAuthorizedToAcceptProposal(
                    environment.getContext(),
                    proposal.getType(),
                    Urn.createFromString(proposal.getEntity().getUrn()),
                    subResource)) {
              throw new AuthorizationException(
                  "Unauthorized to perform this action. Please contact your DataHub administrator.");
            }

            if (!proposal.getStatus().equals(ActionRequestStatus.PENDING)) {
              log.error("Cannot reject proposal- proposal has already been completed");
              return false;
            }

            // Special cleanup required for Tag and Term Association Proposals
            if (proposal.getType().equals(ActionRequestType.TAG_ASSOCIATION)) {
              Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
              List<Urn> tagUrns =
                  proposal.getParams().getTagProposal().getTags() != null
                          && !proposal.getParams().getTagProposal().getTags().isEmpty()
                      ? proposal.getParams().getTagProposal().getTags().stream()
                          .map(tag -> UrnUtils.getUrn(tag.getUrn()))
                          .collect(Collectors.toList())
                      : ImmutableList.of(
                          Urn.createFromString(
                              proposal.getParams().getTagProposal().getTag().getUrn()));
              ProposalUtils.deleteTagFromEntityOrSchemaProposalsAspect(
                  context.getOperationContext(),
                  actor,
                  tagUrns,
                  targetUrn,
                  subResource,
                  _entityService);
            } else if (proposal.getType().equals(ActionRequestType.TERM_ASSOCIATION)) {
              Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
              List<Urn> termUrns =
                  proposal.getParams().getGlossaryTermProposal().getGlossaryTerms() != null
                          && !proposal
                              .getParams()
                              .getGlossaryTermProposal()
                              .getGlossaryTerms()
                              .isEmpty()
                      ? proposal.getParams().getGlossaryTermProposal().getGlossaryTerms().stream()
                          .map(tag -> UrnUtils.getUrn(tag.getUrn()))
                          .collect(Collectors.toList())
                      : ImmutableList.of(
                          Urn.createFromString(
                              proposal
                                  .getParams()
                                  .getGlossaryTermProposal()
                                  .getGlossaryTerm()
                                  .getUrn()));
              ProposalUtils.deleteTermFromEntityOrSchemaProposalsAspect(
                  context.getOperationContext(),
                  actor,
                  termUrns,
                  targetUrn,
                  subResource,
                  _entityService);
            } else if (proposal.getType().equals(ActionRequestType.CREATE_GLOSSARY_NODE)) {
              boolean canManageGlossaries = GlossaryUtils.canManageGlossaries(context);
              if (!_proposalService.canResolveGlossaryNodeProposal(
                  context.getOperationContext(),
                  actor,
                  actionRequestSnapshot,
                  canManageGlossaries)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
            } else if (proposal.getType().equals(ActionRequestType.CREATE_GLOSSARY_TERM)) {
              boolean canManageGlossaries = GlossaryUtils.canManageGlossaries(context);
              if (!_proposalService.canResolveGlossaryTermProposal(
                  context.getOperationContext(),
                  actor,
                  actionRequestSnapshot,
                  canManageGlossaries)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
            }

            _proposalService.completeProposal(
                context.getOperationContext(),
                actor,
                ActionRequestStatus.COMPLETED.toString(),
                ActionRequestResult.REJECTED.toString(),
                maybeNote,
                proposalEntity);

            return true;
          } catch (Exception e) {
            log.error("Failed to perform update against input {}, {}", proposalUrn, e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", proposalUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
