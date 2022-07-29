package com.linkedin.datahub.graphql.resolvers.proposal;

import com.datahub.authentication.proposal.ProposalService;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class RejectProposalResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;
  private final ProposalService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final Urn proposalUrn = Urn.createFromString(bindArgument(environment.getArgument("urn"), String.class));
    QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {
      if (!proposalUrn.getEntityType().equals("actionRequest")) {
        throw new RuntimeException(
            String.format("Failed to reject proposal, Urn provided (%s) is not a valid actionRequest urn",
                proposalUrn));
      }
      if (!_entityService.exists(proposalUrn)) {
        throw new RuntimeException(
            String.format("Failed to reject proposal, proposal provided (%s) does not exist", proposalUrn));
      }

      Entity proposalEntity = _entityService.getEntity(proposalUrn, new HashSet<>());
      ActionRequestSnapshot actionRequestSnapshot = proposalEntity.getValue().getActionRequestSnapshot();
      ActionRequest proposal =
          ActionRequestUtils.mapActionRequest(proposalEntity.getValue().getActionRequestSnapshot());
      ActionRequestType actionRequestType = proposal.getType();
      boolean isTagOrTermProposal =
          actionRequestType.equals(ActionRequestType.TAG_ASSOCIATION) || actionRequestType.equals(
              ActionRequestType.TERM_ASSOCIATION);

      try {
        log.info("Rejecting term proposal. Proposal urn: {}", proposalUrn);
        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActorUrn());
        String subResource = proposal.getSubResource();

        if (isTagOrTermProposal && !ProposalUtils.isAuthorizedToAcceptProposal(environment.getContext(),
            proposal.getType(), Urn.createFromString(proposal.getEntity().getUrn()), subResource)) {
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
          Urn tagUrn = Urn.createFromString(proposal.getParams().getTagProposal().getTag().getUrn());
          ProposalUtils.deleteTagFromEntityOrSchemaProposalsAspect(actor, tagUrn, targetUrn, subResource,
              _entityService);
        } else if (proposal.getType().equals(ActionRequestType.TERM_ASSOCIATION)) {
          Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
          Urn termUrn = Urn.createFromString(proposal.getParams().getGlossaryTermProposal().getGlossaryTerm().getUrn());
          ProposalUtils.deleteTermFromEntityOrSchemaProposalsAspect(actor, termUrn, targetUrn, subResource,
              _entityService);
        } else if (proposal.getType().equals(ActionRequestType.CREATE_GLOSSARY_NODE)) {
          boolean canManageGlossaries = AuthorizationUtils.canManageGlossaries(context);
          if (!_proposalService.canResolveGlossaryNodeProposal(actor, actionRequestSnapshot, canManageGlossaries)) {
            throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
        } else if (proposal.getType().equals(ActionRequestType.CREATE_GLOSSARY_TERM)) {
          boolean canManageGlossaries = AuthorizationUtils.canManageGlossaries(context);
          if (!_proposalService.canResolveGlossaryTermProposal(actor, actionRequestSnapshot, canManageGlossaries)) {
            throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
        }

        _proposalService.completeProposal(actor, ActionRequestStatus.COMPLETED.toString(),
            ActionRequestResult.REJECTED.toString(), proposalEntity);

        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", proposalUrn, e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", proposalUrn), e);
      }
    });
  }
}
