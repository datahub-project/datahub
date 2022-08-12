package com.linkedin.datahub.graphql.resolvers.proposal;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.proposal.ProposalService;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
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
public class AcceptProposalResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService _entityService;
  private final ProposalService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final Urn proposalUrn = Urn.createFromString(bindArgument(environment.getArgument("urn"), String.class));
    QueryContext context = environment.getContext();
    Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(() -> {
      if (!proposalUrn.getEntityType().equals("actionRequest")) {
        throw new RuntimeException(
            String.format("Failed to accept proposal, Urn provided (%s) is not a valid actionRequest urn",
                proposalUrn));
      }
      if (!_entityService.exists(proposalUrn)) {
        throw new RuntimeException(
            String.format("Failed to accept proposal, proposal provided (%s) does not exist", proposalUrn));
      }

      Entity proposalEntity = _entityService.getEntity(proposalUrn, new HashSet<>());
      ActionRequestSnapshot actionRequestSnapshot = proposalEntity.getValue().getActionRequestSnapshot();
      ActionRequest proposal =
          ActionRequestUtils.mapActionRequest(actionRequestSnapshot);

      try {
        log.info("Accepting term proposal. Proposal urn: {}", proposalUrn);
        Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
        String subResource = proposal.getSubResource();
        ActionRequestType actionRequestType = proposal.getType();
        boolean canManageGlossaries = AuthorizationUtils.canManageGlossaries(context);

        if (!proposal.getStatus().equals(ActionRequestStatus.PENDING)) {
          log.error("Cannot accept proposal- proposal has already been completed");
          return false;
        }

        if (proposal.getType().equals(ActionRequestType.TAG_ASSOCIATION)) {
          if (!ProposalUtils.isAuthorizedToAcceptProposal(context, actionRequestType,
              Urn.createFromString(proposal.getEntity().getUrn()), subResource)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          Urn tagUrn = Urn.createFromString(proposal.getParams().getTagProposal().getTag().getUrn());
          Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
          LabelUtils.addTagsToResources(
              ImmutableList.of(tagUrn),
              ImmutableList.of(new ResourceRefInput(
                  targetUrn.toString(),
                  proposal.getSubResourceType() == null ? null : SubResourceType.valueOf(proposal.getSubResourceType()),
                  proposal.getSubResource())),
              actor,
              _entityService);
          ProposalUtils.deleteTagFromEntityOrSchemaProposalsAspect(actor, tagUrn, targetUrn, subResource,
              _entityService);
        } else if (proposal.getType().equals(ActionRequestType.TERM_ASSOCIATION)) {
          if (!ProposalUtils.isAuthorizedToAcceptProposal(context, actionRequestType,
              Urn.createFromString(proposal.getEntity().getUrn()), subResource)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          Urn termUrn = Urn.createFromString(proposal.getParams().getGlossaryTermProposal().getGlossaryTerm().getUrn());
          Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
          LabelUtils.addTermsToResources(
              ImmutableList.of(termUrn),
              ImmutableList.of(new ResourceRefInput(
                  targetUrn.toString(),
                  proposal.getSubResourceType() == null ? null : SubResourceType.valueOf(proposal.getSubResourceType()),
                  proposal.getSubResource())),              actor,
              _entityService);
          ProposalUtils.deleteTermFromEntityOrSchemaProposalsAspect(actor, termUrn, targetUrn, subResource,
              _entityService);
        } else if (proposal.getType().equals(ActionRequestType.CREATE_GLOSSARY_NODE)) {
          if (!_proposalService.canResolveGlossaryNodeProposal(actor, actionRequestSnapshot, canManageGlossaries)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          _proposalService.acceptCreateGlossaryNodeProposal(actor, actionRequestSnapshot, canManageGlossaries,
              authentication);
        } else if (proposal.getType().equals(ActionRequestType.CREATE_GLOSSARY_TERM)) {
          if (!_proposalService.canResolveGlossaryTermProposal(actor, actionRequestSnapshot, canManageGlossaries)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          _proposalService.acceptCreateGlossaryTermProposal(actor, actionRequestSnapshot, canManageGlossaries,
              authentication);
        } else if (proposal.getType().equals(ActionRequestType.UPDATE_DESCRIPTION)) {
          _proposalService.acceptUpdateResourceDescriptionProposal(actionRequestSnapshot, authentication);
        } else {
          log.error("Cannot accept proposal- proposal is not acceptable");
          return false;
        }

        _proposalService.completeProposal(actor, ActionRequestStatus.COMPLETED.toString(),
            ActionRequestResult.ACCEPTED.toString(), proposalEntity);

        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", proposalUrn, e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", proposalUrn), e);
      }
    });
  }
}
