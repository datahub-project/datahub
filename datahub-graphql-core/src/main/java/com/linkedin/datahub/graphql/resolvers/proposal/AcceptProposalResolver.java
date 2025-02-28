package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
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

/**
 * This resolver is deprecated! Use {@link
 * com.linkedin.datahub.graphql.resolvers.proposal.AcceptProposalsResolver} instead.
 */
@Slf4j
@RequiredArgsConstructor
public class AcceptProposalResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityService<?> _entityService;
  private final ActionRequestService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final Urn proposalUrn =
        Urn.createFromString(bindArgument(environment.getArgument("urn"), String.class));
    final String maybeNote = environment.getArgument("note");
    QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          if (!proposalUrn.getEntityType().equals("actionRequest")) {
            throw new RuntimeException(
                String.format(
                    "Failed to accept proposal, Urn provided (%s) is not a valid actionRequest urn",
                    proposalUrn));
          }
          if (!_entityService.exists(context.getOperationContext(), proposalUrn, true)) {
            throw new RuntimeException(
                String.format(
                    "Failed to accept proposal, proposal provided (%s) does not exist",
                    proposalUrn));
          }

          Entity proposalEntity =
              _entityService.getEntity(
                  context.getOperationContext(), proposalUrn, new HashSet<>(), true);
          ActionRequestSnapshot actionRequestSnapshot =
              proposalEntity.getValue().getActionRequestSnapshot();
          ActionRequest proposal =
              ActionRequestUtils.mapActionRequest(context, actionRequestSnapshot);

          try {
            log.info("Accepting term proposal. Proposal urn: {}", proposalUrn);
            Urn actor = CorpuserUrn.createFromString(context.getActorUrn());
            String subResource = proposal.getSubResource();
            ActionRequestType actionRequestType = proposal.getType();
            boolean canManageGlossaries = GlossaryUtils.canManageGlossaries(context);

            if (!proposal.getStatus().equals(ActionRequestStatus.PENDING)) {
              log.error("Cannot accept proposal- proposal has already been completed");
              return false;
            }

            if (proposal.getType().equals(ActionRequestType.TAG_ASSOCIATION)) {
              if (!ProposalUtils.isAuthorizedToAcceptProposal(
                  context,
                  actionRequestType,
                  Urn.createFromString(proposal.getEntity().getUrn()),
                  subResource)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
              List<Urn> tagUrns =
                  proposal.getParams().getTagProposal().getTags() != null
                          && !proposal.getParams().getTagProposal().getTags().isEmpty()
                      ? proposal.getParams().getTagProposal().getTags().stream()
                          .map(tag -> UrnUtils.getUrn(tag.getUrn()))
                          .collect(Collectors.toList())
                      : ImmutableList.of(
                          Urn.createFromString(
                              proposal.getParams().getTagProposal().getTag().getUrn()));
              Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
              LabelUtils.addTagsToResources(
                  context.getOperationContext(),
                  tagUrns,
                  ImmutableList.of(
                      new ResourceRefInput(
                          targetUrn.toString(),
                          proposal.getSubResourceType() == null
                              ? null
                              : SubResourceType.valueOf(proposal.getSubResourceType()),
                          proposal.getSubResource())),
                  actor,
                  _entityService);
              ProposalUtils.deleteTagFromEntityOrSchemaProposalsAspect(
                  context.getOperationContext(),
                  actor,
                  tagUrns,
                  targetUrn,
                  subResource,
                  _entityService);
            } else if (proposal.getType().equals(ActionRequestType.TERM_ASSOCIATION)) {
              if (!ProposalUtils.isAuthorizedToAcceptProposal(
                  context,
                  actionRequestType,
                  Urn.createFromString(proposal.getEntity().getUrn()),
                  subResource)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
              List<Urn> termUrns =
                  proposal.getParams().getGlossaryTermProposal().getGlossaryTerms() != null
                          && !proposal
                              .getParams()
                              .getGlossaryTermProposal()
                              .getGlossaryTerms()
                              .isEmpty()
                      ? proposal.getParams().getGlossaryTermProposal().getGlossaryTerms().stream()
                          .map(term -> UrnUtils.getUrn(term.getUrn()))
                          .collect(Collectors.toList())
                      : ImmutableList.of(
                          Urn.createFromString(
                              proposal
                                  .getParams()
                                  .getGlossaryTermProposal()
                                  .getGlossaryTerm()
                                  .getUrn()));
              Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
              LabelUtils.addTermsToResources(
                  context.getOperationContext(),
                  termUrns,
                  ImmutableList.of(
                      new ResourceRefInput(
                          targetUrn.toString(),
                          proposal.getSubResourceType() == null
                              ? null
                              : SubResourceType.valueOf(proposal.getSubResourceType()),
                          proposal.getSubResource())),
                  actor,
                  _entityService);
              ProposalUtils.deleteTermFromEntityOrSchemaProposalsAspect(
                  context.getOperationContext(),
                  actor,
                  termUrns,
                  targetUrn,
                  subResource,
                  _entityService);
            } else if (proposal.getType().equals(ActionRequestType.CREATE_GLOSSARY_NODE)) {
              if (!_proposalService.canResolveGlossaryNodeProposal(
                  context.getOperationContext(),
                  actor,
                  actionRequestSnapshot,
                  canManageGlossaries)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }

              _proposalService.acceptCreateGlossaryNodeProposal(
                  context.getOperationContext(), actor, actionRequestSnapshot, canManageGlossaries);
            } else if (proposal.getType().equals(ActionRequestType.CREATE_GLOSSARY_TERM)) {
              if (!_proposalService.canResolveGlossaryTermProposal(
                  context.getOperationContext(),
                  actor,
                  actionRequestSnapshot,
                  canManageGlossaries)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }

              _proposalService.acceptCreateGlossaryTermProposal(
                  context.getOperationContext(), actor, actionRequestSnapshot, canManageGlossaries);
            } else if (proposal.getType().equals(ActionRequestType.UPDATE_DESCRIPTION)) {
              if (!ProposalUtils.isAuthorizedToAcceptProposal(
                  context,
                  actionRequestType,
                  Urn.createFromString(proposal.getEntity().getUrn()),
                  subResource)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
              _proposalService.acceptUpdateResourceDescriptionProposal(
                  context.getOperationContext(), actionRequestSnapshot);
            } else if (proposal.getType().equals(ActionRequestType.DATA_CONTRACT)) {
              if (!ProposalUtils.isAuthorizedToAcceptProposal(
                  context,
                  actionRequestType,
                  Urn.createFromString(proposal.getEntity().getUrn()),
                  subResource)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
              _proposalService.acceptDataContractProposal(
                  context.getOperationContext(), actionRequestSnapshot);
            } else if (proposal
                .getType()
                .equals(ActionRequestType.STRUCTURED_PROPERTY_ASSOCIATION)) {
              if (!ProposalUtils.isAuthorizedToAcceptProposal(
                  context,
                  actionRequestType,
                  Urn.createFromString(proposal.getEntity().getUrn()),
                  subResource)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
              _proposalService.acceptStructuredPropertyProposal(
                  context.getOperationContext(),
                  ActionRequestService.findActionRequestInfoAspect(actionRequestSnapshot));
            } else if (proposal.getType().equals(ActionRequestType.DOMAIN_ASSOCIATION)) {
              if (!ProposalUtils.isAuthorizedToAcceptProposal(
                  context,
                  actionRequestType,
                  Urn.createFromString(proposal.getEntity().getUrn()),
                  subResource)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
              _proposalService.acceptDomainProposal(
                  context.getOperationContext(),
                  ActionRequestService.findActionRequestInfoAspect(actionRequestSnapshot));
            } else if (proposal.getType().equals(ActionRequestType.OWNER_ASSOCIATION)) {
              if (!ProposalUtils.isAuthorizedToAcceptProposal(
                  context,
                  actionRequestType,
                  Urn.createFromString(proposal.getEntity().getUrn()),
                  subResource)) {
                throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
              }
              _proposalService.acceptOwnerProposal(
                  context.getOperationContext(),
                  ActionRequestService.findActionRequestInfoAspect(actionRequestSnapshot));
            } else {
              log.error("Cannot accept proposal- proposal is not acceptable");
              return false;
            }

            _proposalService.completeProposal(
                context.getOperationContext(),
                actor,
                ActionRequestStatus.COMPLETED.toString(),
                ActionRequestResult.ACCEPTED.toString(),
                maybeNote,
                proposalEntity);

            return true;
          } catch (Exception e) {
            log.error("Failed to perform update against input {}, {}", proposalUrn, e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", proposalUrn), e);
          }
        });
  }
}
