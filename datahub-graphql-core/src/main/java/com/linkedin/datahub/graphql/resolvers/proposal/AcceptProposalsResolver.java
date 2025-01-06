package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_PENDING;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_DATA_CONTRACT_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL;

import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// todo: push bulk proposal acceptance into ActionRequestService.java layer as much as possible.
// this is way too spread out across multiple classes. (ProposalUtils, AcceptProposalsResolver,
// ProposalService, etc)
@Slf4j
@RequiredArgsConstructor
public class AcceptProposalsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> _entityService;
  private final ActionRequestService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final List<String> proposalUrnStrs = bindArgument(environment.getArgument("urns"), List.class);
    final Set<Urn> proposalUrns =
        proposalUrnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());
    final String maybeNote = (String) environment.getArgument("note");
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          // First, validate all proposals.
          ProposalUtils.validateProposalUrns(proposalUrns);

          // Then, accept the proposals
          acceptProposals(proposalUrns, maybeNote, context);

          // Return true if all are successful.
          return true;
        });
  }

  private void acceptProposals(
      @Nonnull final Set<Urn> proposalUrns,
      @Nullable final String note,
      @Nonnull final QueryContext context) {
    final Map<Urn, Entity> resolvedProposalEntities =
        _entityService.getEntities(
            context.getOperationContext(), proposalUrns, new HashSet<>(), true);

    // Validate each proposal entity
    // Yes, passing _proposalService here is gross. Ideally proposal service should handle all of
    // this.
    ProposalUtils.validateProposals(resolvedProposalEntities.values(), context, _proposalService);

    // Finally, accept each proposal.
    for (final Urn proposalUrn : proposalUrns) {
      if (resolvedProposalEntities.get(proposalUrn) == null) {
        throw new RuntimeException(
            String.format(
                "Failed to accept proposal. Propose with urn %s was not found.", proposalUrn));
      }
      acceptProposal(resolvedProposalEntities.get(proposalUrn), note, context);
    }
  }

  private void acceptProposal(
      @Nonnull final Entity proposalEntity,
      @Nullable final String note,
      @Nonnull final QueryContext context) {

    // TODO: Migrate away from using deprecated 'snapshot' entities here.
    final ActionRequestSnapshot actionRequestSnapshot =
        proposalEntity.getValue().getActionRequestSnapshot();
    final ActionRequestInfo actionRequestInfo =
        ProposalUtils.extractActionRequestInfo(actionRequestSnapshot);
    final ActionRequestStatus actionRequestStatus =
        ProposalUtils.extractActionRequestStatus(actionRequestSnapshot);

    if (!actionRequestStatus.getStatus().equals(ACTION_REQUEST_STATUS_PENDING)) {
      // Already accepted.
      return;
    }

    try {
      final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());

      switch (actionRequestInfo.getType()) {
        case ACTION_REQUEST_TYPE_TAG_PROPOSAL:
          acceptTagProposal(actionRequestInfo, context);
          break;
        case ACTION_REQUEST_TYPE_TERM_PROPOSAL:
          acceptTermProposal(actionRequestInfo, context);
          break;
        case ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL:
          acceptDescriptionProposal(actionRequestSnapshot, context);
          break;
        case ACTION_REQUEST_TYPE_DATA_CONTRACT_PROPOSAL:
          acceptDataContractProposal(actionRequestSnapshot, context);
          break;
        case ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL:
          acceptGlossaryNodeProposal(actionRequestSnapshot, context);
          break;
        case ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL:
          acceptGlossaryTermProposal(actionRequestSnapshot, context);
          break;
        case ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL:
          acceptStructuredPropertyProposal(actionRequestSnapshot, context);
        default:
          log.warn(
              String.format(
                  "Unrecognized action request type %s provided. Unable to apply proposal, skipping!",
                  actionRequestInfo.getType()));
      }

      _proposalService.completeProposal(
          context.getOperationContext(),
          actorUrn,
          ACTION_REQUEST_STATUS_COMPLETE,
          ACTION_REQUEST_RESULT_ACCEPTED,
          note,
          proposalEntity);

    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to accept proposal of type %s. Cause:", actionRequestInfo.getType()),
          e);
    }
  }

  private void acceptTagProposal(
      @Nonnull final ActionRequestInfo actionRequestInfo, @Nonnull final QueryContext context)
      throws Exception {

    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    final List<Urn> tagUrns =
        actionRequestInfo.getParams().getTagProposal().getTags() != null
                && !actionRequestInfo.getParams().getTagProposal().getTags().isEmpty()
            ? actionRequestInfo.getParams().getTagProposal().getTags()
            : ImmutableList.of(actionRequestInfo.getParams().getTagProposal().getTag());
    final Urn targetUrn = UrnUtils.getUrn(actionRequestInfo.getResource());

    LabelUtils.addTagsToResources(
        context.getOperationContext(),
        tagUrns,
        ImmutableList.of(
            new ResourceRefInput(
                targetUrn.toString(),
                actionRequestInfo.getSubResourceType() == null
                    ? null
                    : SubResourceType.valueOf(actionRequestInfo.getSubResourceType()),
                actionRequestInfo.getSubResource())),
        actorUrn,
        _entityService);

    // Remove from the Schema Proposals aspect which is used for search indexing. Important!
    ProposalUtils.deleteTagFromEntityOrSchemaProposalsAspect(
        context.getOperationContext(),
        actorUrn,
        tagUrns,
        targetUrn,
        actionRequestInfo.getSubResource(),
        _entityService);
  }

  private void acceptTermProposal(
      @Nonnull final ActionRequestInfo actionRequestInfo, @Nonnull final QueryContext context)
      throws Exception {

    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    final List<Urn> termUrns =
        actionRequestInfo.getParams().getGlossaryTermProposal().getGlossaryTerms() != null
                && !actionRequestInfo
                    .getParams()
                    .getGlossaryTermProposal()
                    .getGlossaryTerms()
                    .isEmpty()
            ? actionRequestInfo.getParams().getGlossaryTermProposal().getGlossaryTerms()
            : ImmutableList.of(
                actionRequestInfo.getParams().getGlossaryTermProposal().getGlossaryTerm());
    final Urn targetUrn = UrnUtils.getUrn(actionRequestInfo.getResource());

    LabelUtils.addTermsToResources(
        context.getOperationContext(),
        termUrns,
        ImmutableList.of(
            new ResourceRefInput(
                targetUrn.toString(),
                actionRequestInfo.getSubResourceType() == null
                    ? null
                    : SubResourceType.valueOf(actionRequestInfo.getSubResourceType()),
                actionRequestInfo.getSubResource())),
        actorUrn,
        _entityService);

    // Remove from the Schema Proposals aspect which is used for search indexing. Important!
    ProposalUtils.deleteTermFromEntityOrSchemaProposalsAspect(
        context.getOperationContext(),
        actorUrn,
        termUrns,
        targetUrn,
        actionRequestInfo.getSubResource(),
        _entityService);
  }

  private void acceptDescriptionProposal(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      @Nonnull final QueryContext context)
      throws Exception {
    _proposalService.acceptUpdateResourceDescriptionProposal(
        context.getOperationContext(), actionRequestSnapshot);
  }

  private void acceptGlossaryNodeProposal(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      @Nonnull final QueryContext context)
      throws Exception {
    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    _proposalService.acceptCreateGlossaryNodeProposal(
        context.getOperationContext(),
        actorUrn,
        actionRequestSnapshot,
        GlossaryUtils.canManageGlossaries(context));
  }

  private void acceptGlossaryTermProposal(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      @Nonnull final QueryContext context)
      throws Exception {
    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    _proposalService.acceptCreateGlossaryTermProposal(
        context.getOperationContext(),
        actorUrn,
        actionRequestSnapshot,
        GlossaryUtils.canManageGlossaries(context));
  }

  private void acceptDataContractProposal(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      @Nonnull final QueryContext context)
      throws Exception {
    _proposalService.acceptDataContractProposal(
        context.getOperationContext(), actionRequestSnapshot);
  }

  private void acceptStructuredPropertyProposal(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      @Nonnull final QueryContext context)
      throws Exception {
    _proposalService.acceptStructuredPropertyProposal(
        context.getOperationContext(),
        ActionRequestService.findActionRequestInfoAspect(actionRequestSnapshot));
  }
}
