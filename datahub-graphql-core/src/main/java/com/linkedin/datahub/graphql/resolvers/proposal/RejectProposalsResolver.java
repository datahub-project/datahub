package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_REJECTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_PENDING;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL;

import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
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

// todo: push bulk proposal rejecting into ProposalService.java layer as much as possible.
// this is way too spread out across multiple classes. (ProposalUtils, AcceptProposalsResolver,
// ProposalService, etc)
@Slf4j
@RequiredArgsConstructor
public class RejectProposalsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService<?> _entityService;
  private final ActionRequestService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final List<String> proposalUrnStrs = bindArgument(environment.getArgument("urns"), List.class);
    final Set<Urn> proposalUrns =
        proposalUrnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());
    final String maybeNote = environment.getArgument("note");
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          // First, validate all proposals.
          ProposalUtils.validateProposalUrns(proposalUrns);

          // Then, reject the proposals
          rejectProposals(proposalUrns, maybeNote, context);

          // Return true if all are successful.
          return true;
        });
  }

  private void rejectProposals(
      @Nonnull final Set<Urn> proposalUrns,
      @Nullable String note,
      @Nonnull final QueryContext context) {
    final Map<Urn, Entity> resolvedProposalEntities =
        _entityService.getEntities(
            context.getOperationContext(), proposalUrns, new HashSet<>(), true);

    // Validate each proposal entity
    // Yes, passing _proposalService here is gross. Ideally proposal service should handle all of
    // this.
    ProposalUtils.validateProposals(resolvedProposalEntities.values(), context, _proposalService);

    // Finally, reject each proposal.
    for (final Urn proposalUrn : proposalUrns) {
      if (resolvedProposalEntities.get(proposalUrn) == null) {
        throw new RuntimeException(
            String.format(
                "Failed to reject proposal. Propose with urn %s was not found.", proposalUrn));
      }
      rejectProposal(resolvedProposalEntities.get(proposalUrn), note, context);
    }
  }

  private void rejectProposal(
      @Nonnull final Entity proposalEntity,
      @Nullable String note,
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
          rejectTagProposal(actionRequestInfo, context);
          break;
        case ACTION_REQUEST_TYPE_TERM_PROPOSAL:
          rejectTermProposal(actionRequestInfo, context);
          break;
        default:
          // Ignore - nothing to do!
      }

      _proposalService.completeProposal(
          context.getOperationContext(),
          actorUrn,
          ACTION_REQUEST_STATUS_COMPLETE,
          ACTION_REQUEST_RESULT_REJECTED,
          note,
          proposalEntity);

    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to reject proposal of type %s. Cause:", actionRequestInfo.getType()),
          e);
    }
  }

  private void rejectTagProposal(
      @Nonnull final ActionRequestInfo actionRequestInfo, @Nonnull final QueryContext context) {

    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    final Urn targetUrn = UrnUtils.getUrn(actionRequestInfo.getResource());
    final List<Urn> tagUrns =
        actionRequestInfo.getParams().getTagProposal().getTags() != null
                && !actionRequestInfo.getParams().getTagProposal().getTags().isEmpty()
            ? actionRequestInfo.getParams().getTagProposal().getTags()
            : ImmutableList.of(actionRequestInfo.getParams().getTagProposal().getTag());
    // Simply remove it from the searchable aspect
    ProposalUtils.deleteTagFromEntityOrSchemaProposalsAspect(
        context.getOperationContext(),
        actorUrn,
        tagUrns,
        targetUrn,
        actionRequestInfo.getSubResource(),
        _entityService);
  }

  private void rejectTermProposal(
      @Nonnull final ActionRequestInfo actionRequestInfo, @Nonnull final QueryContext context) {

    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    final Urn targetUrn = UrnUtils.getUrn(actionRequestInfo.getResource());
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

    // Simply remove it from the searchable aspect
    ProposalUtils.deleteTermFromEntityOrSchemaProposalsAspect(
        context.getOperationContext(),
        actorUrn,
        termUrns,
        targetUrn,
        actionRequestInfo.getResource(),
        _entityService);
  }
}
