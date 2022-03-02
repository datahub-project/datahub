package com.linkedin.datahub.graphql.resolvers.mutate;

import com.linkedin.common.AuditStamp;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
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

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final Urn proposalUrn =
        Urn.createFromString(bindArgument(environment.getArgument("urn"), String.class));

    return CompletableFuture.supplyAsync(() -> {
      if (!proposalUrn.getEntityType().equals("actionRequest")) {
        throw new RuntimeException(String.format(
            "Failed to accept proposal, Urn provided (%s) is not a valid actionRequest urn",
            proposalUrn
        ));
      }
      if (!_entityService.exists(proposalUrn)) {
        throw new RuntimeException(String.format(
            "Failed to accept proposal, proposal provided (%s) does not exist",
            proposalUrn
        ));
      }

      Entity proposalEntity = _entityService.getEntity(proposalUrn, new HashSet<>());
      ActionRequest proposal =
          ActionRequestUtils.mapActionRequest(proposalEntity.getValue().getActionRequestSnapshot());

      try {
        log.info("Accepting term proposal. Proposal urn: {}", proposalUrn);
        Urn actor = CorpuserUrn.createFromString(((QueryContext) environment.getContext()).getActorUrn());
        String subResource = proposal.getSubResource();

        if (!ProposalUtils.isAuthorizedToAcceptProposal(environment.getContext(), proposal.getType(),
            Urn.createFromString(proposal.getEntity().getUrn()), subResource)) {
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        }

        if (!proposal.getStatus().equals(ActionRequestStatus.PENDING)) {
          log.error("Cannot accept proposal- proposal has already been completed");
          return false;
        }

        if (proposal.getType().equals(ActionRequestType.TAG_ASSOCIATION)) {
          Urn tagUrn = Urn.createFromString(proposal.getParams().getTagProposal().getTag().getUrn());
          Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
          LabelUtils.addTagToTarget(tagUrn, targetUrn, subResource, actor, _entityService);
          ProposalUtils.deleteTagFromEntityOrSchemaProposalsAspect(actor, tagUrn, targetUrn, subResource,
              _entityService);
        } else if (proposal.getType().equals(ActionRequestType.TERM_ASSOCIATION)) {
          Urn termUrn = Urn.createFromString(proposal.getParams().getGlossaryTermProposal().getGlossaryTerm().getUrn());
          Urn targetUrn = Urn.createFromString(proposal.getEntity().getUrn());
          LabelUtils.addTermToTarget(termUrn, targetUrn, subResource, actor, _entityService);
          ProposalUtils.deleteTermFromEntityOrSchemaProposalsAspect(actor, termUrn, targetUrn, subResource,
              _entityService);
        } else {
          log.error("Cannot accept proposal- proposal is not acceptable");
          return false;
        }

        ActionRequestSnapshot snapshot =
            ProposalUtils.setStatusSnapshot(actor, ActionRequestStatus.COMPLETED, ActionRequestResult.ACCEPTED, proposalEntity);

        final AuditStamp auditStamp = new AuditStamp();
        auditStamp.setActor(actor, SetMode.IGNORE_NULL);
        auditStamp.setTime(System.currentTimeMillis());

        Entity entity = new Entity();
        entity.setValue(Snapshot.create(snapshot));
        _entityService.ingestEntity(entity, auditStamp);

        return true;
      } catch (Exception e) {
        log.error("Failed to perform update against input {}, {}", proposalUrn, e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update against input %s", proposalUrn), e);
      }
    });
  }
}
