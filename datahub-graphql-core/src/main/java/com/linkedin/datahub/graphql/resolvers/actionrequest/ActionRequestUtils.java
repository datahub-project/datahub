package com.linkedin.datahub.graphql.resolvers.actionrequest;

import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestParams;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTermProposalParams;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.generated.TagProposalParams;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;


public class ActionRequestUtils {
  private static final String STATUS_FIELD_NAME = "status";
  private static final String TYPE_FIELD_NAME = "type";
  private static final String RESOURCE_FIELD_NAME = "resource";
  private static final String SUBRESOURCE_FIELD_NAME = "subResource";

  public static ActionRequest mapActionRequest(final ActionRequestSnapshot snapshot) {
    final ActionRequest actionRequest = new ActionRequest();
    actionRequest.setUrn(snapshot.getUrn().toString());
    for (ActionRequestAspect aspect : snapshot.getAspects()) {
      if (aspect.isActionRequestInfo()) {
        ActionRequestInfo actionRequestInfo = aspect.getActionRequestInfo();
        actionRequest.setType(ActionRequestType.valueOf(actionRequestInfo.getType()));

        actionRequest.setDueDate(actionRequestInfo.getDueDate());
        actionRequest.setAssignedUsers(actionRequestInfo.getAssignedUsers().stream().map(Urn::toString).collect(
            Collectors.toList()));
        actionRequest.setAssignedGroups(actionRequestInfo.getAssignedGroups().stream().map(Urn::toString).collect(
            Collectors.toList()));

        if (actionRequestInfo.hasResource()) {
          // For now, resource must be of Urn type to qualify. This assumption needs to be documented explicitly somewhere.
          try {
            Urn resourceUrn = Urn.createFromString(actionRequestInfo.getResource());
            actionRequest.setEntity(UrnToEntityMapper.map(resourceUrn));
          } catch (URISyntaxException e) {
            throw new RuntimeException(
                String.format("Failed to convert ActionRequest Resource field into an Entity URN %s", actionRequestInfo.getResource()));
          }
        }

        actionRequest.setSubResource(actionRequestInfo.getSubResource());
        actionRequest.setSubResourceType(actionRequestInfo.getSubResourceType());

        final ResolvedAuditStamp createdStamp = new ResolvedAuditStamp();
        final CorpUser emptyCreatedUser = new CorpUser();
        emptyCreatedUser.setUrn(actionRequestInfo.getCreatedBy().toString());
        createdStamp.setActor(emptyCreatedUser);
        createdStamp.setTime(actionRequestInfo.getCreated());
        actionRequest.setCreated(createdStamp);

        if (actionRequestInfo.hasParams()) {
          actionRequest.setParams(mapParams(actionRequestInfo.getParams()));
        }

      } else if (aspect.isActionRequestStatus()) {
        com.linkedin.actionrequest.ActionRequestStatus actionRequestStatus = aspect.getActionRequestStatus();
        actionRequest.setStatus(ActionRequestStatus.valueOf(actionRequestStatus.getStatus()));
        if (actionRequestStatus.hasResult() && actionRequestStatus.getResult().length() > 0) {
          actionRequest.setResult(ActionRequestResult.valueOf(actionRequestStatus.getResult()));
        }
        if (actionRequestStatus.hasLastModified()) {
          final ResolvedAuditStamp lastModifiedStamp = new ResolvedAuditStamp();
          final CorpUser emptyModifiedUser = new CorpUser();
          emptyModifiedUser.setUrn(actionRequestStatus.getLastModified().getActor().toString());
          lastModifiedStamp.setActor(emptyModifiedUser);
          lastModifiedStamp.setTime(actionRequestStatus.getLastModified().getTime());
          actionRequest.setLastModified(lastModifiedStamp);
        }
      }
    }
    return actionRequest;
  }

  public static ActionRequestParams mapParams(final com.linkedin.actionrequest.ActionRequestParams params) {
    final ActionRequestParams result = new ActionRequestParams();
    if (params.hasGlossaryTermProposal()) {
      result.setGlossaryTermProposal(mapGlossaryTermProposal(params.getGlossaryTermProposal()));
    }
    if (params.hasTagProposal()) {
      result.setTagProposal(mapTagProposal(params.getTagProposal()));
    }
    return result;
  }

  public static GlossaryTermProposalParams mapGlossaryTermProposal(final GlossaryTermProposal proposal) {
    final GlossaryTermProposalParams params = new GlossaryTermProposalParams();
    final GlossaryTerm emptyTerm = new GlossaryTerm();
    emptyTerm.setUrn(proposal.getGlossaryTerm().toString());
    params.setGlossaryTerm(emptyTerm);
    return params;
  }

  public static TagProposalParams mapTagProposal(final TagProposal proposal) {
    final TagProposalParams params = new TagProposalParams();
    final Tag emptyTag = new Tag();
    emptyTag.setUrn(proposal.getTag().toString());
    params.setTag(emptyTag);
    return params;
  }

  public static Criterion createStatusCriterion(ActionRequestStatus status) {
    final Criterion statusCriterion = new Criterion();
    statusCriterion.setField(STATUS_FIELD_NAME);
    statusCriterion.setValue(status.toString());
    statusCriterion.setCondition(Condition.EQUAL);
    return statusCriterion;
  }

  public static Criterion createTypeCriterion(ActionRequestType type) {
    final Criterion typeCriterion = new Criterion();
    typeCriterion.setField(TYPE_FIELD_NAME);
    typeCriterion.setValue(type.toString());
    typeCriterion.setCondition(Condition.EQUAL);
    return typeCriterion;
  }

  public static Criterion createResourceCriterion(String targetUrn) {
    final Criterion typeCriterion = new Criterion();
    typeCriterion.setField(RESOURCE_FIELD_NAME);
    typeCriterion.setValue(targetUrn);
    typeCriterion.setCondition(Condition.EQUAL);
    return typeCriterion;
  }

  public static Criterion createSubResourceCriterion(String subResource) {
    final Criterion typeCriterion = new Criterion();
    typeCriterion.setField(SUBRESOURCE_FIELD_NAME);
    typeCriterion.setValue(subResource);
    typeCriterion.setCondition(Condition.EQUAL);
    return typeCriterion;
  }

  public static List<ActionRequest> mapActionRequests(final Collection<Entity> entities) {
    final List<ActionRequest> results = new ArrayList<>();
    for (final Entity entity : entities) {
      final ActionRequestSnapshot snapshot = entity.getValue().getActionRequestSnapshot();
      results.add(ActionRequestUtils.mapActionRequest(snapshot));
    }
    return results;
  }

  private ActionRequestUtils() { }
}
