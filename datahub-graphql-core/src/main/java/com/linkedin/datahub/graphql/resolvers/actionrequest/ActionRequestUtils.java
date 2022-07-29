package com.linkedin.datahub.graphql.resolvers.actionrequest;

import com.datahub.authentication.Authentication;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.CreateGlossaryNodeProposal;
import com.linkedin.actionrequest.CreateGlossaryTermProposal;
import com.linkedin.actionrequest.DescriptionProposal;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestParams;
import com.linkedin.datahub.graphql.generated.ActionRequestResourceProperties;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestSubResourceProperties;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityProposalProperties;
import com.linkedin.datahub.graphql.generated.CreateGlossaryNodeProposalParams;
import com.linkedin.datahub.graphql.generated.CreateGlossaryTermProposalParams;
import com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTermProposalParams;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.generated.TagProposalParams;
import com.linkedin.datahub.graphql.generated.UpdateDescriptionProposalParams;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.EditableSchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;


public class ActionRequestUtils {
  private static final String STATUS_FIELD_NAME = "status";
  private static final String TYPE_FIELD_NAME = "type";
  private static final String RESULT_FIELD_NAME = "result";
  private static final String RESOURCE_FIELD_NAME = "resource";
  private static final String SUBRESOURCE_FIELD_NAME = "subResource";
  private static final String LAST_MODIFIED_FIELD_NAME = "lastModified";

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

  public static ActionRequest mapRejectedActionRequest(final ActionRequestSnapshot snapshot,
      final EntityService entityService, final @Nullable ActionRequestType type) {
    final ActionRequest rejectedActionRequest = mapActionRequest(snapshot);

    if (rejectedActionRequest.getEntity() != null) {
      ActionRequestResourceProperties resourceProperties = new ActionRequestResourceProperties();
      ActionRequestSubResourceProperties subResourceProperties = new ActionRequestSubResourceProperties();

      if (type == null || type == ActionRequestType.TERM_ASSOCIATION) {
        if (rejectedActionRequest.getSubResource() != null && rejectedActionRequest.getSubResourceType() != null) {
          com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadataAspect =
              (EditableSchemaMetadata) getAspectFromEntity(rejectedActionRequest.getEntity().getUrn(),
                  EDITABLE_SCHEMA_METADATA_ASPECT_NAME, entityService, null);

          if (editableSchemaMetadataAspect != null && editableSchemaMetadataAspect.hasEditableSchemaFieldInfo()) {
            EditableSchemaMetadataMapper editableSchemaMetadataMapper = new EditableSchemaMetadataMapper();
            com.linkedin.datahub.graphql.generated.EditableSchemaMetadata editableSchemaMetadata =
                editableSchemaMetadataMapper.apply(editableSchemaMetadataAspect, UrnUtils.getUrn(rejectedActionRequest.getEntity().getUrn()));

            Optional<EditableSchemaFieldInfo> editableSchemaFieldInfoOptional =
                editableSchemaMetadata.getEditableSchemaFieldInfo()
                    .stream()
                    .filter(editableSchemaFieldInfo -> editableSchemaFieldInfo.getFieldPath()
                        .equals(rejectedActionRequest.getSubResource()))
                    .findFirst();

            if (editableSchemaFieldInfoOptional.isPresent()) {
              subResourceProperties.setGlossaryTerms(editableSchemaFieldInfoOptional.get().getGlossaryTerms());
            }
          }
        } else {
          com.linkedin.common.GlossaryTerms glossaryTermsAspect =
              (GlossaryTerms) getAspectFromEntity(rejectedActionRequest.getEntity().getUrn(),
                  GLOSSARY_TERMS_ASPECT_NAME, entityService, null);

          if (glossaryTermsAspect != null && glossaryTermsAspect.hasTerms()) {
            GlossaryTermsMapper glossaryTermsMapper = new GlossaryTermsMapper();
            com.linkedin.datahub.graphql.generated.GlossaryTerms glossaryTerms =
                glossaryTermsMapper.apply(glossaryTermsAspect, UrnUtils.getUrn(rejectedActionRequest.getEntity().getUrn()));

            resourceProperties.setGlossaryTerms(glossaryTerms);
          }
        }
      }

      if (type == null || type == ActionRequestType.TAG_ASSOCIATION) {
        if (rejectedActionRequest.getSubResource() != null && rejectedActionRequest.getSubResourceType() != null) {
          com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadataAspect =
              (EditableSchemaMetadata) getAspectFromEntity(rejectedActionRequest.getEntity().getUrn(),
                  EDITABLE_SCHEMA_METADATA_ASPECT_NAME, entityService, null);

          if (editableSchemaMetadataAspect != null && editableSchemaMetadataAspect.hasEditableSchemaFieldInfo()) {
            EditableSchemaMetadataMapper editableSchemaMetadataMapper = new EditableSchemaMetadataMapper();
            com.linkedin.datahub.graphql.generated.EditableSchemaMetadata editableSchemaMetadata =
                editableSchemaMetadataMapper.apply(editableSchemaMetadataAspect,
                    UrnUtils.getUrn(rejectedActionRequest.getEntity().getUrn()));

            Optional<EditableSchemaFieldInfo> editableSchemaFieldInfoOptional =
                editableSchemaMetadata.getEditableSchemaFieldInfo()
                    .stream()
                    .filter(editableSchemaFieldInfo -> editableSchemaFieldInfo.getFieldPath()
                        .equals(rejectedActionRequest.getSubResource()))
                    .findFirst();

            if (editableSchemaFieldInfoOptional.isPresent()) {
              subResourceProperties.setTags(editableSchemaFieldInfoOptional.get().getTags());
            }
          }
        } else {
          com.linkedin.common.GlobalTags globalTagsAspect =
              (GlobalTags) getAspectFromEntity(rejectedActionRequest.getEntity().getUrn(), GLOBAL_TAGS_ASPECT_NAME,
                  entityService, null);

          if (globalTagsAspect != null && globalTagsAspect.hasTags()) {
            GlobalTagsMapper globalTagsMapper = new GlobalTagsMapper();
            com.linkedin.datahub.graphql.generated.GlobalTags globalTags = globalTagsMapper.apply(
                globalTagsAspect,
                UrnUtils.getUrn(rejectedActionRequest.getEntity().getUrn()));

            resourceProperties.setTags(globalTags);
          }
        }
      }
      rejectedActionRequest.setResourceProperties(resourceProperties);
      rejectedActionRequest.setSubResourceProperties(subResourceProperties);
    }
    return rejectedActionRequest;
  }

  public static ActionRequestParams mapParams(final com.linkedin.actionrequest.ActionRequestParams params) {
    final ActionRequestParams result = new ActionRequestParams();
    if (params.hasGlossaryTermProposal()) {
      result.setGlossaryTermProposal(mapGlossaryTermProposal(params.getGlossaryTermProposal()));
    }
    if (params.hasTagProposal()) {
      result.setTagProposal(mapTagProposal(params.getTagProposal()));
    }
    if (params.hasCreateGlossaryTermProposal()) {
      result.setCreateGlossaryTermProposal(mapCreateGlossaryTermProposal(params.getCreateGlossaryTermProposal()));
    }
    if (params.hasCreateGlossaryNodeProposal()) {
      result.setCreateGlossaryNodeProposal(mapCreateGlossaryNodeProposal(params.getCreateGlossaryNodeProposal()));
    }
    if (params.hasUpdateDescriptionProposal()) {
      result.setUpdateDescriptionProposal(mapUpdateDescriptionProposal(params.getUpdateDescriptionProposal()));
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

  public static CreateGlossaryTermProposalParams mapCreateGlossaryTermProposal(final CreateGlossaryTermProposal proposal) {
    final CreateGlossaryTermProposalParams params = new CreateGlossaryTermProposalParams();
    final CreateGlossaryEntityProposalProperties glossaryEntity = new CreateGlossaryEntityProposalProperties();
    glossaryEntity.setName(proposal.getName());
    if (proposal.hasParentNode()) {
      final GlossaryNode parentNode = new GlossaryNode();
      parentNode.setUrn(proposal.getParentNode().toString());
      glossaryEntity.setParentNode(parentNode);
    }
    params.setGlossaryTerm(glossaryEntity);
    return params;
  }

  public static CreateGlossaryNodeProposalParams mapCreateGlossaryNodeProposal(final CreateGlossaryNodeProposal proposal) {
    final CreateGlossaryNodeProposalParams params = new CreateGlossaryNodeProposalParams();
    final CreateGlossaryEntityProposalProperties glossaryEntity = new CreateGlossaryEntityProposalProperties();
    glossaryEntity.setName(proposal.getName());
    if (proposal.hasParentNode()) {
      final GlossaryNode parentNode = new GlossaryNode();
      parentNode.setUrn(proposal.getParentNode().toString());
      glossaryEntity.setParentNode(parentNode);
    }
    params.setGlossaryNode(glossaryEntity);
    return params;
  }

  public static UpdateDescriptionProposalParams mapUpdateDescriptionProposal(final DescriptionProposal proposal) {
    final UpdateDescriptionProposalParams params = new UpdateDescriptionProposalParams();
    params.setDescription(proposal.getDescription());
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

  public static Criterion createResultCriterion(ActionRequestResult result) {
    final Criterion resultCriterion = new Criterion();
    resultCriterion.setField(RESULT_FIELD_NAME);
    resultCriterion.setValue(result.toString());
    resultCriterion.setCondition(Condition.EQUAL);
    return resultCriterion;
  }

  public static Criterion createResourceCriterion(String targetUrn) {
    final Criterion resourceCriterion = new Criterion();
    resourceCriterion.setField(RESOURCE_FIELD_NAME);
    resourceCriterion.setValue(targetUrn);
    resourceCriterion.setCondition(Condition.EQUAL);
    return resourceCriterion;
  }

  public static Criterion createSubResourceCriterion(String subResource) {
    final Criterion subResourceCriterion = new Criterion();
    subResourceCriterion.setField(SUBRESOURCE_FIELD_NAME);
    subResourceCriterion.setValue(subResource);
    subResourceCriterion.setCondition(Condition.EQUAL);
    return subResourceCriterion;
  }

  public static Criterion createStartTimestampCriterion(Long startTimestampMillis) {
    final Criterion startTimestampCriterion = new Criterion();
    startTimestampCriterion.setField(LAST_MODIFIED_FIELD_NAME);
    startTimestampCriterion.setValue(startTimestampMillis.toString());
    startTimestampCriterion.setCondition(Condition.GREATER_THAN_OR_EQUAL_TO);
    return startTimestampCriterion;
  }

  public static Criterion createEndTimestampCriterion(Long startTimestampMillis) {
    final Criterion endTimestampCriterion = new Criterion();
    endTimestampCriterion.setField(LAST_MODIFIED_FIELD_NAME);
    endTimestampCriterion.setValue(startTimestampMillis.toString());
    endTimestampCriterion.setCondition(Condition.LESS_THAN_OR_EQUAL_TO);
    return endTimestampCriterion;
  }

  public static List<ActionRequest> mapActionRequests(final Collection<Entity> entities) {
    final List<ActionRequest> results = new ArrayList<>();
    for (final Entity entity : entities) {
      final ActionRequestSnapshot snapshot = entity.getValue().getActionRequestSnapshot();
      results.add(ActionRequestUtils.mapActionRequest(snapshot));
    }
    return results;
  }

  public static List<ActionRequest> mapRejectedActionRequests(final Collection<Entity> entities,
      final EntityService entityService, final @Nullable ActionRequestType type) {
    final List<ActionRequest> results = new ArrayList<>();
    for (final Entity entity : entities) {
      final ActionRequestSnapshot snapshot = entity.getValue().getActionRequestSnapshot();
      results.add(ActionRequestUtils.mapRejectedActionRequest(snapshot, entityService, type));
    }
    return results.stream()
        .sorted(Comparator.comparing(actionRequest -> actionRequest.getLastModified().getTime()))
        .collect(Collectors.toList());
  }

  public static Optional<GroupMembership> resolveGroupMembership(final Urn actor, final Authentication authentication,
      EntityClient entityClient) {
    try {
      final CorpUserSnapshot corpUser = entityClient.get(actor, authentication).getValue().getCorpUserSnapshot();
      for (CorpUserAspect aspect : corpUser.getAspects()) {
        if (aspect.isGroupMembership()) {
          // Found group membership.
          return Optional.of(aspect.getGroupMembership());
        }
      }
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to fetch corpUser for urn %s", actor), e);
    }
    return Optional.empty();
  }

  private ActionRequestUtils() {
  }
}
