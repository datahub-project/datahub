package com.linkedin.datahub.graphql.resolvers.actionrequest;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.CreateGlossaryNodeProposal;
import com.linkedin.actionrequest.CreateGlossaryTermProposal;
import com.linkedin.actionrequest.DataContractProposal;
import com.linkedin.actionrequest.DescriptionProposal;
import com.linkedin.actionrequest.DomainProposal;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.OwnerProposal;
import com.linkedin.actionrequest.StructuredPropertyProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestOrigin;
import com.linkedin.datahub.graphql.generated.ActionRequestParams;
import com.linkedin.datahub.graphql.generated.ActionRequestResourceProperties;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestSubResourceProperties;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityProposalProperties;
import com.linkedin.datahub.graphql.generated.CreateGlossaryNodeProposalParams;
import com.linkedin.datahub.graphql.generated.CreateGlossaryTermProposalParams;
import com.linkedin.datahub.graphql.generated.DataContractProposalOperationType;
import com.linkedin.datahub.graphql.generated.DataContractProposalParams;
import com.linkedin.datahub.graphql.generated.DataQualityContract;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainProposalParams;
import com.linkedin.datahub.graphql.generated.EditableSchemaFieldInfo;
import com.linkedin.datahub.graphql.generated.FreshnessContract;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryTerm;
import com.linkedin.datahub.graphql.generated.GlossaryTermProposalParams;
import com.linkedin.datahub.graphql.generated.InferenceMetadata;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.generated.OwnerProposalParams;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.datahub.graphql.generated.SchemaContract;
import com.linkedin.datahub.graphql.generated.StructuredPropertiesEntry;
import com.linkedin.datahub.graphql.generated.StructuredPropertyProposalParams;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.generated.TagProposalParams;
import com.linkedin.datahub.graphql.generated.UpdateDescriptionProposalParams;
import com.linkedin.datahub.graphql.types.common.mappers.OwnerMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.dataset.mappers.EditableSchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

public class ActionRequestUtils {
  private static final String STATUS_FIELD_NAME = "status";
  private static final String TYPE_FIELD_NAME = "type";
  private static final String RESULT_FIELD_NAME = "result";
  private static final String RESOURCE_FIELD_NAME = "resource";
  private static final String SUBRESOURCE_FIELD_NAME = "subResource";
  private static final String LAST_MODIFIED_FIELD_NAME = "lastModified";

  public static ActionRequest mapActionRequest(
      @Nullable QueryContext context, final ActionRequestSnapshot snapshot) {
    final ActionRequest actionRequest = new ActionRequest();
    actionRequest.setUrn(snapshot.getUrn().toString());
    for (ActionRequestAspect aspect : snapshot.getAspects()) {
      if (aspect.isActionRequestInfo()) {
        ActionRequestInfo actionRequestInfo = aspect.getActionRequestInfo();
        actionRequest.setType(ActionRequestType.valueOf(actionRequestInfo.getType()));

        actionRequest.setDueDate(actionRequestInfo.getDueDate());
        actionRequest.setAssignedUsers(
            actionRequestInfo.getAssignedUsers().stream()
                .map(Urn::toString)
                .collect(Collectors.toList()));
        actionRequest.setAssignedGroups(
            actionRequestInfo.getAssignedGroups().stream()
                .map(Urn::toString)
                .collect(Collectors.toList()));

        List<String> roles =
            actionRequestInfo.hasAssignedRoles()
                ? actionRequestInfo.getAssignedRoles().stream()
                    .map(Urn::toString)
                    .collect(Collectors.toList())
                : ImmutableList.of();
        actionRequest.setAssignedRoles(roles);

        if (actionRequestInfo.hasResource()) {
          // For now, resource must be of Urn type to qualify. This assumption needs to be
          // documented explicitly somewhere.
          try {
            Urn resourceUrn = Urn.createFromString(actionRequestInfo.getResource());
            actionRequest.setEntity(UrnToEntityMapper.map(context, resourceUrn));
          } catch (URISyntaxException e) {
            throw new RuntimeException(
                String.format(
                    "Failed to convert ActionRequest Resource field into an Entity URN %s",
                    actionRequestInfo.getResource()));
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
          actionRequest.setParams(
              mapParams(
                  context,
                  actionRequestInfo.getParams(),
                  actionRequestInfo.hasResource()
                      ? UrnUtils.getUrn(actionRequestInfo.getResource())
                      : null));
        }

        if (actionRequestInfo.hasOrigin()) {
          actionRequest.setOrigin(
              ActionRequestOrigin.valueOf(actionRequestInfo.getOrigin().toString()));
        }

        if (actionRequestInfo.hasInferenceMetadata()) {
          actionRequest.setInferenceMetadata(
              mapInferenceMetadata(actionRequestInfo.getInferenceMetadata()));
        }

      } else if (aspect.isActionRequestStatus()) {
        com.linkedin.actionrequest.ActionRequestStatus actionRequestStatus =
            aspect.getActionRequestStatus();
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

  public static ActionRequest mapRejectedActionRequest(
      @Nullable QueryContext context,
      final ActionRequestSnapshot snapshot,
      final EntityService entityService,
      final @Nullable ActionRequestType type) {
    final ActionRequest rejectedActionRequest = mapActionRequest(context, snapshot);

    if (rejectedActionRequest.getEntity() != null) {
      ActionRequestResourceProperties resourceProperties = new ActionRequestResourceProperties();
      ActionRequestSubResourceProperties subResourceProperties =
          new ActionRequestSubResourceProperties();

      if (type == null || type == ActionRequestType.TERM_ASSOCIATION) {
        if (rejectedActionRequest.getSubResource() != null
            && rejectedActionRequest.getSubResourceType() != null) {
          com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadataAspect =
              (EditableSchemaMetadata)
                  EntityUtils.getAspectFromEntity(
                      context.getOperationContext(),
                      rejectedActionRequest.getEntity().getUrn(),
                      EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                      entityService,
                      null);

          if (editableSchemaMetadataAspect != null
              && editableSchemaMetadataAspect.hasEditableSchemaFieldInfo()) {
            EditableSchemaMetadataMapper editableSchemaMetadataMapper =
                new EditableSchemaMetadataMapper();
            com.linkedin.datahub.graphql.generated.EditableSchemaMetadata editableSchemaMetadata =
                editableSchemaMetadataMapper.apply(
                    context,
                    editableSchemaMetadataAspect,
                    UrnUtils.getUrn(rejectedActionRequest.getEntity().getUrn()));

            Optional<EditableSchemaFieldInfo> editableSchemaFieldInfoOptional =
                editableSchemaMetadata.getEditableSchemaFieldInfo().stream()
                    .filter(
                        editableSchemaFieldInfo ->
                            editableSchemaFieldInfo
                                .getFieldPath()
                                .equals(rejectedActionRequest.getSubResource()))
                    .findFirst();

            if (editableSchemaFieldInfoOptional.isPresent()) {
              subResourceProperties.setGlossaryTerms(
                  editableSchemaFieldInfoOptional.get().getGlossaryTerms());
            }
          }
        } else {
          com.linkedin.common.GlossaryTerms glossaryTermsAspect =
              (GlossaryTerms)
                  EntityUtils.getAspectFromEntity(
                      context.getOperationContext(),
                      rejectedActionRequest.getEntity().getUrn(),
                      GLOSSARY_TERMS_ASPECT_NAME,
                      entityService,
                      null);

          if (glossaryTermsAspect != null && glossaryTermsAspect.hasTerms()) {
            GlossaryTermsMapper glossaryTermsMapper = new GlossaryTermsMapper();
            com.linkedin.datahub.graphql.generated.GlossaryTerms glossaryTerms =
                glossaryTermsMapper.apply(
                    context,
                    glossaryTermsAspect,
                    UrnUtils.getUrn(rejectedActionRequest.getEntity().getUrn()));

            resourceProperties.setGlossaryTerms(glossaryTerms);
          }
        }
      }

      if (type == null || type == ActionRequestType.TAG_ASSOCIATION) {
        if (rejectedActionRequest.getSubResource() != null
            && rejectedActionRequest.getSubResourceType() != null) {
          com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadataAspect =
              (EditableSchemaMetadata)
                  EntityUtils.getAspectFromEntity(
                      context.getOperationContext(),
                      rejectedActionRequest.getEntity().getUrn(),
                      EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                      entityService,
                      null);

          if (editableSchemaMetadataAspect != null
              && editableSchemaMetadataAspect.hasEditableSchemaFieldInfo()) {
            EditableSchemaMetadataMapper editableSchemaMetadataMapper =
                new EditableSchemaMetadataMapper();
            com.linkedin.datahub.graphql.generated.EditableSchemaMetadata editableSchemaMetadata =
                editableSchemaMetadataMapper.apply(
                    context,
                    editableSchemaMetadataAspect,
                    UrnUtils.getUrn(rejectedActionRequest.getEntity().getUrn()));

            Optional<EditableSchemaFieldInfo> editableSchemaFieldInfoOptional =
                editableSchemaMetadata.getEditableSchemaFieldInfo().stream()
                    .filter(
                        editableSchemaFieldInfo ->
                            editableSchemaFieldInfo
                                .getFieldPath()
                                .equals(rejectedActionRequest.getSubResource()))
                    .findFirst();

            if (editableSchemaFieldInfoOptional.isPresent()) {
              subResourceProperties.setTags(editableSchemaFieldInfoOptional.get().getTags());
            }
          }
        } else {
          com.linkedin.common.GlobalTags globalTagsAspect =
              (GlobalTags)
                  EntityUtils.getAspectFromEntity(
                      context.getOperationContext(),
                      rejectedActionRequest.getEntity().getUrn(),
                      GLOBAL_TAGS_ASPECT_NAME,
                      entityService,
                      null);

          if (globalTagsAspect != null && globalTagsAspect.hasTags()) {
            GlobalTagsMapper globalTagsMapper = new GlobalTagsMapper();
            com.linkedin.datahub.graphql.generated.GlobalTags globalTags =
                globalTagsMapper.apply(
                    context,
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

  public static ActionRequestParams mapParams(
      @Nullable final QueryContext context,
      final com.linkedin.actionrequest.ActionRequestParams params,
      @Nullable final Urn entityUrn) {
    final ActionRequestParams result = new ActionRequestParams();
    if (params.hasGlossaryTermProposal()) {
      result.setGlossaryTermProposal(mapGlossaryTermProposal(params.getGlossaryTermProposal()));
    }
    if (params.hasTagProposal()) {
      result.setTagProposal(mapTagProposal(params.getTagProposal()));
    }
    if (params.hasCreateGlossaryTermProposal()) {
      result.setCreateGlossaryTermProposal(
          mapCreateGlossaryTermProposal(params.getCreateGlossaryTermProposal()));
    }
    if (params.hasCreateGlossaryNodeProposal()) {
      result.setCreateGlossaryNodeProposal(
          mapCreateGlossaryNodeProposal(params.getCreateGlossaryNodeProposal()));
    }
    if (params.hasUpdateDescriptionProposal()) {
      result.setUpdateDescriptionProposal(
          mapUpdateDescriptionProposal(params.getUpdateDescriptionProposal()));
    }
    if (params.hasDataContractProposal()) {
      result.setDataContractProposal(mapDataContractProposal(params.getDataContractProposal()));
    }
    if (params.hasStructuredPropertyProposal() && entityUrn != null) {
      result.setStructuredPropertyProposal(
          mapStructuredPropertyProposal(
              context, params.getStructuredPropertyProposal(), entityUrn));
    }
    if (params.hasDomainProposal()) {
      result.setDomainProposal(mapDomainProposal(params.getDomainProposal()));
    }
    if (params.hasOwnerProposal()) {
      result.setOwnerProposal(mapOwnerProposal(context, params.getOwnerProposal(), entityUrn));
    }
    return result;
  }

  public static GlossaryTermProposalParams mapGlossaryTermProposal(
      final GlossaryTermProposal proposal) {
    final GlossaryTermProposalParams params = new GlossaryTermProposalParams();

    if (proposal.hasGlossaryTerm()) {
      // Map legacy "term" field.
      final GlossaryTerm emptyTerm = new GlossaryTerm();
      emptyTerm.setUrn(proposal.getGlossaryTerm().toString());
      params.setGlossaryTerm(emptyTerm);
    }

    if (proposal.hasGlossaryTerms()) {
      // Map new "terms" field.
      final List<GlossaryTerm> emptyTerms = new ArrayList<>();
      for (Urn term : proposal.getGlossaryTerms()) {
        final GlossaryTerm emptyGlossaryTerm = new GlossaryTerm();
        emptyGlossaryTerm.setUrn(term.toString());
        emptyTerms.add(emptyGlossaryTerm);
      }
      params.setGlossaryTerms(emptyTerms);
    }

    return params;
  }

  public static TagProposalParams mapTagProposal(final TagProposal proposal) {
    final TagProposalParams params = new TagProposalParams();
    final Tag emptyTag = new Tag();

    // Map legacy "tag" field.
    if (proposal.hasTag()) {
      emptyTag.setUrn(proposal.getTag().toString());
      params.setTag(emptyTag);
    }

    if (proposal.hasTags()) {
      // Map new "tags" field.
      final List<Tag> emptyTags = new ArrayList<>();
      for (Urn tag : proposal.getTags()) {
        final Tag emptyTagProposal = new Tag();
        emptyTagProposal.setUrn(tag.toString());
        emptyTags.add(emptyTagProposal);
      }
      params.setTags(emptyTags);
    }

    return params;
  }

  public static CreateGlossaryTermProposalParams mapCreateGlossaryTermProposal(
      final CreateGlossaryTermProposal proposal) {
    final CreateGlossaryTermProposalParams params = new CreateGlossaryTermProposalParams();
    final CreateGlossaryEntityProposalProperties glossaryEntity =
        new CreateGlossaryEntityProposalProperties();
    glossaryEntity.setName(proposal.getName());
    if (proposal.hasParentNode()) {
      final GlossaryNode parentNode = new GlossaryNode();
      parentNode.setUrn(proposal.getParentNode().toString());
      glossaryEntity.setParentNode(parentNode);
    }
    if (proposal.hasDescription()) {
      glossaryEntity.setDescription(proposal.getDescription());
    }
    params.setGlossaryTerm(glossaryEntity);
    return params;
  }

  public static CreateGlossaryNodeProposalParams mapCreateGlossaryNodeProposal(
      final CreateGlossaryNodeProposal proposal) {
    final CreateGlossaryNodeProposalParams params = new CreateGlossaryNodeProposalParams();
    final CreateGlossaryEntityProposalProperties glossaryEntity =
        new CreateGlossaryEntityProposalProperties();
    glossaryEntity.setName(proposal.getName());
    if (proposal.hasParentNode()) {
      final GlossaryNode parentNode = new GlossaryNode();
      parentNode.setUrn(proposal.getParentNode().toString());
      glossaryEntity.setParentNode(parentNode);
    }
    if (proposal.hasDescription()) {
      glossaryEntity.setDescription(proposal.getDescription());
    }
    params.setGlossaryNode(glossaryEntity);
    return params;
  }

  public static UpdateDescriptionProposalParams mapUpdateDescriptionProposal(
      final DescriptionProposal proposal) {
    final UpdateDescriptionProposalParams params = new UpdateDescriptionProposalParams();
    params.setDescription(proposal.getDescription());
    return params;
  }

  public static DataContractProposalParams mapDataContractProposal(
      final DataContractProposal proposal) {
    final DataContractProposalParams params = new DataContractProposalParams();
    params.setOperationType(
        DataContractProposalOperationType.valueOf(proposal.getType().toString()));
    if (proposal.hasSchema()) {
      params.setSchema(
          proposal.getSchema().stream()
              .map(ActionRequestUtils::mapSchemaContract)
              .collect(Collectors.toList()));
    }
    if (proposal.hasFreshness()) {
      params.setFreshness(
          proposal.getFreshness().stream()
              .map(ActionRequestUtils::mapFreshnessContract)
              .collect(Collectors.toList()));
    }
    if (proposal.hasDataQuality()) {
      params.setDataQuality(
          proposal.getDataQuality().stream()
              .map(ActionRequestUtils::mapDataQualityContract)
              .collect(Collectors.toList()));
    }
    return params;
  }

  public static StructuredPropertyProposalParams mapStructuredPropertyProposal(
      final QueryContext context, final StructuredPropertyProposal proposal, final Urn entityUrn) {
    final StructuredPropertyProposalParams propertyProposalParams =
        new StructuredPropertyProposalParams();
    final List<StructuredPropertiesEntry> actualPropertyProposals = new ArrayList<>();
    for (final StructuredPropertyValueAssignment entry : proposal.getStructuredPropertyValues()) {
      final StructuredPropertiesEntry propertyProposal =
          StructuredPropertiesMapper.INSTANCE.mapStructuredProperty(context, entry, entityUrn);
      actualPropertyProposals.add(propertyProposal);
    }
    propertyProposalParams.setStructuredProperties(actualPropertyProposals);
    return propertyProposalParams;
  }

  public static DomainProposalParams mapDomainProposal(final DomainProposal proposal) {
    final DomainProposalParams domainProposalParams = new DomainProposalParams();
    if (proposal.hasDomains() && proposal.getDomains().size() > 0) {
      // Extract the first domain for proposal
      final Urn domainUrn = proposal.getDomains().get(0);
      final Domain partialDomain = new Domain();
      partialDomain.setUrn(domainUrn.toString());
      domainProposalParams.setDomain(partialDomain);
    }
    return domainProposalParams;
  }

  public static OwnerProposalParams mapOwnerProposal(
      final QueryContext context, final OwnerProposal proposal, final Urn entityUrn) {
    final OwnerProposalParams ownerProposalParams = new OwnerProposalParams();
    if (proposal.hasOwners()) {
      List<Owner> owners = new ArrayList<>();
      for (com.linkedin.common.Owner owner : proposal.getOwners()) {
        Owner proposedOwner = OwnerMapper.map(context, owner, entityUrn);
        owners.add(proposedOwner);
      }
      ownerProposalParams.setOwners(owners);
    }
    return ownerProposalParams;
  }

  public static Criterion createStatusCriterion(ActionRequestStatus status) {
    return CriterionUtils.buildCriterion(STATUS_FIELD_NAME, Condition.EQUAL, status.toString());
  }

  public static Criterion createTypeCriterion(ActionRequestType type) {
    return CriterionUtils.buildCriterion(TYPE_FIELD_NAME, Condition.EQUAL, type.toString());
  }

  public static Criterion createResultCriterion(ActionRequestResult result) {
    return CriterionUtils.buildCriterion(RESULT_FIELD_NAME, Condition.EQUAL, result.toString());
  }

  public static Criterion createResourceCriterion(String targetUrn) {
    return CriterionUtils.buildCriterion(RESOURCE_FIELD_NAME, Condition.EQUAL, targetUrn);
  }

  public static Criterion createSubResourceCriterion(String subResource) {
    return CriterionUtils.buildCriterion(SUBRESOURCE_FIELD_NAME, Condition.EQUAL, subResource);
  }

  public static Criterion createStartTimestampCriterion(Long startTimestampMillis) {
    return CriterionUtils.buildCriterion(
        LAST_MODIFIED_FIELD_NAME,
        Condition.GREATER_THAN_OR_EQUAL_TO,
        startTimestampMillis.toString());
  }

  public static Criterion createEndTimestampCriterion(Long endTimestampMillis) {
    return CriterionUtils.buildCriterion(
        LAST_MODIFIED_FIELD_NAME, Condition.LESS_THAN_OR_EQUAL_TO, endTimestampMillis.toString());
  }

  public static List<ActionRequest> mapActionRequests(
      @Nullable final QueryContext context, final Collection<Entity> entities) {
    final List<ActionRequest> results = new ArrayList<>();
    for (final Entity entity : entities) {
      actionsRequestSnapshotCanView(context, entity.getValue().getActionRequestSnapshot())
          .ifPresent(
              snapshot -> results.add(ActionRequestUtils.mapActionRequest(context, snapshot)));
    }
    return results;
  }

  private static Optional<ActionRequestSnapshot> actionsRequestSnapshotCanView(
      @Nullable final QueryContext context, ActionRequestSnapshot snapshot) {
    if (context != null) {
      if (snapshot.getAspects().stream()
          .filter(ActionRequestAspect::isActionRequestInfo)
          .map(ActionRequestAspect::getActionRequestInfo)
          .filter(ActionRequestInfo::hasResource)
          .flatMap(
              info -> {
                // extracting urns
                final Urn resourceUrn = UrnUtils.getUrn(info.getResource());

                final List<Urn> termUrns =
                    info.hasParams() && info.getParams().hasGlossaryTermProposal()
                        ? ActionRequestUtils.extractTermsFromTermProposal(
                            info.getParams().getGlossaryTermProposal())
                        : Collections.emptyList();

                final List<Urn> tagUrns =
                    info.hasParams() && info.getParams().hasTagProposal()
                        ? ActionRequestUtils.extractTagsFromTagProposal(
                            info.getParams().getTagProposal())
                        : Collections.emptyList();

                final List<Urn> propertyUrns =
                    info.hasParams() && info.getParams().hasStructuredPropertyProposal()
                        ? ActionRequestUtils.extractPropertiesFromStructuredPropertyProposal(
                            info.getParams().getStructuredPropertyProposal())
                        : Collections.emptyList();

                return Stream.concat(
                    Stream.concat(Stream.of(resourceUrn), propertyUrns.stream()),
                    Stream.concat(termUrns.stream(), tagUrns.stream()));
              })
          .filter(Objects::nonNull)
          .anyMatch(optUrn -> !canView(context.getOperationContext(), optUrn))) {

        // return empty if contains reference to restricted urn
        return Optional.empty();
      }
    }

    return Optional.of(snapshot);
  }

  private static List<Urn> extractTagsFromTagProposal(TagProposal tagProposal) {
    if (tagProposal.hasTags() && tagProposal.getTags().size() > 0) {
      return tagProposal.getTags();
    } else if (tagProposal.hasTag()) {
      return ImmutableList.of(tagProposal.getTag());
    } else {
      // Technically, should never happen!
      return Collections.emptyList();
    }
  }

  private static List<Urn> extractTermsFromTermProposal(GlossaryTermProposal termProposal) {
    if (termProposal.hasGlossaryTerms() && termProposal.getGlossaryTerms().size() > 0) {
      return termProposal.getGlossaryTerms();
    } else if (termProposal.hasGlossaryTerm()) {
      return ImmutableList.of(termProposal.getGlossaryTerm());
    } else {
      // Technically, should never happen!
      return Collections.emptyList();
    }
  }

  private static List<Urn> extractPropertiesFromStructuredPropertyProposal(
      StructuredPropertyProposal structuredPropertyProposal) {
    if (structuredPropertyProposal.hasStructuredPropertyValues()
        && structuredPropertyProposal.getStructuredPropertyValues().size() > 0) {
      return structuredPropertyProposal.getStructuredPropertyValues().stream()
          .map(StructuredPropertyValueAssignment::getPropertyUrn)
          .collect(Collectors.toList());
    } else {
      // Technically, should never happen!
      return Collections.emptyList();
    }
  }

  public static List<ActionRequest> mapRejectedActionRequests(
      @Nullable final QueryContext context,
      final Collection<Entity> entities,
      final EntityService entityService,
      final @Nullable ActionRequestType type) {
    final List<ActionRequest> results = new ArrayList<>();
    for (final Entity entity : entities) {
      final ActionRequestSnapshot snapshot = entity.getValue().getActionRequestSnapshot();
      results.add(
          ActionRequestUtils.mapRejectedActionRequest(context, snapshot, entityService, type));
    }
    return results.stream()
        .sorted(Comparator.comparing(actionRequest -> actionRequest.getLastModified().getTime()))
        .collect(Collectors.toList());
  }

  public static AssignedUrns getGroupAndRoleUrns(
      @Nonnull OperationContext opContext, final Urn actor, EntityClient entityClient)
      throws Exception {
    List<Urn> groupUrns = new ArrayList<>();
    List<Urn> roleUrns = new ArrayList<>();
    try {
      final EntityResponse response =
          entityClient.getV2(opContext, CORP_USER_ENTITY_NAME, actor, null);

      final EnvelopedAspectMap aspects = response.getAspects();

      if (aspects.get(GROUP_MEMBERSHIP_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(GROUP_MEMBERSHIP_ASPECT_NAME);
        final GroupMembership groupMembership = new GroupMembership(aspect.getValue().data());
        groupUrns.addAll(groupMembership.getGroups());
      }
      if (aspects.get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME);
        final NativeGroupMembership nativeGroupMembership =
            new NativeGroupMembership(aspect.getValue().data());
        groupUrns.addAll(nativeGroupMembership.getNativeGroups());
      }
      if (aspects.get(ROLE_MEMBERSHIP_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(ROLE_MEMBERSHIP_ASPECT_NAME);
        final RoleMembership roleMembership = new RoleMembership(aspect.getValue().data());
        roleUrns.addAll(roleMembership.getRoles());
      }
      return new AssignedUrns(groupUrns, roleUrns);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to fetch corpUser for urn %s", actor), e);
    }
  }

  @Value
  static class AssignedUrns {
    List<Urn> groupUrns;
    List<Urn> roleUrns;
  }

  private static SchemaContract mapSchemaContract(
      final com.linkedin.datacontract.SchemaContract schemaContract) {
    final SchemaContract result = new SchemaContract();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(schemaContract.getAssertion().toString());
    result.setAssertion(partialAssertion);
    return result;
  }

  private static FreshnessContract mapFreshnessContract(
      final com.linkedin.datacontract.FreshnessContract freshnessContract) {
    final FreshnessContract result = new FreshnessContract();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(freshnessContract.getAssertion().toString());
    result.setAssertion(partialAssertion);
    return result;
  }

  private static DataQualityContract mapDataQualityContract(
      final com.linkedin.datacontract.DataQualityContract qualityContract) {
    final DataQualityContract result = new DataQualityContract();
    final Assertion partialAssertion = new Assertion();
    partialAssertion.setUrn(qualityContract.getAssertion().toString());
    result.setAssertion(partialAssertion);
    return result;
  }

  private static InferenceMetadata mapInferenceMetadata(
      final com.linkedin.ai.InferenceMetadata inferenceMetadata) {
    final InferenceMetadata result = new InferenceMetadata();
    result.setVersion(inferenceMetadata.getVersion());
    result.setLastInferredAt(inferenceMetadata.getLastInferredAt());
    if (inferenceMetadata.hasConfidenceLevel()) {
      result.setConfidenceLevel(inferenceMetadata.getConfidenceLevel());
    }
    return result;
  }

  private ActionRequestUtils() {}
}
