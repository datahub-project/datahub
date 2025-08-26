package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_DATA_CONTRACT_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.Proposals;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ActionRequestOrigin;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.InferenceMetadataInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.GlossaryUtils;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.query.filter.*;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaProposal;
import com.linkedin.schema.SchemaProposalArray;
import com.linkedin.schema.SchemaProposals;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
// NOTE TO ALL: This file is now deprecated and slotted to be slowly replaced by the
// ActionRequestService.java file in metadata-io, which will be more thoroughly unit tested
// & maintained in correspondence with how we maintain other service classes for easier re-use
// elsewhere across the stack.
// TODO(Gabe): Unit test this file
public class ProposalUtils {
  public static final String PROPOSALS_ASPECT_NAME = "proposals";
  public static final String SCHEMA_PROPOSALS_ASPECT_NAME = "schemaProposals";
  public static final String CONTAINER_ASPECT_NAME = "container";

  private static final String ASSIGNED_USERS_FIELD_NAME = "assignedUsers";
  private static final String ASSIGNED_GROUPS_FIELD_NAME = "assignedGroups";
  private static final String ASSIGNED_ROLES_FIELD_NAME = "assignedRoles";

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  public static boolean isAuthorizedToProposeTags(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the asset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.PROPOSE_DATASET_COL_TAGS_PRIVILEGE.getType()
                            : PoliciesConfig.PROPOSE_ENTITY_TAGS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToProposeTerms(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the asset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.PROPOSE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()
                            : PoliciesConfig.PROPOSE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToProposeDescription(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    boolean isTargetingSchema = subResource != null && !subResource.isEmpty();

    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.PROPOSE_DATASET_COL_DESCRIPTION_PRIVILEGE.getType()
                            : PoliciesConfig.PROPOSE_ENTITY_DOCS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToProposeStructuredProperties(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the asset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.PROPOSE_DATASET_COL_PROPERTIES_PRIVILEGE.getType()
                            : PoliciesConfig.PROPOSE_ENTITY_PROPERTIES_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToProposeDomains(@Nonnull QueryContext context, Urn targetUrn) {
    // Decide whether the current principal should be allowed to update the asset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.PROPOSE_ENTITY_DOMAINS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToProposeOwners(@Nonnull QueryContext context, Urn targetUrn) {
    // Decide whether the current principal should be allowed to update the asset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.PROPOSE_ENTITY_OWNERS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptProposal(
      @Nonnull QueryContext context, ActionRequestType type, Urn targetUrn, String subResource) {
    if (type.equals(ActionRequestType.TAG_ASSOCIATION)) {
      return isAuthorizedToAcceptTags(context, targetUrn, subResource);
    }
    if (type.equals(ActionRequestType.TERM_ASSOCIATION)) {
      return isAuthorizedToAcceptTerms(context, targetUrn, subResource);
    }
    if (type.equals(ActionRequestType.UPDATE_DESCRIPTION)) {
      return isAuthorizedToAcceptDescriptionProposals(context, targetUrn, subResource);
    }
    if (type.equals(ActionRequestType.DATA_CONTRACT)) {
      return isAuthorizedToAcceptDataContractProposals(context, targetUrn, subResource);
    }
    if (type.equals(ActionRequestType.STRUCTURED_PROPERTY_ASSOCIATION)) {
      return isAuthorizedToAcceptStructuredPropertyProposals(context, targetUrn, subResource);
    }
    if (type.equals(ActionRequestType.DOMAIN_ASSOCIATION)) {
      return isAuthorizedToAcceptDomainProposals(context, targetUrn);
    }
    if (type.equals(ActionRequestType.OWNER_ASSOCIATION)) {
      return isAuthorizedToAcceptOwnerProposals(context, targetUrn);
    }
    return false;
  }

  public static boolean isAuthorizedToAcceptTerms(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the asset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.MANAGE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()
                            : PoliciesConfig.MANAGE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptTags(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the asset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType()
                            : PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptDescriptionProposals(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    boolean isGlossaryEntity =
        targetUrn.getEntityType().equals(GLOSSARY_TERM_ENTITY_NAME)
            || targetUrn.getEntityType().equals(GLOSSARY_NODE_ENTITY_NAME);
    // Decide whether the current principal should be allowed to update the asset.
    // If you either have all entity privileges, or have the specific privileges required, you are
    // authorized.

    // NOTE: for editing column descriptions also, we are piggybacking on
    // MANAGE_ENTITY_DOCS_PROPOSALS_PRIVILEGE
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            new ArrayList<>(
                Arrays.asList(
                    ALL_PRIVILEGES_GROUP,
                    new ConjunctivePrivilegeGroup(
                        List.of(
                            PoliciesConfig.MANAGE_ENTITY_DOCS_PROPOSALS_PRIVILEGE.getType())))));

    if (isGlossaryEntity) {
      orPrivilegeGroups
          .getAuthorizedPrivilegeGroups()
          .add(
              new ConjunctivePrivilegeGroup(
                  ImmutableList.of(PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType())));
    }

    // TODO: Add these new privileges to default places like superuser as well as roles

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptDataContractProposals(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {

    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            new ArrayList<>(
                Arrays.asList(
                    ALL_PRIVILEGES_GROUP,
                    new ConjunctivePrivilegeGroup(
                        List.of(PoliciesConfig.EDIT_ENTITY_DATA_CONTRACT_PRIVILEGE.getType())))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptStructuredPropertyProposals(
      @Nonnull QueryContext context, Urn targetUrn, String subResource) {
    boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        isTargetingSchema
                            ? PoliciesConfig.MANAGE_DATASET_COL_PROPERTIES_PRIVILEGE.getType()
                            : PoliciesConfig.MANAGE_ENTITY_PROPERTIES_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptDomainProposals(
      @Nonnull QueryContext context, Urn targetUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_ENTITY_DOMAINS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptOwnerProposals(
      @Nonnull QueryContext context, Urn targetUrn) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_ENTITY_OWNERS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context, targetUrn.getEntityType(), targetUrn.toString(), orPrivilegeGroups);
  }

  public static boolean proposeTag(
      @Nonnull OperationContext opContext,
      Urn creator,
      Urn tagUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      EntityService entityService) {
    AuthorizedActors actors;

    EntitySpec spec = new EntitySpec(targetUrn.getEntityType(), targetUrn.toString());
    if (subResource != null && subResource.length() > 0) {
      actors =
          opContext
              .getAuthorizationContext()
              .getAuthorizer()
              .authorizedActors(
                  PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType(), Optional.of(spec));

      addTagToSchemaProposalsAspect(
          opContext, creator, tagUrn, targetUrn, subResource, entityService);
    } else {
      actors =
          opContext
              .getAuthorizationContext()
              .getAuthorizer()
              .authorizedActors(
                  PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType(), Optional.of(spec));

      addTagToEntityProposalsAspect(opContext, creator, tagUrn, targetUrn, entityService);
    }

    List<Urn> assignedUsers = new ArrayList<>();
    List<Urn> assignedGroups = new ArrayList<>();
    List<Urn> assignedRoles = new ArrayList<>();

    if (actors != null) {
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
      assignedRoles = actors.getRoles();
    }

    ActionRequestSnapshot snapshot =
        createTagProposalRequest(
            creator,
            assignedUsers,
            assignedGroups,
            assignedRoles,
            tagUrn,
            targetUrn,
            subResource,
            subResourceType);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    entityService.ingestEntity(opContext, entity, auditStamp);

    return true;
  }

  private static ActionRequestSnapshot createTagProposalRequest(
      Urn creator,
      List<Urn> assignedUsers,
      List<Urn> assignedGroups,
      List<Urn> assignedRoles,
      Urn tagUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple("actionRequest", uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(creator)));
    aspects.add(
        ActionRequestAspect.create(
            createActionRequestInfo(
                ActionRequestType.TAG_ASSOCIATION,
                creator,
                assignedUsers,
                assignedGroups,
                assignedRoles,
                tagUrn,
                targetUrn,
                subResource,
                subResourceType,
                null,
                null)));

    result.setAspects(aspects);

    return result;
  }

  public static boolean proposeTerm(
      @Nonnull OperationContext opContext,
      Urn creator,
      Urn termUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      @Nullable ActionRequestOrigin origin,
      @Nullable InferenceMetadataInput inferenceMetadata,
      EntityService<?> entityService) {
    AuthorizedActors actors;

    if (subResource != null && subResource.length() > 0) {
      EntitySpec spec = new EntitySpec(targetUrn.getEntityType(), targetUrn.toString());
      actors =
          opContext
              .getAuthorizationContext()
              .getAuthorizer()
              .authorizedActors(
                  PoliciesConfig.MANAGE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType(),
                  Optional.of(spec));

      addTermToSchemaProposalsAspect(
          opContext, creator, termUrn, targetUrn, subResource, entityService);
    } else {
      EntitySpec spec = new EntitySpec(targetUrn.getEntityType(), targetUrn.toString());
      actors =
          opContext
              .getAuthorizationContext()
              .getAuthorizer()
              .authorizedActors(
                  PoliciesConfig.MANAGE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType(),
                  Optional.of(spec));

      addTermToEntityProposalsAspect(opContext, creator, termUrn, targetUrn, entityService);
    }

    List<Urn> assignedUsers = new ArrayList<>();
    List<Urn> assignedGroups = new ArrayList<>();
    List<Urn> assignedRoles = new ArrayList<>();

    if (actors != null) {
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
      assignedRoles = actors.getRoles();
    }

    // if no users, groups or roles were assigned, attempt to assign the owners of the container
    if (assignedUsers.isEmpty() && assignedGroups.isEmpty() && assignedRoles.isEmpty()) {
      // also potentially fetch the owners of the dataset's container
      Container containerAspect =
          (Container)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  targetUrn.toString(),
                  CONTAINER_ASPECT_NAME,
                  entityService,
                  new Container());

      if (containerAspect.hasContainer()) {
        // the dataset has a container- fetch the container's owners
        Ownership ownership =
            (Ownership)
                EntityUtils.getAspectFromEntity(
                    opContext,
                    containerAspect.getContainer().toString(),
                    OWNERSHIP_ASPECT_NAME,
                    entityService,
                    new Ownership());
        if (ownership.hasOwners()) {
          List<Urn> usersToAdd =
              ownership.getOwners().stream()
                  .filter(owner -> owner.getOwner().getEntityType().equals(CORP_USER_ENTITY_NAME))
                  .map(owner -> owner.getOwner())
                  .collect(Collectors.toList());
          List<Urn> groupsToAdd =
              ownership.getOwners().stream()
                  .filter(owner -> owner.getOwner().getEntityType().equals(CORP_GROUP_ENTITY_NAME))
                  .map(owner -> owner.getOwner())
                  .collect(Collectors.toList());

          assignedUsers.addAll(usersToAdd);
          assignedGroups.addAll(groupsToAdd);
        }
      }
    }

    // TODO: ActionRequest is using old snapshot model, this should get updated to use
    // ingestProposal
    ActionRequestSnapshot snapshot =
        createTermProposalRequest(
            creator,
            assignedUsers,
            assignedGroups,
            assignedRoles,
            termUrn,
            targetUrn,
            subResource,
            subResourceType,
            origin,
            inferenceMetadata);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    entityService.ingestEntity(opContext, entity, auditStamp);

    return true;
  }

  private static void ingestEntityProposalsUpdate(
      @Nonnull OperationContext opContext,
      Urn creator,
      Urn targetUrn,
      EntityService<?> entityService,
      Proposals proposals) {
    final MetadataChangeProposal metadataChangeProposal =
        buildMetadataChangeProposalWithUrn(targetUrn, PROPOSALS_ASPECT_NAME, proposals);

    ingestMetadataChangeProposal(opContext, creator, entityService, metadataChangeProposal);
  }

  private static void ingestSchemaProposalsUpdate(
      @Nonnull OperationContext opContext,
      Urn creator,
      Urn targetUrn,
      EntityService<?> entityService,
      SchemaProposals schemaProposals) {
    final MetadataChangeProposal metadataChangeProposal = new MetadataChangeProposal();
    metadataChangeProposal.setAspect(GenericRecordUtils.serializeAspect(schemaProposals));
    metadataChangeProposal.setEntityUrn(targetUrn);
    metadataChangeProposal.setEntityType(DATASET_ENTITY_NAME);
    metadataChangeProposal.setAspectName(SCHEMA_PROPOSALS_ASPECT_NAME);
    metadataChangeProposal.setChangeType(ChangeType.UPSERT);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    ingestMetadataChangeProposal(opContext, creator, entityService, metadataChangeProposal);
  }

  private static void ingestMetadataChangeProposal(
      @Nonnull OperationContext opContext,
      Urn creator,
      EntityService<?> entityService,
      MetadataChangeProposal metadataChangeProposal) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    System.out.println(String.format("About to ingest %s", metadataChangeProposal));
    try {
      entityService.ingestProposal(opContext, metadataChangeProposal, auditStamp, false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to ingest %s", metadataChangeProposal), e);
    }
  }

  private static void addTagToEntityProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      Urn tagUrn,
      Urn targetUrn,
      EntityService<?> entityService) {
    Proposals proposals =
        (Proposals)
            EntityUtils.getAspectFromEntity(
                opContext,
                targetUrn.toString(),
                PROPOSALS_ASPECT_NAME,
                entityService,
                new Proposals());
    if (!proposals.hasProposedTags()) {
      proposals.setProposedTags(new UrnArray());
    }

    UrnArray tagUrnArray = proposals.getProposedTags();
    if (tagUrnArray.stream().anyMatch(proposedTag -> proposedTag.equals(tagUrn))) {
      return;
    }
    tagUrnArray.add(tagUrn);

    ingestEntityProposalsUpdate(opContext, creator, targetUrn, entityService, proposals);
  }

  private static void addTagToSchemaProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      Urn tagUrn,
      Urn targetUrn,
      String subResource,
      EntityService<?> entityService) {

    SchemaProposals schemaProposals =
        (SchemaProposals)
            EntityUtils.getAspectFromEntity(
                opContext,
                targetUrn.toString(),
                SCHEMA_PROPOSALS_ASPECT_NAME,
                entityService,
                new SchemaProposals());

    if (!schemaProposals.hasSchemaProposals()) {
      schemaProposals.setSchemaProposals(new SchemaProposalArray());
    }

    Optional<SchemaProposal> schemaProposalOptional =
        schemaProposals.getSchemaProposals().stream()
            .filter(s -> s.getFieldPath().equals(subResource))
            .findFirst();

    SchemaProposal schemaProposal;
    if (schemaProposalOptional.isPresent()) {
      schemaProposal = schemaProposalOptional.get();
    } else {
      schemaProposal = new SchemaProposal();
      schemaProposals.getSchemaProposals().add(schemaProposal);
    }
    schemaProposal.setFieldPath(subResource);

    if (!schemaProposal.hasProposedSchemaTags()) {
      schemaProposal.setProposedSchemaTags(new UrnArray());
    }

    UrnArray tagUrnArray = schemaProposal.getProposedSchemaTags();
    if (tagUrnArray.stream().anyMatch(proposedTag -> proposedTag.equals(tagUrn))) {
      return;
    }
    tagUrnArray.add(tagUrn);

    ingestSchemaProposalsUpdate(opContext, creator, targetUrn, entityService, schemaProposals);
  }

  public static void deleteTagFromEntityOrSchemaProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      List<Urn> tagUrns,
      Urn targetUrn,
      String subResource,
      EntityService<?> entityService) {
    if (subResource != null && subResource.length() > 0) {
      deleteTagFromSchemaProposalsAspect(
          opContext, creator, tagUrns, targetUrn, subResource, entityService);
    } else {
      deleteTagFromEntityProposalsAspect(opContext, creator, tagUrns, targetUrn, entityService);
    }
  }

  private static void deleteTagFromEntityProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      List<Urn> tagUrns,
      Urn targetUrn,
      EntityService<?> entityService) {
    Proposals proposals =
        (Proposals)
            EntityUtils.getAspectFromEntity(
                opContext,
                targetUrn.toString(),
                PROPOSALS_ASPECT_NAME,
                entityService,
                new Proposals());
    if (!proposals.hasProposedTags()) {
      return;
    }

    UrnArray tagUrnArray = proposals.getProposedTags();
    if (tagUrnArray == null || tagUrnArray.isEmpty()) {
      return;
    }
    tagUrnArray.removeAll(tagUrns);
    proposals.setProposedTags(tagUrnArray);

    ingestEntityProposalsUpdate(opContext, creator, targetUrn, entityService, proposals);
  }

  private static void deleteTagFromSchemaProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      List<Urn> tagUrns,
      Urn targetUrn,
      String subResource,
      EntityService<?> entityService) {
    SchemaProposals schemaProposals =
        (SchemaProposals)
            EntityUtils.getAspectFromEntity(
                opContext,
                targetUrn.toString(),
                SCHEMA_PROPOSALS_ASPECT_NAME,
                entityService,
                new SchemaProposals());
    if (!schemaProposals.hasSchemaProposals()) {
      return;
    }

    Optional<SchemaProposal> schemaProposalOptional =
        schemaProposals.getSchemaProposals().stream()
            .filter(s -> s.getFieldPath().equals(subResource))
            .findFirst();
    if (!schemaProposalOptional.isPresent()) {
      return;
    }

    SchemaProposal schemaProposal = schemaProposalOptional.get();
    UrnArray tagUrnArray = schemaProposal.getProposedSchemaTags();
    if (tagUrnArray == null || tagUrnArray.isEmpty()) {
      return;
    }
    tagUrnArray.removeAll(tagUrns);
    schemaProposal.setProposedSchemaTags(tagUrnArray);

    ingestSchemaProposalsUpdate(opContext, creator, targetUrn, entityService, schemaProposals);
  }

  private static void addTermToEntityProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      Urn termUrn,
      Urn targetUrn,
      EntityService<?> entityService) {
    Proposals proposals =
        (Proposals)
            EntityUtils.getAspectFromEntity(
                opContext,
                targetUrn.toString(),
                PROPOSALS_ASPECT_NAME,
                entityService,
                new Proposals());
    if (!proposals.hasProposedGlossaryTerms()) {
      proposals.setProposedGlossaryTerms(new UrnArray());
    }

    UrnArray glossaryTermUrnArray = proposals.getProposedGlossaryTerms();
    if (glossaryTermUrnArray.stream()
        .anyMatch(proposedGlossaryTerm -> proposedGlossaryTerm.equals(termUrn))) {
      return;
    }
    glossaryTermUrnArray.add(termUrn);

    ingestEntityProposalsUpdate(opContext, creator, targetUrn, entityService, proposals);
  }

  private static void addTermToSchemaProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      Urn termUrn,
      Urn targetUrn,
      String subResource,
      EntityService<?> entityService) {

    SchemaProposals schemaProposals =
        (SchemaProposals)
            EntityUtils.getAspectFromEntity(
                opContext,
                targetUrn.toString(),
                SCHEMA_PROPOSALS_ASPECT_NAME,
                entityService,
                new SchemaProposals());

    if (!schemaProposals.hasSchemaProposals()) {
      schemaProposals.setSchemaProposals(new SchemaProposalArray());
    }

    Optional<SchemaProposal> schemaProposalOptional =
        schemaProposals.getSchemaProposals().stream()
            .filter(s -> s.getFieldPath().equals(subResource))
            .findFirst();

    SchemaProposal schemaProposal;
    if (schemaProposalOptional.isPresent()) {
      schemaProposal = schemaProposalOptional.get();
    } else {
      schemaProposal = new SchemaProposal();
      schemaProposals.getSchemaProposals().add(schemaProposal);
    }
    schemaProposal.setFieldPath(subResource);

    if (!schemaProposal.hasProposedSchemaGlossaryTerms()) {
      schemaProposal.setProposedSchemaGlossaryTerms(new UrnArray());
    }

    UrnArray glossaryTermUrnArray = schemaProposal.getProposedSchemaGlossaryTerms();
    if (glossaryTermUrnArray.stream()
        .anyMatch(proposedGlossaryTerm -> proposedGlossaryTerm.equals(termUrn))) {
      return;
    }
    glossaryTermUrnArray.add(termUrn);

    ingestSchemaProposalsUpdate(opContext, creator, targetUrn, entityService, schemaProposals);
  }

  public static void deleteTermFromEntityOrSchemaProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      List<Urn> termUrns,
      Urn targetUrn,
      String subResource,
      EntityService<?> entityService) {
    if (subResource != null && subResource.length() > 0) {
      deleteTermFromSchemaProposalsAspect(
          opContext, creator, termUrns, targetUrn, subResource, entityService);
    } else {
      deleteTermFromEntityProposalsAspect(opContext, creator, termUrns, targetUrn, entityService);
    }
  }

  private static void deleteTermFromEntityProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      List<Urn> termUrns,
      Urn targetUrn,
      EntityService<?> entityService) {
    Proposals proposals =
        (Proposals)
            EntityUtils.getAspectFromEntity(
                opContext,
                targetUrn.toString(),
                PROPOSALS_ASPECT_NAME,
                entityService,
                new Proposals());
    if (!proposals.hasProposedGlossaryTerms()) {
      return;
    }

    UrnArray glossaryTermUrnArray = proposals.getProposedGlossaryTerms();
    if (glossaryTermUrnArray == null || glossaryTermUrnArray.isEmpty()) {
      return;
    }
    glossaryTermUrnArray.removeAll(termUrns);
    proposals.setProposedGlossaryTerms(glossaryTermUrnArray);

    ingestEntityProposalsUpdate(opContext, creator, targetUrn, entityService, proposals);
  }

  private static void deleteTermFromSchemaProposalsAspect(
      @Nonnull OperationContext opContext,
      Urn creator,
      List<Urn> termUrns,
      Urn targetUrn,
      String subResource,
      EntityService<?> entityService) {
    SchemaProposals schemaProposals =
        (SchemaProposals)
            EntityUtils.getAspectFromEntity(
                opContext,
                targetUrn.toString(),
                SCHEMA_PROPOSALS_ASPECT_NAME,
                entityService,
                new SchemaProposals());
    if (!schemaProposals.hasSchemaProposals()) {
      return;
    }

    Optional<SchemaProposal> schemaProposalOptional =
        schemaProposals.getSchemaProposals().stream()
            .filter(s -> s.getFieldPath().equals(subResource))
            .findFirst();
    if (!schemaProposalOptional.isPresent()) {
      return;
    }

    SchemaProposal schemaProposal = schemaProposalOptional.get();

    UrnArray glossaryTermUrnArray = schemaProposal.getProposedSchemaGlossaryTerms();
    if (glossaryTermUrnArray == null || glossaryTermUrnArray.isEmpty()) {
      return;
    }
    glossaryTermUrnArray.removeAll(termUrns);
    schemaProposal.setProposedSchemaGlossaryTerms(glossaryTermUrnArray);

    ingestSchemaProposalsUpdate(opContext, creator, targetUrn, entityService, schemaProposals);
  }

  private static ActionRequestSnapshot createTermProposalRequest(
      Urn creator,
      List<Urn> assignedUsers,
      List<Urn> assignedGroups,
      List<Urn> assignedRoles,
      Urn termUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      @Nullable ActionRequestOrigin origin,
      @Nullable InferenceMetadataInput inferenceMetadata) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple("actionRequest", uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(creator)));
    aspects.add(
        ActionRequestAspect.create(
            createActionRequestInfo(
                ActionRequestType.TERM_ASSOCIATION,
                creator,
                assignedUsers,
                assignedGroups,
                assignedRoles,
                termUrn,
                targetUrn,
                subResource,
                subResourceType,
                origin,
                inferenceMetadata)));

    result.setAspects(aspects);

    return result;
  }

  public static ActionRequestStatus createActionRequestStatus(Urn creator) {
    final ActionRequestStatus status = new ActionRequestStatus();

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    status.setStatus("PENDING");
    status.setResult("");
    status.setLastModified(auditStamp);

    return status;
  }

  public static ActionRequestInfo createActionRequestInfo(
      ActionRequestType type,
      Urn creator,
      List<Urn> assignedUsers,
      List<Urn> assignedGroups,
      List<Urn> assignedRoles,
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType,
      @Nullable ActionRequestOrigin origin,
      @Nullable InferenceMetadataInput inferenceMetadata) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(type.toString());
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setAssignedRoles(new UrnArray(assignedRoles));
    info.setResourceType(targetUrn.getEntityType());
    info.setResource(targetUrn.toString());
    if (subResourceType != null) {
      info.setSubResourceType(subResourceType.toString());
    }
    if (subResource != null) {
      info.setSubResource(subResource);
    }
    if (origin != null) {
      info.setOrigin(com.linkedin.actionrequest.ActionRequestOrigin.valueOf(origin.toString()));
    }
    if (inferenceMetadata != null) {
      info.setInferenceMetadata(mapInferenceMetadata(inferenceMetadata));
    }
    info.setParams(createActionRequestParams(labelUrn));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(creator);

    return info;
  }

  public static ActionRequestParams createActionRequestParams(Urn label) {
    ActionRequestParams params = new ActionRequestParams();

    if (label.getEntityType().equals("tag")) {
      TagProposal proposal = new TagProposal();
      proposal.setTag(label);
      params.setTagProposal(proposal);
    }

    if (label.getEntityType().equals("glossaryTerm")) {
      GlossaryTermProposal proposal = new GlossaryTermProposal();
      proposal.setGlossaryTerm(label);
      params.setGlossaryTermProposal(proposal);
    }
    return params;
  }

  public static ActionRequestSnapshot setStatusSnapshot(
      Urn actor,
      com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      ActionRequestResult result,
      Entity proposalEntity) {
    Optional<ActionRequestAspect> actionRequestStatusWrapper =
        proposalEntity.getValue().getActionRequestSnapshot().getAspects().stream()
            .filter(actionRequestAspect -> actionRequestAspect.isActionRequestStatus())
            .findFirst();

    if (!actionRequestStatusWrapper.isPresent()) {
      actionRequestStatusWrapper =
          Optional.of(ActionRequestAspect.create(ProposalUtils.createActionRequestStatus(actor)));
    }

    ActionRequestStatus statusAspect = actionRequestStatusWrapper.get().getActionRequestStatus();
    statusAspect.setStatus(status.toString());
    statusAspect.setResult(result.toString());

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actor, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    statusAspect.setLastModified(auditStamp);

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(statusAspect));

    ActionRequestSnapshot snapshot = new ActionRequestSnapshot();
    snapshot.setUrn(proposalEntity.getValue().getActionRequestSnapshot().getUrn());
    snapshot.setAspects(aspects);

    return snapshot;
  }

  @SneakyThrows
  public static Boolean isTagAlreadyProposedToTarget(
      @Nonnull OperationContext opContext,
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityClient entityClient) {
    Filter filter =
        createActionRequestFilter(
            ActionRequestType.TAG_ASSOCIATION,
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING,
            targetUrn.toString(),
            subResource);

    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
            .filter(
                actionRequestInfo ->
                    (subResource != null
                            ? actionRequestInfo.getSubResource().equals(subResource)
                            : !actionRequestInfo.hasSubResource())
                        && actionRequestInfo.getResource().equals(targetUrn.toString())
                        && actionRequestInfo.getParams().hasTagProposal()
                        && actionRequestInfo.getParams().getTagProposal().getTag().equals(labelUrn))
            .count()
        > 0;
  }

  @SneakyThrows
  public static Boolean isTermAlreadyProposedToTarget(
      @Nonnull OperationContext opContext,
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityClient entityClient) {
    Filter filter =
        createActionRequestFilter(
            ActionRequestType.TERM_ASSOCIATION,
            com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING,
            targetUrn.toString(),
            subResource);

    return getActionRequestInfosFromFilter(opContext, filter, entityClient)
            .filter(
                actionRequestInfo ->
                    (subResource != null
                            ? actionRequestInfo.getSubResource().equals(subResource)
                            : !actionRequestInfo.hasSubResource())
                        && actionRequestInfo.getResource().equals(targetUrn.toString())
                        && actionRequestInfo.getParams().hasGlossaryTermProposal()
                        && actionRequestInfo
                            .getParams()
                            .getGlossaryTermProposal()
                            .getGlossaryTerm()
                            .equals(labelUrn))
            .count()
        > 0;
  }

  public static Stream<ActionRequestInfo> getActionRequestInfosFromFilter(
      @Nonnull OperationContext opContext, Filter filter, EntityClient entityClient)
      throws RemoteInvocationException {
    final SearchResult searchResult =
        entityClient.filter(opContext, ACTION_REQUEST_ENTITY_NAME, filter, null, 0, 20);
    final Map<Urn, Entity> entities =
        entityClient.batchGet(
            opContext,
            new HashSet<>(
                searchResult.getEntities().stream()
                    .map(result -> result.getEntity())
                    .collect(Collectors.toList())));

    return entities.values().stream()
        .map(
            entity ->
                entity.getValue().getActionRequestSnapshot().getAspects().stream()
                    .filter(aspect -> aspect.isActionRequestInfo())
                    .map(aspect -> aspect.getActionRequestInfo())
                    .findFirst())
        .filter(maybeActionRequestInfo -> maybeActionRequestInfo.isPresent())
        .map(maybeActionRequestInfo -> maybeActionRequestInfo.get());
  }

  public static Boolean isTagAlreadyAttachedToTarget(
      @Nonnull OperationContext opContext,
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityService<?> entityService) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlobalTags tags =
          (com.linkedin.common.GlobalTags)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  targetUrn.toString(),
                  GLOBAL_TAGS_ASPECT_NAME,
                  entityService,
                  new GlobalTags());

      if (!tags.hasTags()) {
        return false;
      }

      return doesTagsListContainTag(tags, labelUrn);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  targetUrn.toString(),
                  EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                  entityService,
                  new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo =
          getFieldInfoFromSchema(editableSchemaMetadata, subResource);

      if (!editableFieldInfo.hasGlobalTags()) {
        return false;
      }

      return doesTagsListContainTag(editableFieldInfo.getGlobalTags(), labelUrn);
    }
  }

  public static Boolean isTermAlreadyAttachedToTarget(
      @Nonnull OperationContext opContext,
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityService<?> entityService) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  targetUrn.toString(),
                  GLOSSARY_TERMS_ASPECT_NAME,
                  entityService,
                  new GlossaryTerms());

      if (!terms.hasTerms()) {
        return false;
      }

      return doesTermsListContainTerm(terms, labelUrn);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata)
              EntityUtils.getAspectFromEntity(
                  opContext,
                  targetUrn.toString(),
                  EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
                  entityService,
                  new EditableSchemaMetadata());

      EditableSchemaFieldInfo editableFieldInfo =
          getFieldInfoFromSchema(editableSchemaMetadata, subResource);
      if (!editableFieldInfo.hasGlossaryTerms()) {
        return false;
      }

      return doesTermsListContainTerm(editableFieldInfo.getGlossaryTerms(), labelUrn);
    }
  }

  private static Boolean doesTermsListContainTerm(GlossaryTerms terms, Urn termUrn) {
    if (!terms.hasTerms()) {
      return false;
    }

    GlossaryTermAssociationArray termArray = terms.getTerms();
    return termArray.stream().anyMatch(association -> association.getUrn().equals(termUrn));
  }

  private static Boolean doesTagsListContainTag(GlobalTags tags, Urn tagUrn) {
    if (!tags.hasTags()) {
      return false;
    }

    TagAssociationArray tagAssociationArray = tags.getTags();
    return tagAssociationArray.stream()
        .anyMatch(association -> association.getTag().equals(tagUrn));
  }

  public static Filter createActionRequestFilter(
      final @Nullable ActionRequestType type,
      final @Nullable com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      final @Nonnull String targetUrn,
      final @Nullable String targetSubresource) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();
    if (status != null) {
      andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
    }
    if (type != null) {
      andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
    }
    if (targetSubresource != null) {
      andCriterion.add(ActionRequestUtils.createSubResourceCriterion(targetSubresource));
    }
    andCriterion.add(ActionRequestUtils.createResourceCriterion(targetUrn));

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);

    filter.setOr(disjunction);
    return filter;
  }

  public static Filter createFilter(
      final @Nullable Urn actorUrn,
      final @Nullable List<Urn> groupUrns,
      final @Nullable List<Urn> roleUrns,
      final @Nullable ActionRequestType type,
      final @Nullable com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      final @Nullable Urn resourceUrn,
      final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();

    if (hasAssigneeFilters(actorUrn, groupUrns, roleUrns)) {
      addAssigneeFilters(
          disjunction,
          actorUrn,
          groupUrns,
          roleUrns,
          type,
          status,
          resourceUrn,
          startTimestampMillis,
          endTimestampMillis);
    } else {
      addNonAssigneeFilters(
          disjunction, type, status, resourceUrn, startTimestampMillis, endTimestampMillis);
    }

    filter.setOr(disjunction);
    return filter;
  }

  public static boolean hasAssigneeFilters(Urn actorUrn, List<Urn> groupUrns, List<Urn> roleUrns) {
    return actorUrn != null
        || (groupUrns != null && !groupUrns.isEmpty())
        || (roleUrns != null && !roleUrns.isEmpty());
  }

  public static void addAssigneeFilters(
      ConjunctiveCriterionArray disjunction,
      Urn actorUrn,
      List<Urn> groupUrns,
      List<Urn> roleUrns,
      ActionRequestType type,
      com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      Urn resourceUrn,
      Long startTimestampMillis,
      Long endTimestampMillis) {
    if (actorUrn != null) {
      disjunction.add(
          createUserFilterConjunction(
              actorUrn, type, status, resourceUrn, startTimestampMillis, endTimestampMillis));
    }
    if (groupUrns != null) {
      disjunction.addAll(
          createGroupFilterDisjunction(
              groupUrns, type, status, resourceUrn, startTimestampMillis, endTimestampMillis));
    }
    if (roleUrns != null) {
      disjunction.addAll(
          createRoleFilterDisjunction(
              roleUrns, type, status, resourceUrn, startTimestampMillis, endTimestampMillis));
    }
  }

  public static void addNonAssigneeFilters(
      ConjunctiveCriterionArray disjunction,
      ActionRequestType type,
      com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      Urn resourceUrn,
      Long startTimestampMillis,
      Long endTimestampMillis) {
    if (hasNonAssigneeFilters(
        type, status, resourceUrn, startTimestampMillis, endTimestampMillis)) {
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();

      if (status != null) {
        andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
      }
      if (type != null) {
        andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
      }
      if (resourceUrn != null) {
        andCriterion.add(ActionRequestUtils.createResourceCriterion(resourceUrn.toString()));
      }
      if (startTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
      }
      if (endTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
      }

      conjunction.setAnd(andCriterion);
      disjunction.add(conjunction);
    }
  }

  public static boolean hasNonAssigneeFilters(
      ActionRequestType type,
      com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      Urn resourceUrn,
      Long startTimestampMillis,
      Long endTimestampMillis) {
    return status != null
        || type != null
        || resourceUrn != null
        || startTimestampMillis != null
        || endTimestampMillis != null;
  }

  public static <T> void addCriterionIfNotNull(
      CriterionArray criteria,
      T value,
      java.util.function.Function<T, Criterion> criterionCreator) {
    if (value != null) {
      criteria.add(criterionCreator.apply(value));
    }
  }

  public static ConjunctiveCriterion createUserFilterConjunction(
      final Urn userUrn,
      final @Nullable ActionRequestType type,
      final @Nullable com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      final @Nullable Urn resourceUrn,
      final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();
    andCriterion.add(
        CriterionUtils.buildCriterion(
            ASSIGNED_USERS_FIELD_NAME + ".keyword", Condition.EQUAL, userUrn.toString()));
    if (status != null) {
      andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
    }
    if (type != null) {
      andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
    }
    if (resourceUrn != null) {
      andCriterion.add(ActionRequestUtils.createResourceCriterion(resourceUrn.toString()));
    }
    if (startTimestampMillis != null) {
      andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
    }
    if (endTimestampMillis != null) {
      andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
    }
    conjunction.setAnd(andCriterion);
    return conjunction;
  }

  public static List<ConjunctiveCriterion> createGroupFilterDisjunction(
      final List<Urn> groupUrns,
      final @Nullable ActionRequestType type,
      final @Nullable com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      final @Nullable Urn resourceUrn,
      final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final List<ConjunctiveCriterion> disjunction = new ArrayList<>();
    // Create a new filter for each group urn, where the urn, type, and status must all match.
    for (Urn groupUrn : groupUrns) {
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();
      andCriterion.add(
          CriterionUtils.buildCriterion(
              ASSIGNED_GROUPS_FIELD_NAME + ".keyword", Condition.EQUAL, groupUrn.toString()));
      if (status != null) {
        andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
      }
      if (type != null) {
        andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
      }
      if (resourceUrn != null) {
        andCriterion.add(ActionRequestUtils.createResourceCriterion(resourceUrn.toString()));
      }
      if (startTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
      }
      if (endTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
      }
      conjunction.setAnd(andCriterion);
      disjunction.add(conjunction);
    }
    return disjunction;
  }

  public static List<ConjunctiveCriterion> createRoleFilterDisjunction(
      final List<Urn> roleUrns,
      final @Nullable ActionRequestType type,
      final @Nullable com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      final @Nullable Urn resourceUrn,
      final @Nullable Long startTimestampMillis,
      final @Nullable Long endTimestampMillis) {
    final List<ConjunctiveCriterion> disjunction = new ArrayList<>();
    // Create a new filter for each role urn, where the urn, type, and status must all match.
    for (Urn groupUrn : roleUrns) {
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();
      andCriterion.add(
          CriterionUtils.buildCriterion(
              ASSIGNED_ROLES_FIELD_NAME + ".keyword", Condition.EQUAL, groupUrn.toString()));
      if (status != null) {
        andCriterion.add(ActionRequestUtils.createStatusCriterion(status));
      }
      if (type != null) {
        andCriterion.add(ActionRequestUtils.createTypeCriterion(type));
      }
      if (resourceUrn != null) {
        andCriterion.add(ActionRequestUtils.createResourceCriterion(resourceUrn.toString()));
      }
      if (startTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createStartTimestampCriterion(startTimestampMillis));
      }
      if (endTimestampMillis != null) {
        andCriterion.add(ActionRequestUtils.createEndTimestampCriterion(endTimestampMillis));
      }
      conjunction.setAnd(andCriterion);
      disjunction.add(conjunction);
    }
    return disjunction;
  }

  public static void validateProposalUrns(@Nonnull final Set<Urn> proposalUrns) {
    // First, validate that we have a valid Action Request.
    for (final Urn proposalUrn : proposalUrns) {
      if (!proposalUrn.getEntityType().equals(ACTION_REQUEST_ENTITY_NAME)) {
        throw new RuntimeException(
            String.format(
                "Failed to act on proposal, Urn provided (%s) is not a valid Action Request urn",
                proposalUrn));
      }
    }
  }

  public static void validateProposals(
      @Nonnull final Collection<Entity> entities,
      @Nonnull final QueryContext context,
      @Nonnull final ActionRequestService proposalService) {
    for (final Entity proposal : entities) {
      validateProposal(proposal, context, proposalService);
    }
  }

  private static void validateProposal(
      @Nonnull final Entity proposalEntity,
      @Nonnull final QueryContext context,
      @Nonnull final ActionRequestService proposalService) {
    // TODO: Migrate away from using deprecated 'snapshot' entities here.
    final ActionRequestSnapshot actionRequestSnapshot =
        proposalEntity.getValue().getActionRequestSnapshot();

    final ActionRequestStatus actionRequestStatus =
        extractActionRequestStatus(actionRequestSnapshot);

    final ActionRequestInfo actionRequestInfo = extractActionRequestInfo(actionRequestSnapshot);

    if (actionRequestStatus == null || actionRequestInfo == null) {
      throw new DataHubGraphQLException(
          String.format(
              "Unable to validate proposal. Provided urn %s does not exist!",
              proposalEntity.getValue().getActionRequestSnapshot().getUrn()),
          DataHubGraphQLErrorCode.NOT_FOUND);
    }

    // Validate each type slightly separately.
    if (!isAuthorizedToAcceptProposal(actionRequestSnapshot, context, proposalService)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
  }

  private static boolean isAuthorizedToAcceptProposal(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      @Nonnull final QueryContext context,
      @Nonnull final ActionRequestService proposalService) {

    final ActionRequestInfo actionRequestInfo = extractActionRequestInfo(actionRequestSnapshot);

    final Urn actor = UrnUtils.getUrn(context.getActorUrn());
    final String subResource = actionRequestInfo.getSubResource();
    final String actionRequestType = actionRequestInfo.getType();

    switch (actionRequestInfo.getType()) {
      case ACTION_REQUEST_TYPE_TAG_PROPOSAL:
      case ACTION_REQUEST_TYPE_TERM_PROPOSAL:
      case ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL:
      case ACTION_REQUEST_TYPE_DATA_CONTRACT_PROPOSAL:
      case ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL:
      case ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL:
      case ACTION_REQUEST_TYPE_OWNER_PROPOSAL:
        return ProposalUtils.isAuthorizedToAcceptProposal(
            context,
            ActionRequestType.valueOf(actionRequestType),
            UrnUtils.getUrn(actionRequestInfo.getResource()),
            subResource);
        // Glossary Node Proposal Case is a bit more complex. We need to check the parent glossary
        // nodes at every level,
        // which happens below.
      case ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL:
        return proposalService.canResolveGlossaryNodeProposal(
            context.getOperationContext(),
            actor,
            actionRequestSnapshot,
            GlossaryUtils.canManageGlossaries(context));
      case ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL:
        return proposalService.canResolveGlossaryTermProposal(
            context.getOperationContext(),
            actor,
            actionRequestSnapshot,
            GlossaryUtils.canManageGlossaries(context));
      default:
        log.warn(
            String.format(
                "Unrecognized action request type %s provided. Denying authorization for unknown action!",
                actionRequestInfo.getType()));
        return false;
    }
  }

  @Nullable
  public static ActionRequestInfo extractActionRequestInfo(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot) {
    for (ActionRequestAspect aspect : actionRequestSnapshot.getAspects()) {
      if (aspect.isActionRequestInfo()) {
        return aspect.getActionRequestInfo();
      }
    }
    return null;
  }

  @Nullable
  public static ActionRequestStatus extractActionRequestStatus(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot) {
    for (ActionRequestAspect aspect : actionRequestSnapshot.getAspects()) {
      if (aspect.isActionRequestStatus()) {
        return aspect.getActionRequestStatus();
      }
    }
    return null;
  }

  public static com.linkedin.ai.InferenceMetadata mapInferenceMetadata(
      @Nonnull final InferenceMetadataInput input) {
    com.linkedin.ai.InferenceMetadata metadata = new com.linkedin.ai.InferenceMetadata();
    metadata.setVersion(input.getVersion());
    if (input.getConfidenceLevel() != null) {
      metadata.setConfidenceLevel(input.getConfidenceLevel());
    }
    if (input.getLastInferredAt() != null) {
      metadata.setLastInferredAt(input.getLastInferredAt());
    } else {
      metadata.setLastInferredAt(System.currentTimeMillis());
    }
    return metadata;
  }

  public static List<Urn> toUrns(List<String> urnStrs) {
    return urnStrs.stream().map(ProposalUtils::toUrn).collect(Collectors.toList());
  }

  public static Urn toUrn(String urnStr) {
    return UrnUtils.getUrn(urnStr);
  }

  private ProposalUtils() {}
}
