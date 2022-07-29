package com.linkedin.datahub.graphql.resolvers.proposal;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.Authorizer;
import com.datahub.authorization.ResourceSpec;
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
import com.linkedin.container.Container;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.generated.ActionRequestResult;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.LabelUtils;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaProposal;
import com.linkedin.schema.SchemaProposalArray;
import com.linkedin.schema.SchemaProposals;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

@Slf4j
// TODO(Gabe): Unit test this file
public class ProposalUtils {
  public static final String PROPOSALS_ASPECT_NAME = "proposals";
  public static final String SCHEMA_PROPOSALS_ASPECT_NAME = "schemaProposals";
  public static final String CONTAINER_ASPECT_NAME = "container";

  private static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP = new ConjunctivePrivilegeGroup(ImmutableList.of(
      PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()
  ));

  public static boolean isAuthorizedToProposeTags(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.PROPOSE_DATASET_COL_TAGS_PRIVILEGE.getType()
            : PoliciesConfig.PROPOSE_ENTITY_TAGS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToProposeTerms(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.PROPOSE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()
            : PoliciesConfig.PROPOSE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptProposal(
      @Nonnull QueryContext context,
      ActionRequestType type,
      Urn targetUrn,
      String subResource
  ) {
    if (type.equals(ActionRequestType.TAG_ASSOCIATION)) {
      return isAuthorizedToAcceptTags(context, targetUrn, subResource);
    }
    if (type.equals(ActionRequestType.TERM_ASSOCIATION)) {
      return isAuthorizedToAcceptTerms(context, targetUrn, subResource);
    }

    return false;
  }

  public static boolean isAuthorizedToAcceptTerms(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.MANAGE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType()
            : PoliciesConfig.MANAGE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean isAuthorizedToAcceptTags(@Nonnull QueryContext context, Urn targetUrn, String subResource) {

    Boolean isTargetingSchema = subResource != null && subResource.length() > 0;
    // Decide whether the current principal should be allowed to update the Dataset.
    // If you either have all entity privileges, or have the specific privileges required, you are authorized.
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(isTargetingSchema
            ? PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType()
            : PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        targetUrn.getEntityType(),
        targetUrn.toString(),
        orPrivilegeGroups);
  }

  public static boolean proposeTag(Urn creator, Urn tagUrn, Urn targetUrn, String subResource,
      SubResourceType subResourceType, EntityService entityService, Authorizer dataHubAuthorizer) {
    AuthorizedActors actors;

    ResourceSpec spec = new ResourceSpec(targetUrn.getEntityType(), targetUrn.toString());
    if (subResource != null && subResource.length() > 0) {
      actors = dataHubAuthorizer.authorizedActors(PoliciesConfig.MANAGE_DATASET_COL_TAGS_PRIVILEGE.getType(),
          Optional.of(spec));

      addTagToSchemaProposalsAspect(creator, tagUrn, targetUrn, subResource, entityService);
    } else {
      actors = dataHubAuthorizer.authorizedActors(
          PoliciesConfig.MANAGE_ENTITY_TAGS_PRIVILEGE.getType(),
          Optional.of(spec)
      );

      addTagToEntityProposalsAspect(creator, tagUrn, targetUrn, entityService);
    }

    List<Urn> assignedUsers = new ArrayList<>();
    List<Urn> assignedGroups = new ArrayList<>();

    if (actors != null) {
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
    }

    ActionRequestSnapshot snapshot = createTagProposalRequest(
        creator,
        assignedUsers,
        assignedGroups,
        tagUrn,
        targetUrn,
        subResource,
        subResourceType
    );

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  private static ActionRequestSnapshot createTagProposalRequest(
      Urn creator,
      List<Urn> assignedUsers,
      List<Urn> assignedGroups,
      Urn tagUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType
  ) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple("actionRequest", uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(creator)));
    aspects.add(ActionRequestAspect.create(createActionRequestInfo(
        ActionRequestType.TAG_ASSOCIATION,
        creator,
        assignedUsers,
        assignedGroups,
        tagUrn, targetUrn, subResource, subResourceType)));

    result.setAspects(aspects);

    return result;
  }

  public static boolean proposeTerm(Urn creator, Urn termUrn, Urn targetUrn, String subResource,
      SubResourceType subResourceType, EntityService entityService, Authorizer dataHubAuthorizer) {
    AuthorizedActors actors;

    if (subResource != null && subResource.length() > 0) {
      ResourceSpec spec = new ResourceSpec(targetUrn.getEntityType(), targetUrn.toString());
      actors = dataHubAuthorizer.authorizedActors(PoliciesConfig.MANAGE_DATASET_COL_GLOSSARY_TERMS_PRIVILEGE.getType(),
          Optional.of(spec));

      addTermToSchemaProposalsAspect(creator, termUrn, targetUrn, subResource, entityService);
    } else {
      ResourceSpec spec = new ResourceSpec(targetUrn.getEntityType(), targetUrn.toString());
      actors = dataHubAuthorizer.authorizedActors(
          PoliciesConfig.MANAGE_ENTITY_GLOSSARY_TERMS_PRIVILEGE.getType(),
          Optional.of(spec)
      );

      addTermToEntityProposalsAspect(creator, termUrn, targetUrn, entityService);
    }

    List<Urn> assignedUsers = new ArrayList<>();
    List<Urn> assignedGroups = new ArrayList<>();

    if (actors != null) {
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
    }

    // if no users or groups were assigned, attempt to assign the owners of the container
    if (assignedUsers.isEmpty() && assignedGroups.isEmpty()) {
      // also potentially fetch the owners of the dataset's container
      Container containerAspect =
          (Container) getAspectFromEntity(targetUrn.toString(), CONTAINER_ASPECT_NAME, entityService, new Container());

      if (containerAspect.hasContainer()) {
        // the dataset has a container- fetch the container's owners
        Ownership ownership =
            (Ownership) getAspectFromEntity(containerAspect.getContainer().toString(), OWNERSHIP_ASPECT_NAME, entityService, new Ownership());
        if (ownership.hasOwners()) {
          List<Urn> usersToAdd = ownership.getOwners().stream()
              .filter(owner -> owner.getOwner().getEntityType().equals(CORP_USER_ENTITY_NAME))
              .map(owner -> owner.getOwner())
              .collect(Collectors.toList());
          List<Urn> groupsToAdd = ownership.getOwners().stream()
              .filter(owner -> owner.getOwner().getEntityType().equals(CORP_GROUP_ENTITY_NAME))
              .map(owner -> owner.getOwner())
              .collect(Collectors.toList());

          assignedUsers.addAll(usersToAdd);
          assignedGroups.addAll(groupsToAdd);
        }
      }
    }

    ActionRequestSnapshot snapshot = createTermProposalRequest(
        creator,
        assignedUsers,
        assignedGroups,
        termUrn,
        targetUrn,
        subResource,
        subResourceType
    );

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  private static void ingestEntityProposalsUpdate(Urn creator, Urn targetUrn, EntityService entityService,
      Proposals proposals) {
    final MetadataChangeProposal metadataChangeProposal = new MetadataChangeProposal();
    metadataChangeProposal.setAspect(GenericRecordUtils.serializeAspect(proposals));
    metadataChangeProposal.setEntityUrn(targetUrn);
    metadataChangeProposal.setEntityType(DATASET_ENTITY_NAME);
    metadataChangeProposal.setAspectName(PROPOSALS_ASPECT_NAME);
    metadataChangeProposal.setChangeType(ChangeType.UPSERT);

    ingestMetadataChangeProposal(creator, entityService, metadataChangeProposal);
  }

  private static void ingestSchemaProposalsUpdate(Urn creator, Urn targetUrn, EntityService entityService,
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

    ingestMetadataChangeProposal(creator, entityService, metadataChangeProposal);
  }

  private static void ingestMetadataChangeProposal(Urn creator, EntityService entityService,
      MetadataChangeProposal metadataChangeProposal) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(creator, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    System.out.println(String.format("About to ingest %s", metadataChangeProposal));
    try {
      entityService.ingestProposal(metadataChangeProposal, auditStamp);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to ingest %s", metadataChangeProposal), e);
    }
  }

  private static void addTagToEntityProposalsAspect(Urn creator, Urn tagUrn, Urn targetUrn,
      EntityService entityService) {
    Proposals proposals =
        (Proposals) getAspectFromEntity(targetUrn.toString(), PROPOSALS_ASPECT_NAME, entityService, new Proposals());
    if (!proposals.hasProposedTags()) {
      proposals.setProposedTags(new UrnArray());
    }

    UrnArray tagUrnArray = proposals.getProposedTags();
    if (tagUrnArray.stream().anyMatch(proposedTag -> proposedTag.equals(tagUrn))) {
      return;
    }
    tagUrnArray.add(tagUrn);

    ingestEntityProposalsUpdate(creator, targetUrn, entityService, proposals);
  }

  private static void addTagToSchemaProposalsAspect(Urn creator, Urn tagUrn, Urn targetUrn, String subResource,
      EntityService entityService) {

    SchemaProposals schemaProposals =
        (SchemaProposals) getAspectFromEntity(targetUrn.toString(), SCHEMA_PROPOSALS_ASPECT_NAME, entityService,
            new SchemaProposals());

    if (!schemaProposals.hasSchemaProposals()) {
      schemaProposals.setSchemaProposals(new SchemaProposalArray());
    }

    Optional<SchemaProposal> schemaProposalOptional =
        schemaProposals.getSchemaProposals().stream().filter(s -> s.getFieldPath().equals(subResource)).findFirst();

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

    ingestSchemaProposalsUpdate(creator, targetUrn, entityService, schemaProposals);
  }

  public static void deleteTagFromEntityOrSchemaProposalsAspect(Urn creator, Urn tagUrn, Urn targetUrn,
      String subResource, EntityService entityService) {
    if (subResource != null && subResource.length() > 0) {
      deleteTagFromSchemaProposalsAspect(creator, tagUrn, targetUrn, subResource, entityService);
    } else {
      deleteTagFromEntityProposalsAspect(creator, tagUrn, targetUrn, entityService);
    }
  }

  private static void deleteTagFromEntityProposalsAspect(Urn creator, Urn tagUrn, Urn targetUrn,
      EntityService entityService) {
    Proposals proposals =
        (Proposals) getAspectFromEntity(targetUrn.toString(), PROPOSALS_ASPECT_NAME, entityService, new Proposals());
    if (!proposals.hasProposedTags()) {
      return;
    }

    UrnArray tagUrnArray = proposals.getProposedTags();
    tagUrnArray.remove(tagUrn);
    proposals.setProposedTags(tagUrnArray);

    ingestEntityProposalsUpdate(creator, targetUrn, entityService, proposals);
  }

  private static void deleteTagFromSchemaProposalsAspect(Urn creator, Urn tagUrn, Urn targetUrn, String subResource,
      EntityService entityService) {
    SchemaProposals schemaProposals =
        (SchemaProposals) getAspectFromEntity(targetUrn.toString(), SCHEMA_PROPOSALS_ASPECT_NAME, entityService,
            new SchemaProposals());
    if (!schemaProposals.hasSchemaProposals()) {
      return;
    }

    Optional<SchemaProposal> schemaProposalOptional =
        schemaProposals.getSchemaProposals().stream().filter(s -> s.getFieldPath().equals(subResource)).findFirst();
    if (!schemaProposalOptional.isPresent()) {
      return;
    }

    SchemaProposal schemaProposal = schemaProposalOptional.get();
    UrnArray tagUrnArray = schemaProposal.getProposedSchemaTags();
    tagUrnArray.remove(tagUrn);
    schemaProposal.setProposedSchemaTags(tagUrnArray);

    ingestSchemaProposalsUpdate(creator, targetUrn, entityService, schemaProposals);
  }

  private static void addTermToEntityProposalsAspect(Urn creator, Urn termUrn, Urn targetUrn,
      EntityService entityService) {
    Proposals proposals =
        (Proposals) getAspectFromEntity(targetUrn.toString(), PROPOSALS_ASPECT_NAME, entityService, new Proposals());
    if (!proposals.hasProposedGlossaryTerms()) {
      proposals.setProposedGlossaryTerms(new UrnArray());
    }

    UrnArray glossaryTermUrnArray = proposals.getProposedGlossaryTerms();
    if (glossaryTermUrnArray.stream().anyMatch(proposedGlossaryTerm -> proposedGlossaryTerm.equals(termUrn))) {
      return;
    }
    glossaryTermUrnArray.add(termUrn);

    ingestEntityProposalsUpdate(creator, targetUrn, entityService, proposals);
  }

  private static void addTermToSchemaProposalsAspect(Urn creator, Urn termUrn, Urn targetUrn, String subResource,
      EntityService entityService) {

    SchemaProposals schemaProposals =
        (SchemaProposals) getAspectFromEntity(targetUrn.toString(), SCHEMA_PROPOSALS_ASPECT_NAME, entityService,
            new SchemaProposals());

    if (!schemaProposals.hasSchemaProposals()) {
      schemaProposals.setSchemaProposals(new SchemaProposalArray());
    }

    Optional<SchemaProposal> schemaProposalOptional =
        schemaProposals.getSchemaProposals().stream().filter(s -> s.getFieldPath().equals(subResource)).findFirst();

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
    if (glossaryTermUrnArray.stream().anyMatch(proposedGlossaryTerm -> proposedGlossaryTerm.equals(termUrn))) {
      return;
    }
    glossaryTermUrnArray.add(termUrn);

    ingestSchemaProposalsUpdate(creator, targetUrn, entityService, schemaProposals);
  }

  public static void deleteTermFromEntityOrSchemaProposalsAspect(Urn creator, Urn termUrn, Urn targetUrn,
      String subResource, EntityService entityService) {
    if (subResource != null && subResource.length() > 0) {
      deleteTermFromSchemaProposalsAspect(creator, termUrn, targetUrn, subResource, entityService);
    } else {
      deleteTermFromEntityProposalsAspect(creator, termUrn, targetUrn, entityService);
    }
  }

  private static void deleteTermFromEntityProposalsAspect(Urn creator, Urn termUrn, Urn targetUrn,
      EntityService entityService) {
    Proposals proposals =
        (Proposals) getAspectFromEntity(targetUrn.toString(), PROPOSALS_ASPECT_NAME, entityService, new Proposals());
    if (!proposals.hasProposedGlossaryTerms()) {
      return;
    }

    UrnArray glossaryTermUrnArray = proposals.getProposedGlossaryTerms();
    glossaryTermUrnArray.remove(termUrn);
    proposals.setProposedGlossaryTerms(glossaryTermUrnArray);

    ingestEntityProposalsUpdate(creator, targetUrn, entityService, proposals);
  }

  private static void deleteTermFromSchemaProposalsAspect(Urn creator, Urn termUrn, Urn targetUrn, String subResource,
      EntityService entityService) {
    SchemaProposals schemaProposals =
        (SchemaProposals) getAspectFromEntity(targetUrn.toString(), SCHEMA_PROPOSALS_ASPECT_NAME, entityService,
            new SchemaProposals());
    if (!schemaProposals.hasSchemaProposals()) {
      return;
    }

    Optional<SchemaProposal> schemaProposalOptional =
        schemaProposals.getSchemaProposals().stream().filter(s -> s.getFieldPath().equals(subResource)).findFirst();
    if (!schemaProposalOptional.isPresent()) {
      return;
    }

    SchemaProposal schemaProposal = schemaProposalOptional.get();

    UrnArray glossaryTermUrnArray = schemaProposal.getProposedSchemaGlossaryTerms();
    glossaryTermUrnArray.remove(termUrn);
    schemaProposal.setProposedSchemaGlossaryTerms(glossaryTermUrnArray);

    ingestSchemaProposalsUpdate(creator, targetUrn, entityService, schemaProposals);
  }

  private static ActionRequestSnapshot createTermProposalRequest(
      Urn creator,
      List<Urn> assignedUsers,
      List<Urn> assignedGroups,
      Urn termUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType
  ) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple("actionRequest", uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(creator)));
    aspects.add(ActionRequestAspect.create(createActionRequestInfo(
        ActionRequestType.TERM_ASSOCIATION,
        creator,
        assignedUsers,
        assignedGroups,
        termUrn,
        targetUrn,
        subResource,
        subResourceType
    )));

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
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      SubResourceType subResourceType
  ) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(type.toString());
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setResourceType(targetUrn.getEntityType());
    info.setResource(targetUrn.toString());
    if (subResourceType != null) {
      info.setSubResourceType(subResourceType.toString());
    }
    if (subResource != null) {
      info.setSubResource(subResource);
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
      Entity proposalEntity
  ) {
    Optional<ActionRequestAspect> actionRequestStatusWrapper =
        proposalEntity.getValue()
            .getActionRequestSnapshot()
            .getAspects()
            .stream()
            .filter(actionRequestAspect -> actionRequestAspect.isActionRequestStatus())
            .findFirst();

    if (!actionRequestStatusWrapper.isPresent()) {
      actionRequestStatusWrapper = Optional.of(ActionRequestAspect.create(ProposalUtils.createActionRequestStatus(actor)));
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
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityClient entityClient,
      Authentication authentication
  ) {
    Filter filter = createActionRequestFilter(
        ActionRequestType.TAG_ASSOCIATION,
        com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING,
        targetUrn.toString(),
        subResource
    );

    return getActionRequestInfosFromFilter(filter, authentication, entityClient).filter(actionRequestInfo ->
        (subResource != null ? actionRequestInfo.getSubResource().equals(subResource) : !actionRequestInfo.hasSubResource())
            && actionRequestInfo.getResource().equals(targetUrn.toString())
            && actionRequestInfo.getParams().hasTagProposal()
            && actionRequestInfo.getParams().getTagProposal().getTag().equals(labelUrn)
    ).count() > 0;
  }

  @SneakyThrows
  public static Boolean isTermAlreadyProposedToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityClient entityClient,
      Authentication authentication
  ) {
    Filter filter = createActionRequestFilter(
        ActionRequestType.TERM_ASSOCIATION,
        com.linkedin.datahub.graphql.generated.ActionRequestStatus.PENDING,
        targetUrn.toString(),
        subResource
    );

    return getActionRequestInfosFromFilter(filter, authentication, entityClient).filter(actionRequestInfo ->
        (subResource != null ? actionRequestInfo.getSubResource().equals(subResource) : !actionRequestInfo.hasSubResource())
            && actionRequestInfo.getResource().equals(targetUrn.toString())
            && actionRequestInfo.getParams().hasGlossaryTermProposal()
            && actionRequestInfo.getParams().getGlossaryTermProposal().getGlossaryTerm().equals(labelUrn)
    ).count() > 0;
  }

  public static Stream<ActionRequestInfo> getActionRequestInfosFromFilter(
      Filter filter,
      Authentication authentication,
      EntityClient entityClient
  ) throws RemoteInvocationException {
    final SearchResult searchResult = entityClient.filter(
        ACTION_REQUEST_ENTITY_NAME,
        filter,
        null,
        0,
        20,
        authentication);
    final Map<Urn, Entity> entities = entityClient.batchGet(new HashSet<>(searchResult.getEntities()
        .stream().map(result -> result.getEntity()).collect(Collectors.toList())), authentication);

    return entities.values()
        .stream()
        .map(entity -> entity.getValue()
            .getActionRequestSnapshot()
            .getAspects()
            .stream()
            .filter(aspect -> aspect.isActionRequestInfo())
            .map(aspect -> aspect.getActionRequestInfo())
            .findFirst())
        .filter(maybeActionRequestInfo -> maybeActionRequestInfo.isPresent())
        .map(maybeActionRequestInfo -> maybeActionRequestInfo.get());
  }

  public static Boolean isTagAlreadyAttachedToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityService entityService
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlobalTags tags =
          (com.linkedin.common.GlobalTags) getAspectFromEntity(targetUrn.toString(), LabelUtils.TAGS_ASPECT_NAME, entityService, new GlobalTags());

      if (!tags.hasTags()) {
        return false;
      }

      return doesTagsListContainTag(tags, labelUrn);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), LabelUtils.EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());
      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);

      if (!editableFieldInfo.hasGlobalTags()) {
        return false;
      }

      return doesTagsListContainTag(editableFieldInfo.getGlobalTags(), labelUrn);
    }
  }

  public static Boolean isTermAlreadyAttachedToTarget(
      Urn labelUrn,
      Urn targetUrn,
      String subResource,
      EntityService entityService
  ) {
    if (subResource == null || subResource.equals("")) {
      com.linkedin.common.GlossaryTerms terms =
          (com.linkedin.common.GlossaryTerms) getAspectFromEntity(
              targetUrn.toString(), LabelUtils.GLOSSARY_TERM_ASPECT_NAME, entityService, new GlossaryTerms()
          );

      if (!terms.hasTerms()) {
        return false;
      }

      return doesTermsListContainTerm(terms, labelUrn);
    } else {
      com.linkedin.schema.EditableSchemaMetadata editableSchemaMetadata =
          (com.linkedin.schema.EditableSchemaMetadata) getAspectFromEntity(
              targetUrn.toString(), LabelUtils.EDITABLE_SCHEMA_METADATA, entityService, new EditableSchemaMetadata());

      EditableSchemaFieldInfo editableFieldInfo = getFieldInfoFromSchema(editableSchemaMetadata, subResource);
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
    return tagAssociationArray.stream().anyMatch(association -> association.getTag().equals(tagUrn));
  }

  public static Filter createActionRequestFilter(
      final @Nullable ActionRequestType type,
      final @Nullable com.linkedin.datahub.graphql.generated.ActionRequestStatus status,
      final @Nonnull String targetUrn,
      final @Nullable String targetSubresource
  ) {
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

  private ProposalUtils() { }
}
