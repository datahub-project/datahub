package com.datahub.authentication.proposal;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.CreateGlossaryNodeProposal;
import com.linkedin.actionrequest.CreateGlossaryTermProposal;
import com.linkedin.actionrequest.DataContractProposal;
import com.linkedin.actionrequest.DataContractProposalOperationType;
import com.linkedin.actionrequest.DescriptionProposal;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.SetMode;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataContractState;
import com.linkedin.datacontract.DataContractStatus;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.DataQualityContractArray;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.FreshnessContractArray;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datacontract.SchemaContractArray;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.GlossaryNodeKey;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ProposalService {
  private static final String ACTION_REQUEST_PENDING_STATUS = "PENDING";
  private static final String ACTION_REQUEST_EMPTY_RESULT = "";
  private static final String CREATE_GLOSSARY_NODE_ACTION_REQUEST_TYPE = "CREATE_GLOSSARY_NODE";
  private static final String CREATE_GLOSSARY_TERM_ACTION_REQUEST_TYPE = "CREATE_GLOSSARY_TERM";
  private static final String UPDATE_DESCRIPTION_ACTION_REQUEST_TYPE = "UPDATE_DESCRIPTION";
  private static final String DATA_CONTRACT_REQUEST_TYPE = "DATA_CONTRACT";

  private final EntityService _entityService;
  private final EntityClient _entityClient;
  private final GraphClient _graphClient;

  public boolean proposeCreateGlossaryNode(
      @Nonnull final Urn actorUrn,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description,
      final Authorizer dataHubAuthorizer) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(name, "name cannot be null");
    Objects.requireNonNull(parentNode, "parentNode cannot be null");

    AssignedActors actors =
        getAssignedUsersAndGroups(
            PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(), parentNode, dataHubAuthorizer);
    List<Urn> assignedUsers = actors.getUsers();
    List<Urn> assignedGroups = actors.getGroups();
    List<Urn> assignedRoles = actors.getRoles();
    ActionRequestSnapshot snapshot =
        createCreateGlossaryNodeProposalActionRequest(
            actorUrn, assignedUsers, assignedGroups, assignedRoles, name, parentNode, description);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    _entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  public boolean proposeCreateGlossaryTerm(
      @Nonnull final Urn actorUrn,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description,
      final Authorizer dataHubAuthorizer) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(name, "name cannot be null");
    Objects.requireNonNull(parentNode, "parentNode cannot be null");

    AssignedActors actors =
        getAssignedUsersAndGroups(
            PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(), parentNode, dataHubAuthorizer);
    List<Urn> assignedUsers = actors.getUsers();
    List<Urn> assignedGroups = actors.getGroups();
    List<Urn> assignedRoles = actors.getRoles();

    ActionRequestSnapshot snapshot =
        createCreateGlossaryTermProposalActionRequest(
            actorUrn, assignedUsers, assignedGroups, assignedRoles, name, parentNode, description);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    _entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  public boolean proposeUpdateResourceDescription(
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      final Authorizer dataHubAuthorizer) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(resourceUrn, "resourceUrn cannot be null");
    Objects.requireNonNull(description, "description cannot be null");

    if (!_entityService.exists(resourceUrn)) {
      throw new RuntimeException(String.format("Entity %s does not exist", resourceUrn));
    }

    List<Urn> assignedUsers;
    List<Urn> assignedGroups;
    List<Urn> assignedRoles;
    EntitySpec spec = new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString());

    if (resourceUrn.getEntityType().equals(GLOSSARY_TERM_ENTITY_NAME)
        || resourceUrn.getEntityType().equals(GLOSSARY_NODE_ENTITY_NAME)) {
      AssignedActors actors =
          getAssignedUsersAndGroups(
              PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(),
              Optional.of(resourceUrn),
              dataHubAuthorizer);
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
      assignedRoles = actors.getRoles();
    } else {
      AuthorizedActors actors =
          dataHubAuthorizer.authorizedActors(
              PoliciesConfig.MANAGE_ENTITY_DOCS_PROPOSALS_PRIVILEGE.getType(), Optional.of(spec));
      assignedUsers = actors.getUsers();
      assignedGroups = actors.getGroups();
      assignedRoles = actors.getRoles();
    }

    ActionRequestSnapshot snapshot =
        createUpdateDescriptionProposalActionRequest(
            actorUrn, resourceUrn, assignedUsers, assignedGroups, assignedRoles, description);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    _entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  public boolean proposeDataContract(
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final DataContractProposalOperationType opType,
      @Nullable final List<FreshnessContract> freshness,
      @Nullable final List<SchemaContract> schema,
      @Nullable final List<DataQualityContract> quality,
      final Authorizer dataHubAuthorizer) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(entityUrn, "entityUrn cannot be null");
    Objects.requireNonNull(opType, "opType cannot be null");

    if (!_entityService.exists(entityUrn)) {
      throw new RuntimeException(String.format("Entity %s does not exist", entityUrn));
    }

    if (freshness != null) {
      verifyAssertionsExist(
          freshness.stream().map(FreshnessContract::getAssertion).collect(Collectors.toList()));
    }

    if (schema != null) {
      verifyAssertionsExist(
          schema.stream().map(SchemaContract::getAssertion).collect(Collectors.toList()));
    }

    if (quality != null) {
      verifyAssertionsExist(
          quality.stream().map(DataQualityContract::getAssertion).collect(Collectors.toList()));
    }

    List<Urn> assignedUsers;
    List<Urn> assignedGroups;
    EntitySpec spec = new EntitySpec(entityUrn.getEntityType(), entityUrn.toString());

    AuthorizedActors actors =
        dataHubAuthorizer.authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_DATA_CONTRACT_PROPOSALS_PRIVILEGE.getType(),
            Optional.of(spec));
    assignedUsers = actors.getUsers();
    assignedGroups = actors.getGroups();

    ActionRequestSnapshot snapshot =
        createDataContractActionRequest(
            actorUrn, assignedUsers, assignedGroups, entityUrn, opType, freshness, schema, quality);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    _entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  public boolean isAuthorizedToResolveGlossaryEntityAsOwner(
      @Nonnull Urn actorUrn, Optional<Urn> parentNode) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");

    if (!parentNode.isPresent()) {
      return false;
    }

    Pair<List<Urn>, List<Urn>> parentNodeOwners = getUrnOwners(parentNode.get());
    Set<Urn> userOwners = new HashSet<>(parentNodeOwners.getFirst());
    if (userOwners.contains(actorUrn)) {
      return true;
    }

    Set<Urn> groupOwners = new HashSet<>(parentNodeOwners.getSecond());
    GroupMembership groupMembership =
        (GroupMembership) _entityService.getLatestAspect(actorUrn, GROUP_MEMBERSHIP_ASPECT_NAME);

    // TODO: Union with Native Group Membership
    Set<Urn> actorGroups = new HashSet<>(groupMembership.getGroups());
    actorGroups.retainAll(groupOwners);
    return !actorGroups.isEmpty();
  }

  public void acceptCreateGlossaryNodeProposal(
      @Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final boolean canManageGlossaries,
      final Authentication authentication)
      throws Exception {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    CreateGlossaryNodeProposal createGlossaryNodeProposal =
        actionRequestInfo.getParams().getCreateGlossaryNodeProposal();
    String name = createGlossaryNodeProposal.getName();
    Optional<Urn> parentNode =
        createGlossaryNodeProposal.hasParentNode()
                && createGlossaryNodeProposal.getParentNode() != null
            ? Optional.of(createGlossaryNodeProposal.getParentNode())
            : Optional.empty();
    Optional<String> description =
        createGlossaryNodeProposal.hasDescription()
            ? Optional.ofNullable(createGlossaryNodeProposal.getDescription())
            : Optional.empty();
    if (!canManageGlossaries && !isAuthorizedToResolveGlossaryEntityAsOwner(actorUrn, parentNode)) {
      throw new RuntimeException(
          "Unauthorized to accept creating this Glossary Node. Please contact your DataHub administrator.");
    }

    createGlossaryNodeEntity(name, parentNode, description, authentication);
  }

  public void acceptCreateGlossaryTermProposal(
      @Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final boolean canManageGlossaries,
      final Authentication authentication)
      throws Exception {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    CreateGlossaryTermProposal createGlossaryTermProposal =
        actionRequestInfo.getParams().getCreateGlossaryTermProposal();
    String name = createGlossaryTermProposal.getName();
    Optional<Urn> parentNode =
        createGlossaryTermProposal.hasParentNode()
                && createGlossaryTermProposal.getParentNode() != null
            ? Optional.of(createGlossaryTermProposal.getParentNode())
            : Optional.empty();
    Optional<String> description =
        createGlossaryTermProposal.hasDescription()
            ? Optional.ofNullable(createGlossaryTermProposal.getDescription())
            : Optional.empty();
    if (!canManageGlossaries && !isAuthorizedToResolveGlossaryEntityAsOwner(actorUrn, parentNode)) {
      throw new RuntimeException(
          "Unauthorized to accept creating this Glossary Node. Please contact your DataHub administrator.");
    }

    createGlossaryTermEntity(name, parentNode, description, authentication);
  }

  public boolean canResolveGlossaryNodeProposal(
      @Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final boolean canManageGlossaries) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    if (canManageGlossaries) {
      return true;
    }

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    try {
      CreateGlossaryNodeProposal createGlossaryNodeProposal =
          actionRequestInfo.getParams().getCreateGlossaryNodeProposal();
      Optional<Urn> parentNode =
          createGlossaryNodeProposal.hasParentNode()
                  && createGlossaryNodeProposal.getParentNode() != null
              ? Optional.of(createGlossaryNodeProposal.getParentNode())
              : Optional.empty();
      return isAuthorizedToResolveGlossaryEntityAsOwner(actorUrn, parentNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create glossary term entity");
    }
  }

  public boolean canResolveGlossaryTermProposal(
      @Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final boolean canManageGlossaries) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    if (canManageGlossaries) {
      return true;
    }

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    try {
      CreateGlossaryTermProposal createGlossaryTermProposal =
          actionRequestInfo.getParams().getCreateGlossaryTermProposal();
      Optional<Urn> parentNode =
          createGlossaryTermProposal.hasParentNode()
                  && createGlossaryTermProposal.getParentNode() != null
              ? Optional.of(createGlossaryTermProposal.getParentNode())
              : Optional.empty();
      return isAuthorizedToResolveGlossaryEntityAsOwner(actorUrn, parentNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create glossary term entity");
    }
  }

  public void acceptUpdateResourceDescriptionProposal(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final Authentication authentication)
      throws Exception {
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    if (!actionRequestInfo.hasResource()) {
      throw new RuntimeException("actionRequestInfo is missing resource");
    }
    Urn resourceUrn = Urn.createFromString(actionRequestInfo.getResource());
    DescriptionProposal descriptionProposal =
        actionRequestInfo.getParams().getUpdateDescriptionProposal();
    String description = descriptionProposal.getDescription();
    switch (resourceUrn.getEntityType()) {
      case GLOSSARY_NODE_ENTITY_NAME:
        updateGlossaryNodeDescription(resourceUrn, description, authentication);
        break;
      case GLOSSARY_TERM_ENTITY_NAME:
        updateGlossaryTermDescription(resourceUrn, description, authentication);
        break;
      case DATASET_ENTITY_NAME:
        updateDatasetDescription(resourceUrn, description, authentication);
        break;
      default:
        log.warn(
            String.format(
                "Proposing an update to a description is currently not supported for entity type %s",
                resourceUrn.getEntityType()));
        break;
    }
  }

  public void acceptDataContractProposal(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      @Nonnull final Authentication authentication)
      throws Exception {
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");
    Objects.requireNonNull(authentication, "authentication cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    DataContractProposal dataContractProposal =
        actionRequestInfo.getParams().getDataContractProposal();
    if (dataContractProposal != null) {
      final Urn entityUrn = UrnUtils.getUrn(actionRequestInfo.getResource());
      final Urn existingContractUrn = getDataContractUrn(entityUrn, authentication);
      final DataContractProperties existingProperties =
          existingContractUrn != null
              ? getDataContractProperties(existingContractUrn, authentication)
              : null;
      final Urn finalContractUrn =
          existingContractUrn != null
              ? existingContractUrn
              : Urn.createFromString(String.format("urn:li:dataContract:%s", UUID.randomUUID()));
      final DataContractProperties newProperties =
          new DataContractProperties()
              .setEntity(entityUrn)
              .setFreshness(dataContractProposal.getFreshness(GetMode.NULL), SetMode.IGNORE_NULL)
              .setDataQuality(
                  dataContractProposal.getDataQuality(GetMode.NULL), SetMode.IGNORE_NULL)
              .setSchema(dataContractProposal.getSchema(GetMode.NULL), SetMode.IGNORE_NULL);
      final DataContractProperties finalProperties =
          existingProperties != null
              ? mergeDataContractProperties(
                  dataContractProposal.getType(), existingProperties, newProperties)
              : newProperties;

      // TODO: If the contract contains external assertions, then mark it as pending.
      final DataContractStatus status =
          new DataContractStatus().setState(getStateForContract(entityUrn, dataContractProposal));

      final MetadataChangeProposal propertiesProposal = new MetadataChangeProposal();
      propertiesProposal.setEntityUrn(finalContractUrn);
      propertiesProposal.setEntityType(DATA_CONTRACT_ENTITY_NAME);
      propertiesProposal.setAspectName(DATA_CONTRACT_PROPERTIES_ASPECT_NAME);
      propertiesProposal.setAspect(GenericRecordUtils.serializeAspect(finalProperties));
      propertiesProposal.setChangeType(ChangeType.UPSERT);

      final MetadataChangeProposal statusProposal = new MetadataChangeProposal();
      statusProposal.setEntityUrn(finalContractUrn);
      statusProposal.setEntityType(DATA_CONTRACT_ENTITY_NAME);
      statusProposal.setAspectName(DATA_CONTRACT_STATUS_ASPECT_NAME);
      statusProposal.setAspect(GenericRecordUtils.serializeAspect(status));
      statusProposal.setChangeType(ChangeType.UPSERT);

      // TODO: In the future, we may need to mint Monitors to execute the assertions if they
      // are executable.
      // Currently, we are expecting that the assertions will be purely external.
      _entityClient.batchIngestProposals(
          ImmutableList.of(propertiesProposal, statusProposal), authentication);
    } else {
      throw new IllegalArgumentException(
          "Failed to accept Data Contract Proposal. Action Request is missing required parameters.");
    }
  }

  @Nullable
  private Urn getDataContractUrn(final Urn entityUrn, final Authentication authentication) {
    EntityRelationships relationships =
        _graphClient.getRelatedEntities(
            entityUrn.toString(),
            ImmutableList.of("ContractFor"),
            RelationshipDirection.INCOMING,
            0,
            1,
            authentication.getActor().toUrnStr());

    if (relationships.getTotal() > 1) {
      // Bad state - There are multiple contracts for a single entity! Cannot update.
      log.warn(
          String.format(
              "Found entity with multiple data contracts! urn: %s, num contracts: %s",
              entityUrn, relationships.getTotal()));
    }

    if (relationships.getRelationships().size() >= 1) {
      return relationships.getRelationships().get(0).getEntity();
    }
    return null;
  }

  @Nullable
  private DataContractProperties getDataContractProperties(
      Urn contractUrn, Authentication authentication) {
    try {
      EntityResponse response =
          _entityClient.getV2(
              DATA_CONTRACT_ENTITY_NAME,
              contractUrn,
              ImmutableSet.of(DATA_CONTRACT_PROPERTIES_ASPECT_NAME),
              authentication);
      if (response != null
          && response.getAspects().containsKey(DATA_CONTRACT_PROPERTIES_ASPECT_NAME)) {
        return new DataContractProperties(
            response.getAspects().get(DATA_CONTRACT_PROPERTIES_ASPECT_NAME).data());
      }
      return null;
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to retrieve data contract properties for contract urn %s", contractUrn));
      return null;
    }
  }

  @Nonnull
  private DataContractProperties mergeDataContractProperties(
      @Nonnull final DataContractProposalOperationType type,
      @Nonnull final DataContractProperties first,
      @Nonnull final DataContractProperties second) {
    final DataContractProperties properties = new DataContractProperties();
    final List<FreshnessContract> freshness = new ArrayList<>();
    final List<SchemaContract> schema = new ArrayList<>();
    final List<DataQualityContract> quality = new ArrayList<>();
    if (DataContractProposalOperationType.ADD.equals(type) && first.hasFreshness()) {
      freshness.addAll(first.getFreshness());
    }
    if (second.hasFreshness()) {
      freshness.addAll(second.getFreshness());
    }
    if (DataContractProposalOperationType.ADD.equals(type) && first.hasSchema()) {
      schema.addAll(first.getSchema());
    }
    if (second.hasSchema()) {
      schema.addAll(second.getSchema());
    }
    if (DataContractProposalOperationType.ADD.equals(type) && first.hasDataQuality()) {
      quality.addAll(first.getDataQuality());
    }
    if (second.hasDataQuality()) {
      quality.addAll(second.getDataQuality());
    }
    properties.setFreshness(new FreshnessContractArray(freshness));
    properties.setSchema(new SchemaContractArray(schema));
    properties.setDataQuality(new DataQualityContractArray(quality));
    properties.setEntity(second.getEntity());
    return properties;
  }

  public void completeProposal(
      @Nonnull final Urn actorUrn,
      @Nonnull final String actionRequestStatus,
      @Nonnull final String actionRequestResult,
      @Nonnull final Entity proposalEntity) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestStatus, "actionRequestStatus cannot be null");
    Objects.requireNonNull(actionRequestResult, "actionRequestResult cannot be null");

    final ActionRequestSnapshot actionRequestSnapshot =
        setStatusSnapshot(actorUrn, actionRequestStatus, actionRequestResult, proposalEntity);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(actionRequestSnapshot));
    _entityService.ingestEntity(entity, auditStamp);
  }

  AssignedActors getAssignedUsersAndGroups(
      @Nonnull final String privilegeType,
      @Nonnull final Optional<Urn> urnOptional,
      Authorizer dataHubAuthorizer) {
    AuthorizedActors authorizedActors =
        dataHubAuthorizer.authorizedActors(privilegeType, Optional.empty());

    List<Urn> assignedUsers =
        authorizedActors == null ? new ArrayList<>() : authorizedActors.getUsers();
    List<Urn> assignedGroups =
        authorizedActors == null ? new ArrayList<>() : authorizedActors.getGroups();
    List<Urn> assignedRoles =
        authorizedActors == null ? new ArrayList<>() : authorizedActors.getRoles();
    if (urnOptional.isPresent()) {
      Ownership ownership =
          (Ownership) _entityService.getLatestAspect(urnOptional.get(), OWNERSHIP_ASPECT_NAME);
      if (ownership != null && ownership.hasOwners()) {
        Pair<List<Urn>, List<Urn>> urnOwners = getUrnOwners(urnOptional.get());
        List<Urn> userOwners = urnOwners.getFirst();
        List<Urn> groupOwners = urnOwners.getSecond();

        assignedUsers.addAll(userOwners);
        assignedGroups.addAll(groupOwners);
      }
    }
    return new AssignedActors(assignedUsers, assignedGroups, assignedRoles);
  }

  @Value
  private static class AssignedActors {
    List<Urn> users;
    List<Urn> groups;
    List<Urn> roles;
  }

  Pair<List<Urn>, List<Urn>> getUrnOwners(@Nonnull final Urn urn) {
    Ownership ownership = (Ownership) _entityService.getLatestAspect(urn, OWNERSHIP_ASPECT_NAME);
    List<Urn> userOwners =
        ownership.getOwners().stream()
            .map(Owner::getOwner)
            .filter(owner -> owner.getEntityType().equals(CORP_USER_ENTITY_NAME))
            .collect(Collectors.toList());
    List<Urn> groupOwners =
        ownership.getOwners().stream()
            .map(Owner::getOwner)
            .filter(owner -> owner.getEntityType().equals(CORP_GROUP_ENTITY_NAME))
            .collect(Collectors.toList());

    return Pair.of(userOwners, groupOwners);
  }

  static ActionRequestInfo findActionRequestInfoAspect(
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot) {
    Optional<ActionRequestAspect> actionRequestInfoOptional =
        actionRequestSnapshot.getAspects().stream()
            .filter(ActionRequestAspect::isActionRequestInfo)
            .findFirst();
    if (!actionRequestInfoOptional.isPresent()) {
      throw new RuntimeException("Could not find ActionRequestInfo");
    }
    ActionRequestAspect actionRequestInfoAspect = actionRequestInfoOptional.get();
    if (!actionRequestInfoAspect.isActionRequestInfo()) {
      throw new RuntimeException("ActionRequestAspect is not ActionRequestInfo");
    }
    ActionRequestInfo actionRequestInfo = actionRequestInfoAspect.getActionRequestInfo();
    if (!actionRequestInfo.hasParams()) {
      throw new RuntimeException("ActionRequestInfo does not have params");
    }
    return actionRequestInfo;
  }

  static ActionRequestSnapshot setStatusSnapshot(
      @Nonnull final Urn actorUrn,
      @Nonnull final String actionRequestStatus,
      @Nonnull final String actionRequestResult,
      @Nonnull final Entity proposalEntity) {
    ActionRequestStatus statusAspect =
        proposalEntity.getValue().getActionRequestSnapshot().getAspects().stream()
            .filter(ActionRequestAspect::isActionRequestStatus)
            .findFirst()
            .orElse(ActionRequestAspect.create(createActionRequestStatus(actorUrn)))
            .getActionRequestStatus();

    statusAspect.setStatus(actionRequestStatus);
    statusAspect.setResult(actionRequestResult);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    statusAspect.setLastModified(auditStamp);

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(statusAspect));

    ActionRequestSnapshot snapshot = new ActionRequestSnapshot();
    snapshot.setUrn(proposalEntity.getValue().getActionRequestSnapshot().getUrn());
    snapshot.setAspects(aspects);

    return snapshot;
  }

  static ActionRequestStatus createActionRequestStatus(@Nonnull final Urn actorUrn) {
    final ActionRequestStatus status = new ActionRequestStatus();

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    status.setStatus(ACTION_REQUEST_PENDING_STATUS);
    status.setResult(ACTION_REQUEST_EMPTY_RESULT);
    status.setLastModified(auditStamp);

    return status;
  }

  static ActionRequestSnapshot createCreateGlossaryNodeProposalActionRequest(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(
        ActionRequestAspect.create(
            createCreateGlossaryNodeActionRequestInfo(
                actorUrn,
                assignedUsers,
                assignedGroups,
                assignedRoles,
                name,
                parentNode,
                description)));

    result.setAspects(aspects);

    return result;
  }

  static ActionRequestSnapshot createCreateGlossaryTermProposalActionRequest(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(
        ActionRequestAspect.create(
            createCreateGlossaryTermActionRequestInfo(
                actorUrn,
                assignedUsers,
                assignedGroups,
                assignedRoles,
                name,
                parentNode,
                description)));

    result.setAspects(aspects);

    return result;
  }

  static ActionRequestSnapshot createUpdateDescriptionProposalActionRequest(
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn resourceUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String description) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(
        ActionRequestAspect.create(
            createUpdateDescriptionActionRequestInfo(
                actorUrn, resourceUrn, assignedUsers, assignedGroups, assignedRoles, description)));

    result.setAspects(aspects);

    return result;
  }

  ActionRequestSnapshot createDataContractActionRequest(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final Urn entityUrn,
      @Nonnull final DataContractProposalOperationType opType,
      @Nullable final List<FreshnessContract> freshness,
      @Nullable final List<SchemaContract> schema,
      @Nullable final List<DataQualityContract> quality) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(
        ActionRequestAspect.create(
            createDataContractActionRequestInfo(
                actorUrn,
                assignedUsers,
                assignedGroups,
                entityUrn,
                opType,
                freshness,
                schema,
                quality)));
    result.setAspects(aspects);

    return result;
  }

  static ActionRequestInfo createCreateGlossaryNodeActionRequestInfo(
      final Urn creator,
      final List<Urn> assignedUsers,
      final List<Urn> assignedGroups,
      final List<Urn> assignedRoles,
      final String name,
      final Optional<Urn> parentNode,
      final String description) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(CREATE_GLOSSARY_NODE_ACTION_REQUEST_TYPE);
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setAssignedRoles(new UrnArray(assignedRoles));
    info.setResourceType(GLOSSARY_NODE_ENTITY_NAME);
    info.setParams(createCreateGlossaryNodeActionRequestParams(name, parentNode, description));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(creator);

    return info;
  }

  static ActionRequestInfo createCreateGlossaryTermActionRequestInfo(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(CREATE_GLOSSARY_TERM_ACTION_REQUEST_TYPE);
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setAssignedRoles(new UrnArray(assignedRoles));
    info.setResourceType(GLOSSARY_TERM_ENTITY_NAME);
    info.setParams(createCreateGlossaryTermActionRequestParams(name, parentNode, description));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(actorUrn);

    return info;
  }

  static ActionRequestInfo createUpdateDescriptionActionRequestInfo(
      @Nonnull final Urn actorUrn,
      @Nonnull final Urn resourceUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final List<Urn> assignedRoles,
      @Nonnull final String description) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(UPDATE_DESCRIPTION_ACTION_REQUEST_TYPE);
    info.setResource(resourceUrn.toString());
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setAssignedRoles(new UrnArray(assignedRoles));
    info.setResourceType(resourceUrn.getEntityType());
    info.setParams(createUpdateDescriptionActionRequestParams(description));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(actorUrn);

    return info;
  }

  ActionRequestInfo createDataContractActionRequestInfo(
      @Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers,
      @Nonnull final List<Urn> assignedGroups,
      @Nonnull final Urn entityUrn,
      @Nonnull final DataContractProposalOperationType opType,
      @Nullable final List<FreshnessContract> freshness,
      @Nullable final List<SchemaContract> schema,
      @Nullable final List<DataQualityContract> quality) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(DATA_CONTRACT_REQUEST_TYPE);
    info.setResource(entityUrn.toString());
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setResourceType(entityUrn.getEntityType());
    info.setParams(createDataContractParams(opType, freshness, schema, quality));
    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(actorUrn);
    return info;
  }

  static ActionRequestParams createCreateGlossaryNodeActionRequestParams(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description) {
    CreateGlossaryNodeProposal createGlossaryNodeProposal = new CreateGlossaryNodeProposal();
    createGlossaryNodeProposal.setName(name);
    parentNode.ifPresent(createGlossaryNodeProposal::setParentNode);
    if (description != null) {
      createGlossaryNodeProposal.setDescription(description);
    }

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setCreateGlossaryNodeProposal(createGlossaryNodeProposal);
    return actionRequestParams;
  }

  static ActionRequestParams createCreateGlossaryTermActionRequestParams(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      final String description) {
    CreateGlossaryTermProposal createGlossaryTermProposal = new CreateGlossaryTermProposal();
    createGlossaryTermProposal.setName(name);
    parentNode.ifPresent(createGlossaryTermProposal::setParentNode);
    if (description != null) {
      createGlossaryTermProposal.setDescription(description);
    }

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setCreateGlossaryTermProposal(createGlossaryTermProposal);
    return actionRequestParams;
  }

  static ActionRequestParams createUpdateDescriptionActionRequestParams(
      @Nonnull final String description) {
    DescriptionProposal descriptionProposal = new DescriptionProposal();
    descriptionProposal.setDescription(description);

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setUpdateDescriptionProposal(descriptionProposal);
    return actionRequestParams;
  }

  ActionRequestParams createDataContractParams(
      @Nonnull final DataContractProposalOperationType opType,
      @Nullable final List<FreshnessContract> freshness,
      @Nullable final List<SchemaContract> schema,
      @Nullable final List<DataQualityContract> quality) {
    final ActionRequestParams result = new ActionRequestParams();
    final DataContractProposal params = new DataContractProposal();
    params.setType(opType);
    if (freshness != null) {
      params.setFreshness(new FreshnessContractArray(freshness));
    }
    if (schema != null) {
      params.setSchema(new SchemaContractArray(schema));
    }
    if (quality != null) {
      params.setDataQuality(new DataQualityContractArray(quality));
    }
    result.setDataContractProposal(params);
    return result;
  }

  @Nullable
  private Urn getEntityContractUrn(@Nonnull Urn entityUrn, @Nonnull Urn actorUrn) {
    EntityRelationships relationships =
        _graphClient.getRelatedEntities(
            entityUrn.toString(),
            ImmutableList.of("ContractFor"),
            RelationshipDirection.INCOMING,
            0,
            1,
            actorUrn.toString());
    if (relationships.getRelationships().size() == 1) {
      return relationships.getRelationships().get(0).getEntity();
    } else if (relationships.getTotal() > 1) {
      // Bad state - There are multiple contracts for a single entity! Cannot update.
      log.warn(
          String.format(
              "Found entity with multiple data contracts! urn: %s, num contracts: %s",
              entityUrn, relationships.getTotal()));
    }
    return null;
  }

  void createGlossaryNodeEntity(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      @Nonnull final Optional<String> description,
      final Authentication authentication)
      throws Exception {
    final GlossaryNodeKey key = new GlossaryNodeKey();

    final String id = UUID.randomUUID().toString();
    key.setName(id);

    if (_entityClient.exists(
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.GLOSSARY_NODE_ENTITY_NAME),
        authentication)) {
      throw new IllegalArgumentException("This Glossary Node already exists!");
    }

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.GLOSSARY_NODE_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(mapGlossaryNodeInfo(name, parentNode, description)));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityClient.ingestProposal(proposal, authentication);
  }

  void createGlossaryTermEntity(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      @Nonnull final Optional<String> description,
      final Authentication authentication)
      throws Exception {
    final GlossaryTermKey key = new GlossaryTermKey();

    final String id = UUID.randomUUID().toString();
    key.setName(id);

    if (_entityClient.exists(
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.GLOSSARY_TERM_ENTITY_NAME),
        authentication)) {
      throw new IllegalArgumentException("This Glossary Term already exists!");
    }

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.GLOSSARY_TERM_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    proposal.setAspect(
        GenericRecordUtils.serializeAspect(mapGlossaryTermInfo(name, parentNode, description)));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityClient.ingestProposal(proposal, authentication);
  }

  void updateGlossaryNodeDescription(
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      final Authentication authentication)
      throws Exception {
    GlossaryNodeInfo glossaryNodeInfo =
        (GlossaryNodeInfo)
            _entityService.getLatestAspect(resourceUrn, GLOSSARY_NODE_INFO_ASPECT_NAME);
    Objects.requireNonNull(glossaryNodeInfo, "glossaryNodeInfo cannot be null");

    final MetadataChangeProposal proposal =
        DescriptionUtils.createGlossaryNodeDescriptionChangeProposal(
            glossaryNodeInfo, resourceUrn, description);
    _entityClient.ingestProposal(proposal, authentication);
  }

  void updateGlossaryTermDescription(
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      final Authentication authentication)
      throws Exception {
    GlossaryTermInfo glossaryTermInfo =
        (GlossaryTermInfo)
            _entityService.getLatestAspect(resourceUrn, GLOSSARY_TERM_INFO_ASPECT_NAME);

    final MetadataChangeProposal proposal =
        DescriptionUtils.createGlossaryTermDescriptionChangeProposal(
            glossaryTermInfo, resourceUrn, description);
    _entityClient.ingestProposal(proposal, authentication);
  }

  void updateDatasetDescription(
      @Nonnull final Urn resourceUrn,
      @Nonnull final String description,
      final Authentication authentication)
      throws Exception {
    EditableDatasetProperties editableDatasetProperties =
        (EditableDatasetProperties)
            _entityService.getLatestAspect(resourceUrn, EDITABLE_DATASET_PROPERTIES_ASPECT_NAME);
    if (editableDatasetProperties == null) {
      editableDatasetProperties = new EditableDatasetProperties();
    }

    final MetadataChangeProposal proposal =
        DescriptionUtils.createDatasetDescriptionChangeProposal(
            editableDatasetProperties, resourceUrn, description, authentication.getActor());
    _entityClient.ingestProposal(proposal, authentication);
  }

  GlossaryNodeInfo mapGlossaryNodeInfo(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      @Nonnull final Optional<String> description)
      throws Exception {
    final GlossaryNodeInfo result = new GlossaryNodeInfo();
    result.setName(name);
    result.setDefinition("");
    if (parentNode.isPresent()) {
      final GlossaryNodeUrn nodeUrn = GlossaryNodeUrn.createFromString(parentNode.get().toString());
      result.setParentNode(nodeUrn);
    }
    description.ifPresent(result::setDefinition);

    return result;
  }

  GlossaryTermInfo mapGlossaryTermInfo(
      @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode,
      @Nonnull final Optional<String> description)
      throws Exception {
    final GlossaryTermInfo result = new GlossaryTermInfo();
    result.setName(name);
    result.setDefinition("");
    result.setTermSource("INTERNAL");
    if (parentNode.isPresent()) {
      final GlossaryNodeUrn nodeUrn = GlossaryNodeUrn.createFromString(parentNode.get().toString());
      result.setParentNode(nodeUrn);
    }
    description.ifPresent(result::setDefinition);

    return result;
  }

  private DataContractState getStateForContract(
      @Nonnull final Urn entityUrn, @Nonnull final DataContractProposal proposal) {
    // TOTAL HACK
    if (entityUrn.toString().contains("urn:li:dataPlatform:dbt")) {
      // Assertions must be provisioned for this contract.
      // This is a really bad way to do this. Ideally we know if the assertions are external or not.
      // We should check them before creating the contract.
      return DataContractState.PENDING;
    }
    return DataContractState.ACTIVE;
  }

  private void verifyAssertionsExist(@Nonnull final List<Urn> assertionUrns) {
    for (final Urn assertionUrn : assertionUrns) {
      if (!_entityService.exists(assertionUrn)) {
        throw new RuntimeException(String.format("Assertion %s does not exist", assertionUrn));
      }
    }
  }
}
