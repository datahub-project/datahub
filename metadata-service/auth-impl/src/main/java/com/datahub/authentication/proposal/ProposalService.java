package com.datahub.authentication.proposal;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.Authorizer;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.CreateGlossaryNodeProposal;
import com.linkedin.actionrequest.CreateGlossaryTermProposal;
import com.linkedin.actionrequest.DescriptionProposal;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.Entity;
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
import com.linkedin.metadata.key.GlossaryNodeKey;
import com.linkedin.metadata.key.GlossaryTermKey;
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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class ProposalService {
  private static final String ACTION_REQUEST_PENDING_STATUS = "PENDING";
  private static final String ACTION_REQUEST_EMPTY_RESULT = "";
  private static final String CREATE_GLOSSARY_NODE_ACTION_REQUEST_TYPE = "CREATE_GLOSSARY_NODE";
  private static final String CREATE_GLOSSARY_TERM_ACTION_REQUEST_TYPE = "CREATE_GLOSSARY_TERM";
  private static final String UPDATE_DESCRIPTION_ACTION_REQUEST_TYPE = "UPDATE_DESCRIPTION";

  private final EntityService _entityService;
  private final EntityClient _entityClient;

  public boolean proposeCreateGlossaryNode(@Nonnull final Urn actorUrn, @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode, final Authorizer dataHubAuthorizer) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(name, "name cannot be null");
    Objects.requireNonNull(parentNode, "parentNode cannot be null");

    Pair<List<Urn>, List<Urn>> assignedUsersAndGroups =
        getAssignedUsersAndGroups(PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(), parentNode, dataHubAuthorizer);
    List<Urn> assignedUsers = assignedUsersAndGroups.getFirst();
    List<Urn> assignedGroups = assignedUsersAndGroups.getSecond();

    ActionRequestSnapshot snapshot =
        createCreateGlossaryNodeProposalActionRequest(actorUrn, assignedUsers, assignedGroups, name, parentNode);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    _entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  public boolean proposeCreateGlossaryTerm(@Nonnull final Urn actorUrn, @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode, final Authorizer dataHubAuthorizer) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(name, "name cannot be null");
    Objects.requireNonNull(parentNode, "parentNode cannot be null");

    Pair<List<Urn>, List<Urn>> assignedUsersAndGroups =
        getAssignedUsersAndGroups(PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(), parentNode, dataHubAuthorizer);
    List<Urn> assignedUsers = assignedUsersAndGroups.getFirst();
    List<Urn> assignedGroups = assignedUsersAndGroups.getSecond();

    ActionRequestSnapshot snapshot =
        createCreateGlossaryTermProposalActionRequest(actorUrn, assignedUsers, assignedGroups, name, parentNode);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    _entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  public boolean proposeUpdateResourceDescription(@Nonnull final Urn actorUrn, @Nonnull final Urn resourceUrn,
      @Nonnull final String description, final Authorizer dataHubAuthorizer) {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(resourceUrn, "resourceUrn cannot be null");
    Objects.requireNonNull(description, "description cannot be null");

    if (!_entityService.exists(resourceUrn)) {
      throw new RuntimeException(String.format("Entity %s does not exist", resourceUrn));
    }

    Pair<List<Urn>, List<Urn>> assignedUsersAndGroups =
        getAssignedUsersAndGroups(PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE.getType(), Optional.of(resourceUrn),
            dataHubAuthorizer);
    List<Urn> assignedUsers = assignedUsersAndGroups.getFirst();
    List<Urn> assignedGroups = assignedUsersAndGroups.getSecond();

    ActionRequestSnapshot snapshot =
        createUpdateDescriptionProposalActionRequest(actorUrn, resourceUrn, assignedUsers, assignedGroups, description);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(actorUrn, SetMode.IGNORE_NULL);
    auditStamp.setTime(System.currentTimeMillis());

    Entity entity = new Entity();
    entity.setValue(Snapshot.create(snapshot));
    _entityService.ingestEntity(entity, auditStamp);

    return true;
  }

  public boolean isAuthorizedToResolveGlossaryEntityAsOwner(@Nonnull Urn actorUrn, Optional<Urn> parentNode) {
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

  public void acceptCreateGlossaryNodeProposal(@Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot, final boolean canManageGlossaries,
      final Authentication authentication) throws Exception {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    CreateGlossaryNodeProposal createGlossaryNodeProposal =
        actionRequestInfo.getParams().getCreateGlossaryNodeProposal();
    String name = createGlossaryNodeProposal.getName();
    Optional<Urn> parentNode =
        createGlossaryNodeProposal.hasParentNode() && createGlossaryNodeProposal.getParentNode() != null ? Optional.of(
            createGlossaryNodeProposal.getParentNode()) : Optional.empty();
      if (!canManageGlossaries && !isAuthorizedToResolveGlossaryEntityAsOwner(actorUrn, parentNode)) {
        throw new RuntimeException(
            "Unauthorized to accept creating this Glossary Node. Please contact your DataHub administrator.");
      }

      createGlossaryNodeEntity(name, parentNode, authentication);
  }

  public void acceptCreateGlossaryTermProposal(@Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot, final boolean canManageGlossaries,
      final Authentication authentication) throws Exception {
    Objects.requireNonNull(actorUrn, "actorUrn cannot be null");
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    CreateGlossaryTermProposal createGlossaryTermProposal =
        actionRequestInfo.getParams().getCreateGlossaryTermProposal();
    String name = createGlossaryTermProposal.getName();
    Optional<Urn> parentNode =
        createGlossaryTermProposal.hasParentNode() && createGlossaryTermProposal.getParentNode() != null ? Optional.of(
            createGlossaryTermProposal.getParentNode()) : Optional.empty();
      if (!canManageGlossaries && !isAuthorizedToResolveGlossaryEntityAsOwner(actorUrn, parentNode)) {
        throw new RuntimeException(
            "Unauthorized to accept creating this Glossary Node. Please contact your DataHub administrator.");
      }

      createGlossaryTermEntity(name, parentNode, authentication);
  }

  public boolean canResolveGlossaryNodeProposal(@Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot, final boolean canManageGlossaries) {
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
          createGlossaryNodeProposal.hasParentNode() && createGlossaryNodeProposal.getParentNode() != null
              ? Optional.of(createGlossaryNodeProposal.getParentNode()) : Optional.empty();
      return isAuthorizedToResolveGlossaryEntityAsOwner(actorUrn, parentNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create glossary term entity");
    }
  }

  public boolean canResolveGlossaryTermProposal(@Nonnull final Urn actorUrn,
      @Nonnull final ActionRequestSnapshot actionRequestSnapshot, final boolean canManageGlossaries) {
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
          createGlossaryTermProposal.hasParentNode() && createGlossaryTermProposal.getParentNode() != null
              ? Optional.of(createGlossaryTermProposal.getParentNode()) : Optional.empty();
      return isAuthorizedToResolveGlossaryEntityAsOwner(actorUrn, parentNode);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create glossary term entity");
    }
  }

  public void acceptUpdateResourceDescriptionProposal(@Nonnull final ActionRequestSnapshot actionRequestSnapshot,
      final Authentication authentication) throws Exception {
    Objects.requireNonNull(actionRequestSnapshot, "actionRequestSnapshot cannot be null");

    ActionRequestInfo actionRequestInfo = findActionRequestInfoAspect(actionRequestSnapshot);
    if (!actionRequestInfo.hasResource()) {
      throw new RuntimeException("actionRequestInfo is missing resource");
    }
    Urn resourceUrn = Urn.createFromString(actionRequestInfo.getResource());
    DescriptionProposal descriptionProposal = actionRequestInfo.getParams().getUpdateDescriptionProposal();
    String description = descriptionProposal.getDescription();
    switch (resourceUrn.getEntityType()) {
      case GLOSSARY_NODE_ENTITY_NAME:
        updateGlossaryNodeDescription(resourceUrn, description, authentication);
        break;
      case GLOSSARY_TERM_ENTITY_NAME:
        updateGlossaryTermDescription(resourceUrn, description, authentication);
        break;
      default:
        log.warn(String.format("Proposing an update to a description is currently not supported for entity type %s",
            resourceUrn.getEntityType()));
        break;
    }
  }

  public void completeProposal(@Nonnull final Urn actorUrn, @Nonnull final String actionRequestStatus,
      @Nonnull final String actionRequestResult, @Nonnull final Entity proposalEntity) {
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

  Pair<List<Urn>, List<Urn>> getAssignedUsersAndGroups(@Nonnull final String privilegeType,
      @Nonnull final Optional<Urn> urnOptional, Authorizer dataHubAuthorizer) {
    AuthorizedActors authorizedActors = dataHubAuthorizer.authorizedActors(privilegeType, Optional.empty());

    List<Urn> assignedUsers = authorizedActors == null ? new ArrayList<>() : authorizedActors.getUsers();
    List<Urn> assignedGroups = authorizedActors == null ? new ArrayList<>() : authorizedActors.getGroups();
    if (urnOptional.isPresent()) {
      Ownership ownership = (Ownership) _entityService.getLatestAspect(urnOptional.get(), OWNERSHIP_ASPECT_NAME);
      if (ownership != null && ownership.hasOwners()) {
        Pair<List<Urn>, List<Urn>> urnOwners = getUrnOwners(urnOptional.get());
        List<Urn> userOwners = urnOwners.getFirst();
        List<Urn> groupOwners = urnOwners.getSecond();

        assignedUsers.addAll(userOwners);
        assignedGroups.addAll(groupOwners);
      }
    }
    return Pair.of(assignedUsers, assignedGroups);
  }

  Pair<List<Urn>, List<Urn>> getUrnOwners(@Nonnull final Urn urn) {
    Ownership ownership = (Ownership) _entityService.getLatestAspect(urn, OWNERSHIP_ASPECT_NAME);
    List<Urn> userOwners = ownership.getOwners()
        .stream()
        .map(Owner::getOwner)
        .filter(owner -> owner.getEntityType().equals(CORP_USER_ENTITY_NAME))
        .collect(Collectors.toList());
    List<Urn> groupOwners = ownership.getOwners()
        .stream()
        .map(Owner::getOwner)
        .filter(owner -> owner.getEntityType().equals(CORP_GROUP_ENTITY_NAME))
        .collect(Collectors.toList());

    return Pair.of(userOwners, groupOwners);
  }

  static ActionRequestInfo findActionRequestInfoAspect(@Nonnull final ActionRequestSnapshot actionRequestSnapshot) {
    Optional<ActionRequestAspect> actionRequestInfoOptional =
        actionRequestSnapshot.getAspects().stream().filter(ActionRequestAspect::isActionRequestInfo).findFirst();
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

  static ActionRequestSnapshot setStatusSnapshot(@Nonnull final Urn actorUrn, @Nonnull final String actionRequestStatus,
      @Nonnull final String actionRequestResult, @Nonnull final Entity proposalEntity) {
    ActionRequestStatus statusAspect = proposalEntity.getValue()
        .getActionRequestSnapshot()
        .getAspects()
        .stream()
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

  static ActionRequestSnapshot createCreateGlossaryNodeProposalActionRequest(@Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers, @Nonnull final List<Urn> assignedGroups, @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(ActionRequestAspect.create(
        createCreateGlossaryNodeActionRequestInfo(actorUrn, assignedUsers, assignedGroups, name, parentNode)));

    result.setAspects(aspects);

    return result;
  }

  static ActionRequestSnapshot createCreateGlossaryTermProposalActionRequest(@Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers, @Nonnull final List<Urn> assignedGroups, @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(ActionRequestAspect.create(
        createCreateGlossaryTermActionRequestInfo(actorUrn, assignedUsers, assignedGroups, name, parentNode)));

    result.setAspects(aspects);

    return result;
  }

  static ActionRequestSnapshot createUpdateDescriptionProposalActionRequest(@Nonnull final Urn actorUrn,
      @Nonnull final Urn resourceUrn, @Nonnull final List<Urn> assignedUsers, @Nonnull final List<Urn> assignedGroups,
      @Nonnull final String description) {
    final ActionRequestSnapshot result = new ActionRequestSnapshot();

    final UUID uuid = UUID.randomUUID();
    final String uuidStr = uuid.toString();
    result.setUrn(Urn.createFromTuple(ACTION_REQUEST_ENTITY_NAME, uuidStr));

    final ActionRequestAspectArray aspects = new ActionRequestAspectArray();
    aspects.add(ActionRequestAspect.create(createActionRequestStatus(actorUrn)));
    aspects.add(ActionRequestAspect.create(
        createUpdateDescriptionActionRequestInfo(actorUrn, resourceUrn, assignedUsers, assignedGroups, description)));

    result.setAspects(aspects);

    return result;
  }

  static ActionRequestInfo createCreateGlossaryNodeActionRequestInfo(final Urn creator, final List<Urn> assignedUsers,
      final List<Urn> assignedGroups, final String name, final Optional<Urn> parentNode) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(CREATE_GLOSSARY_NODE_ACTION_REQUEST_TYPE);
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setResourceType(GLOSSARY_NODE_ENTITY_NAME);
    info.setParams(createCreateGlossaryNodeActionRequestParams(name, parentNode));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(creator);

    return info;
  }

  static ActionRequestInfo createCreateGlossaryTermActionRequestInfo(@Nonnull final Urn actorUrn,
      @Nonnull final List<Urn> assignedUsers, @Nonnull final List<Urn> assignedGroups, @Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(CREATE_GLOSSARY_TERM_ACTION_REQUEST_TYPE);
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setResourceType(GLOSSARY_TERM_ENTITY_NAME);
    info.setParams(createCreateGlossaryTermActionRequestParams(name, parentNode));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(actorUrn);

    return info;
  }

  static ActionRequestInfo createUpdateDescriptionActionRequestInfo(@Nonnull final Urn actorUrn,
      @Nonnull final Urn resourceUrn, @Nonnull final List<Urn> assignedUsers, @Nonnull final List<Urn> assignedGroups,
      @Nonnull final String description) {
    final ActionRequestInfo info = new ActionRequestInfo();
    info.setType(UPDATE_DESCRIPTION_ACTION_REQUEST_TYPE);
    info.setResource(resourceUrn.toString());
    info.setAssignedUsers(new UrnArray(assignedUsers));
    info.setAssignedGroups(new UrnArray(assignedGroups));
    info.setResourceType(resourceUrn.getEntityType());
    info.setParams(createUpdateDescriptionActionRequestParams(description));

    info.setCreated(System.currentTimeMillis());
    info.setCreatedBy(actorUrn);

    return info;
  }

  static ActionRequestParams createCreateGlossaryNodeActionRequestParams(@Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode) {
    CreateGlossaryNodeProposal createGlossaryNodeProposal = new CreateGlossaryNodeProposal();
    createGlossaryNodeProposal.setName(name);
    parentNode.ifPresent(createGlossaryNodeProposal::setParentNode);

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setCreateGlossaryNodeProposal(createGlossaryNodeProposal);
    return actionRequestParams;
  }

  static ActionRequestParams createCreateGlossaryTermActionRequestParams(@Nonnull final String name,
      @Nonnull final Optional<Urn> parentNode) {
    CreateGlossaryTermProposal createGlossaryTermProposal = new CreateGlossaryTermProposal();
    createGlossaryTermProposal.setName(name);
    parentNode.ifPresent(createGlossaryTermProposal::setParentNode);

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setCreateGlossaryTermProposal(createGlossaryTermProposal);
    return actionRequestParams;
  }

  static ActionRequestParams createUpdateDescriptionActionRequestParams(@Nonnull final String description) {
    DescriptionProposal descriptionProposal = new DescriptionProposal();
    descriptionProposal.setDescription(description);

    ActionRequestParams actionRequestParams = new ActionRequestParams();
    actionRequestParams.setUpdateDescriptionProposal(descriptionProposal);
    return actionRequestParams;
  }

  void createGlossaryNodeEntity(@Nonnull final String name, @Nonnull final Optional<Urn> parentNode,
      final Authentication authentication) throws Exception {
    final GlossaryNodeKey key = new GlossaryNodeKey();

    final String id = UUID.randomUUID().toString();
    key.setName(id);

    if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(key, Constants.GLOSSARY_NODE_ENTITY_NAME),
        authentication)) {
      throw new IllegalArgumentException("This Glossary Node already exists!");
    }

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.GLOSSARY_NODE_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(mapGlossaryNodeInfo(name, parentNode)));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityClient.ingestProposal(proposal, authentication);
  }

  void createGlossaryTermEntity(@Nonnull final String name, @Nonnull final Optional<Urn> parentNode,
      final Authentication authentication) throws Exception {
    final GlossaryTermKey key = new GlossaryTermKey();

    final String id = UUID.randomUUID().toString();
    key.setName(id);

    if (_entityClient.exists(EntityKeyUtils.convertEntityKeyToUrn(key, Constants.GLOSSARY_TERM_ENTITY_NAME),
        authentication)) {
      throw new IllegalArgumentException("This Glossary Term already exists!");
    }

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));
    proposal.setEntityType(Constants.GLOSSARY_TERM_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(mapGlossaryTermInfo(name, parentNode)));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityClient.ingestProposal(proposal, authentication);
  }

  void updateGlossaryNodeDescription(@Nonnull final Urn resourceUrn, @Nonnull final String description,
      final Authentication authentication) throws Exception {
    GlossaryNodeInfo glossaryNodeInfo =
        (GlossaryNodeInfo) _entityService.getLatestAspect(resourceUrn, GLOSSARY_NODE_INFO_ASPECT_NAME);
    Objects.requireNonNull(glossaryNodeInfo, "glossaryNodeInfo cannot be null");

    glossaryNodeInfo.setDefinition(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.GLOSSARY_NODE_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_NODE_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(glossaryNodeInfo));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityClient.ingestProposal(proposal, authentication);
  }

  void updateGlossaryTermDescription(@Nonnull final Urn resourceUrn, @Nonnull final String description,
      final Authentication authentication) throws Exception {
    GlossaryTermInfo glossaryTermInfo =
        (GlossaryTermInfo) _entityService.getLatestAspect(resourceUrn, GLOSSARY_TERM_INFO_ASPECT_NAME);
    Objects.requireNonNull(glossaryTermInfo, "glossaryTermInfo cannot be null");

    glossaryTermInfo.setDefinition(description);

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(resourceUrn);
    proposal.setEntityType(Constants.GLOSSARY_TERM_ENTITY_NAME);
    proposal.setAspectName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(glossaryTermInfo));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityClient.ingestProposal(proposal, authentication);
  }

  GlossaryNodeInfo mapGlossaryNodeInfo(@Nonnull final String name, @Nonnull final Optional<Urn> parentNode) throws Exception {
    final GlossaryNodeInfo result = new GlossaryNodeInfo();
    result.setName(name);
    result.setDefinition("");
    if (parentNode.isPresent()) {
      final GlossaryNodeUrn nodeUrn = GlossaryNodeUrn.createFromString(parentNode.get().toString());
      result.setParentNode(nodeUrn);
    }

    return result;
  }

  GlossaryTermInfo mapGlossaryTermInfo(@Nonnull final String name, @Nonnull final Optional<Urn> parentNode) throws Exception {
    final GlossaryTermInfo result = new GlossaryTermInfo();
    result.setName(name);
    result.setDefinition("");
    result.setTermSource("INTERNAL");
    if (parentNode.isPresent()) {
      final GlossaryNodeUrn nodeUrn = GlossaryNodeUrn.createFromString(parentNode.get().toString());
      result.setParentNode(nodeUrn);
    }

    return result;
  }
}
