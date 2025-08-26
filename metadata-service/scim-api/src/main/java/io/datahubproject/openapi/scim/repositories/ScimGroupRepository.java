package io.datahubproject.openapi.scim.repositories;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.QueryUtils.*;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Striped;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.directory.scim.server.exception.UnableToResolveIdResourceException;
import org.apache.directory.scim.spec.exception.ResourceException;
import org.apache.directory.scim.spec.resources.ScimGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ScimGroupRepository
    extends AbstractScimRepository<ScimGroup, CorpGroupUrn, CorpGroupKey> {

  private static final int MAX_RELATIONSHIPS = 5000;

  @Autowired ScimUserRepository _userRepository;

  @Autowired GraphService _graphService;

  @Qualifier("systemOperationContext")
  @Autowired
  OperationContext systemOperationContext;

  // been in beta for over a decade apparently ... so using it regardless ...
  private static Striped<Lock> userLocks =
      Striped.lazyWeakLock(Runtime.getRuntime().availableProcessors());

  @Override
  public Class<ScimGroup> getResourceClass() {
    return ScimGroup.class;
  }

  @Override
  void checkKeyMutation(ScimGroup scimGroup, ScimReadableCorpEntity corpGroup)
      throws ResourceException {
    if (scimGroup.getDisplayName() != null
        && !scimGroup.getDisplayName().equals(corpGroup.getKey().getName())) {
      throw new ResourceException(
          HttpStatus.BAD_REQUEST.value(),
          "groupName cannot be modified. "
              + corpGroup.getKey().getName()
              + " => "
              + scimGroup.getDisplayName());
    }
  }

  @Override
  ScimGroup ingestUpdates(
      ScimGroup scimGroup, ScimReadableCorpEntity corpGroup, AuditStamp auditStamp) {

    if (!Strings.isNullOrEmpty(scimGroup.getExternalId()) && scimGroup.getMembers() == null) {
      // intent is likely to send externalId only; no changes in group membership
      // such message patching externalId is observed with MS Entra ID.
      return corpGroup.asScimResource();
    }

    List<org.apache.directory.scim.spec.resources.GroupMembership> expected =
        scimGroup.getMembers().stream()
            .filter(
                x -> {
                  try {
                    // check that id is valid (corresponding to CorpuserUrn)
                    // not checking if user exists
                    return (x.getType() == null
                            || x.getType()
                                .equals(
                                    org.apache.directory.scim.spec.resources.GroupMembership.Type
                                        .USER))
                        && _userRepository.urnFromId(x.getValue()) != null;
                  } catch (UnableToResolveIdResourceException e) {
                    log.warn("Unable to resolve member with id " + x.getValue());
                    return false;
                  }
                })
            .collect(Collectors.toList());

    expected.forEach(
        x -> x.setType(org.apache.directory.scim.spec.resources.GroupMembership.Type.USER));

    List<org.apache.directory.scim.spec.resources.GroupMembership> actual =
        groupMembershipFromRelatedUsers(findUsersInGroup(corpGroup.getUrn()));

    // clear Refs so they don't affect equality
    expected.forEach(x -> x.setRef(null));
    actual.forEach(x -> x.setRef(null));

    List<org.apache.directory.scim.spec.resources.GroupMembership> usersToAdd =
        listDiff(expected, actual);
    List<org.apache.directory.scim.spec.resources.GroupMembership> usersToRemove =
        listDiff(actual, expected);

    boolean groupChanged = processUsers(usersToAdd, usersToRemove, corpGroup.getUrn(), auditStamp);
    ScimGroup result = corpGroup.asScimResource();
    if (groupChanged) {
      // touch entity
      touchGroupStatus(corpGroup.getUrn(), auditStamp);
      result.getMeta().setLastModified(metaTimestamp(auditStamp.getTime()));
      expected.forEach(x -> x.setRef(location(USERS_ENDPOINT, x.getValue())));
      result.setMembers(expected);
    }
    // no other aspects currently; users are populated by this point appropriately.
    return result;
  }

  @Override
  String getResourceType() {
    return ScimGroup.RESOURCE_NAME;
  }

  @Override
  String attributeNameToFindBy() {
    return "displayName";
  }

  private CorpGroupKey ingestGroupKey(
      ScimGroup scimGroup, CorpGroupUrn urn, AuditStamp auditStamp) {
    CorpGroupKey groupKey = new CorpGroupKey();
    groupKey.setName(scimGroup.getDisplayName());
    ingest(urn, CORP_GROUP_KEY_ASPECT_NAME, groupKey, auditStamp);
    return groupKey;
  }

  private void touchGroupStatus(CorpGroupUrn urn, AuditStamp auditStamp) {
    Status status = new Status();
    status.setRemoved(false);
    ingest(urn, STATUS_ASPECT_NAME, status, auditStamp);
  }

  @Override
  Map<Class<? extends RecordTemplate>, RecordTemplate> ingestCreate(
      ScimGroup scimGroup, CorpGroupUrn urn, AuditStamp auditStamp) {
    CorpGroupKey groupKey = ingestGroupKey(scimGroup, urn, auditStamp);

    Map<Class<? extends RecordTemplate>, RecordTemplate> result = new HashMap<>();
    result.put(CorpGroupKey.class, groupKey);

    if (scimGroup.getMembers() != null) {
      processUsers(scimGroup.getMembers(), Collections.emptyList(), urn, auditStamp);
    }

    touchGroupStatus(urn, auditStamp);
    return result;
  }

  private boolean processGroupMembership(
      CorpGroupUrn urn,
      AuditStamp auditStamp,
      org.apache.directory.scim.spec.resources.GroupMembership groupMembership,
      boolean add) {
    String memberId = groupMembership.getValue();

    CorpuserUrn userUrn = null;
    try {
      userUrn = _userRepository.urnFromId(memberId);
    } catch (UnableToResolveIdResourceException e) {
      log.warn("Unable to resolve member with id " + memberId);
      return false;
    }

    // guard against concurrent Read-Modify-Write of a user's groupMembership
    Lock lock = userLocks.get(memberId);
    lock.lock();

    boolean changed = false;
    try {
      // Note: not checking if user exists
      GroupMembership aspect =
          (GroupMembership)
              _entityService.getLatestAspect(
                  systemOperationContext, userUrn, GROUP_MEMBERSHIP_ASPECT_NAME);
      if (aspect == null) {
        aspect = new GroupMembership();
      }
      UrnArray usersGroups = aspect.getGroups(GetMode.NULL);
      if (usersGroups == null) {
        usersGroups = new UrnArray();
      }

      if (add && !usersGroups.contains(urn)) {
        usersGroups.add(urn);
        aspect.setGroups(usersGroups);
        _userRepository.ingest(userUrn, GROUP_MEMBERSHIP_ASPECT_NAME, aspect, auditStamp);
        log.debug(String.format("Added group %s to user %s", urn, userUrn));
        changed = true;
      }
      if (!add && usersGroups.contains(urn)) {
        usersGroups.remove(urn);
        aspect.setGroups(usersGroups);
        _userRepository.ingest(userUrn, GROUP_MEMBERSHIP_ASPECT_NAME, aspect, auditStamp);
        log.debug(String.format("Removed group %s from user %s", urn, userUrn));
        changed = true;
      }
    } finally {
      lock.unlock();
    }
    return changed;
  }

  private boolean processUsers(
      List<org.apache.directory.scim.spec.resources.GroupMembership> usersToAdd,
      List<org.apache.directory.scim.spec.resources.GroupMembership> usersToRemove,
      CorpGroupUrn urn,
      AuditStamp auditStamp) {

    Set<Boolean> added =
        usersToAdd.stream()
            .map(
                groupMembership -> {
                  return processGroupMembership(urn, auditStamp, groupMembership, true);
                })
            .collect(Collectors.toSet());

    Set<Boolean> removed =
        usersToRemove.stream()
            .map(
                groupMembership -> {
                  return processGroupMembership(urn, auditStamp, groupMembership, false);
                })
            .collect(Collectors.toSet());

    return added.contains(true) || removed.contains(true);
  }

  @Override
  CorpGroupUrn urnFromStr(String urnStr) throws UnableToResolveIdResourceException {
    try {
      return CorpGroupUrn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw invalidResourceException(urnStr);
    }
  }

  @Override
  CorpGroupUrn urnFromName(String name) {
    return new CorpGroupUrn(name);
  }

  @Override
  String nameFromResource(ScimGroup resource) {
    return resource.getDisplayName();
  }

  @Override
  String invalidResourceMsg(String id) {
    return "Group " + id + " is invalid.";
  }

  @Override
  String resourceNotFoundMsg(String id) {
    return "Group " + id + " not found.";
  }

  @Override
  String entityName() {
    return CORP_GROUP_ENTITY_NAME;
  }

  @Override
  protected String keyAspectName() {
    return CORP_GROUP_KEY_ASPECT_NAME;
  }

  @Override
  Map<String, Class<? extends RecordTemplate>> aspectNamesToClasses() {
    Map<String, Class<? extends RecordTemplate>> result = new HashMap<>();
    result.put(CORP_GROUP_KEY_ASPECT_NAME, CorpGroupKey.class);
    result.put(STATUS_ASPECT_NAME, Status.class);
    return result;
  }

  @Override
  ScimGroup newScimResource() {
    return new ScimGroup();
  }

  @Override
  void sinkKeyToResource(CorpGroupKey key, ScimGroup resource) {
    resource.setDisplayName(key.getName());
  }

  @Override
  void sinkAspectsToResource(
      CorpGroupUrn urn,
      Map<Class<? extends RecordTemplate>, RecordTemplate> aspects,
      ScimGroup resource) {

    // may need to use scrolling API later for larger sizes
    RelatedEntitiesResult relatedUsers = findUsersInGroup(urn);

    if (relatedUsers.getEntities().isEmpty()) {
      return;
    }

    resource.setMembers(groupMembershipFromRelatedUsers(relatedUsers));
  }

  @Override
  String endpointName() {
    return GROUPS_ENDPOINT;
  }

  private List<org.apache.directory.scim.spec.resources.GroupMembership>
      groupMembershipFromRelatedUsers(RelatedEntitiesResult relatedUsers) {
    return relatedUsers.getEntities().stream()
        .map(
            relatedUser -> {
              String userId = urnToId(relatedUser.getUrn());

              org.apache.directory.scim.spec.resources.GroupMembership groupMembership =
                  new org.apache.directory.scim.spec.resources.GroupMembership();
              groupMembership.setValue(userId);

              String location = location(USERS_ENDPOINT, userId);
              groupMembership.setRef(location);
              groupMembership.setType(
                  org.apache.directory.scim.spec.resources.GroupMembership.Type.USER);

              return groupMembership;
            })
        .collect(Collectors.toList());
  }

  private RelatedEntitiesResult findUsersInGroup(CorpGroupUrn urn) {
    RelatedEntitiesResult relatedUsers =
        _graphService.findRelatedEntities(
            systemOperationContext,
            null,
            QueryUtils.newFilter("urn", urn.toString()),
            null,
            QueryUtils.EMPTY_FILTER,
            ImmutableSet.of(GROUP_MEMBERSHIP_RELATIONSHIP_NAME),
            QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
            0,
            MAX_RELATIONSHIPS);
    return relatedUsers;
  }

  @Override
  void preDelete(CorpGroupUrn urn, AuditStamp auditStamp) {
    super.preDelete(urn, auditStamp);

    log.debug("Predelete of group " + urn);

    // Ideally, this function should not be required.
    // We should simply use DeleteEntityService.
    // But somehow that didn't seem to work in our case, and needs to be revisited.

    RelatedEntitiesResult relatedUsers = findUsersInGroup(urn);

    if (relatedUsers.getEntities().isEmpty()) {
      return;
    }

    relatedUsers
        .getEntities()
        .forEach(
            relatedEntity -> {
              CorpuserUrn userUrn = null;
              try {
                userUrn = _userRepository.urnFromStr(relatedEntity.getUrn());
              } catch (UnableToResolveIdResourceException e) {
                throw new RuntimeException(e);
              }
              Lock lock = userLocks.get(_userRepository.urnToId(userUrn));
              lock.lock();
              try {
                GroupMembership aspect =
                    (GroupMembership)
                        _entityService.getLatestAspect(
                            systemOperationContext, userUrn, GROUP_MEMBERSHIP_ASPECT_NAME);
                if (aspect == null || aspect.getGroups(GetMode.NULL) == null) {
                  return;
                }
                UrnArray usersGroups = aspect.getGroups(GetMode.NULL);
                usersGroups.remove(urn);
                _userRepository.ingest(userUrn, GROUP_MEMBERSHIP_ASPECT_NAME, aspect, auditStamp);
                log.debug(String.format("Removed group %s from user %s", urn, userUrn));
              } finally {
                lock.unlock();
              }
            });
  }
}
