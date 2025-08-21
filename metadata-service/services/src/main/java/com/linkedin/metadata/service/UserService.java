package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for resolving user groups, roles, and other user information.
 *
 * <p>This service provides centralized methods to resolve a user's group memberships, role
 * memberships, and email address, which is a common pattern used across multiple resolvers and
 * services in the codebase.
 */
@Slf4j
public class UserService {

  private static final String IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME = "IsMemberOfRole";
  private static final String IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME = "IsMemberOfGroup";
  private static final String IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME = "IsMemberOfNativeGroup";
  private static final String CORP_GROUP_URN_PREFIX = "urn:li:corpGroup";
  private static final String CORP_USER_URN_PREFIX = "urn:li:corpuser";
  private static final int RESOLUTION_BATCH_SIZE = 1000;

  private final EntityClient entityClient;
  private final OperationContext systemOperationContext;
  private final GraphClient graphClient;

  public UserService(
      @Nonnull EntityClient entityClient,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull GraphClient graphClient) {
    this.entityClient = entityClient;
    this.systemOperationContext = systemOperationContext;
    this.graphClient = graphClient;
  }

  /**
   * Resolves a user's email address from their CorpUserInfo aspect.
   *
   * @param opContext the operation context
   * @param userUrn the URN of the user
   * @return the user's email address, or null if not found
   * @throws Exception if there's an error fetching user data
   */
  @Nullable
  public String getUserEmail(@Nonnull OperationContext opContext, @Nonnull Urn userUrn)
      throws Exception {

    try {
      final EntityResponse response =
          this.entityClient.getV2(opContext, CORP_USER_ENTITY_NAME, userUrn, null);

      if (response == null) {
        log.warn("User not found: {}", userUrn);
        return null;
      }

      final EnvelopedAspectMap aspects = response.getAspects();

      // Extract email from CorpUserInfo
      if (aspects.get(CORP_USER_INFO_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(CORP_USER_INFO_ASPECT_NAME);
        final CorpUserInfo corpUserInfo = new CorpUserInfo(aspect.getValue().data());
        if (corpUserInfo.hasEmail()) {
          return corpUserInfo.getEmail().toString();
        }
      }

      return null;

    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to fetch user data for urn %s", userUrn), e);
    }
  }

  /**
   * Resolves comprehensive user information including email, groups, and roles.
   *
   * @param opContext the operation context
   * @param userUrn the URN of the user
   * @return UserInfo containing the user's email, groups, and roles
   * @throws Exception if there's an error fetching user data
   */
  @Nonnull
  public UserInfo getUserInfo(@Nonnull OperationContext opContext, @Nonnull Urn userUrn)
      throws Exception {

    List<Urn> groupUrns = new ArrayList<>();
    List<Urn> roleUrns = new ArrayList<>();
    String email = null;

    try {
      final EntityResponse response =
          this.entityClient.getV2(opContext, CORP_USER_ENTITY_NAME, userUrn, null);

      if (response == null) {
        log.warn("User not found: {}", userUrn);
        return new UserInfo(email, groupUrns, roleUrns);
      }

      final EnvelopedAspectMap aspects = response.getAspects();

      // Extract email from CorpUserInfo
      if (aspects.get(CORP_USER_INFO_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(CORP_USER_INFO_ASPECT_NAME);
        final CorpUserInfo corpUserInfo = new CorpUserInfo(aspect.getValue().data());
        if (corpUserInfo.hasEmail()) {
          email = corpUserInfo.getEmail().toString();
        }
      }

      // Extract group memberships
      if (aspects.get(GROUP_MEMBERSHIP_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(GROUP_MEMBERSHIP_ASPECT_NAME);
        final GroupMembership groupMembership = new GroupMembership(aspect.getValue().data());
        groupUrns.addAll(groupMembership.getGroups());
      }

      // Extract native group memberships
      if (aspects.get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME);
        final NativeGroupMembership nativeGroupMembership =
            new NativeGroupMembership(aspect.getValue().data());
        groupUrns.addAll(nativeGroupMembership.getNativeGroups());
      }

      // Extract role memberships
      if (aspects.get(ROLE_MEMBERSHIP_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(ROLE_MEMBERSHIP_ASPECT_NAME);
        final RoleMembership roleMembership = new RoleMembership(aspect.getValue().data());
        roleUrns.addAll(roleMembership.getRoles());
      }

      return new UserInfo(email, groupUrns, roleUrns);

    } catch (RemoteInvocationException e) {
      throw new RuntimeException(String.format("Failed to fetch user data for urn %s", userUrn), e);
    }
  }

  /**
   * Resolves a user's group and role memberships.
   *
   * @param opContext the operation context
   * @param userUrn the URN of the user
   * @return UserMemberships containing the user's groups and roles
   * @throws Exception if there's an error fetching user data
   */
  @Nonnull
  public UserMemberships getUserMemberships(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn) throws Exception {
    UserInfo userInfo = getUserInfo(opContext, userUrn);
    // ensure we get all the roles granted to this user through their groups
    Set<Urn> finalRoles = new HashSet<>(userInfo.getRoles());
    if (userInfo.getGroups().size() > 0) {
      finalRoles.addAll(getUserRolesFromUserGroups(opContext, userInfo.getGroups()));
    }
    return new UserMemberships(userInfo.getGroups(), new ArrayList<>(finalRoles));
  }

  /**
   * Resolves only a user's group memberships.
   *
   * @param opContext the operation context
   * @param userUrn the URN of the user
   * @return List of group URNs the user belongs to
   * @throws Exception if there's an error fetching user data
   */
  @Nonnull
  public List<Urn> getUserGroups(@Nonnull OperationContext opContext, @Nonnull Urn userUrn)
      throws Exception {
    return getUserMemberships(opContext, userUrn).getGroups();
  }

  /**
   * Resolves only a user's role memberships.
   *
   * @param opContext the operation context
   * @param userUrn the URN of the user
   * @return List of role URNs the user belongs to
   * @throws Exception if there's an error fetching user data
   */
  @Nonnull
  public List<Urn> getUserRoles(@Nonnull OperationContext opContext, @Nonnull Urn userUrn)
      throws Exception {
    return getUserMemberships(opContext, userUrn).getRoles();
  }

  /**
   * Resolves all the roles a user inherits from their groups
   *
   * @param opContext the operation context
   * @param groupUrns the URNs of the groups this user is a member of
   * @return List of roles URNs the user belongs to based on all of their groups
   */
  @Nonnull
  public List<Urn> getUserRolesFromUserGroups(
      @Nonnull OperationContext opContext, @Nonnull List<Urn> groupUrns) {
    Set<Urn> roleUrns = new HashSet<>();

    groupUrns.forEach(
        groupUrn -> {
          try {
            roleUrns.addAll(getGroupRoles(opContext, groupUrn));
          } catch (Exception e) {
            log.error(String.format("Error resolving roles for group with urn %s", groupUrn));
          }
        });

    return new ArrayList<>(roleUrns);
  }

  /**
   * Resolves a group's roles
   *
   * @param opContext the operation context
   * @param groupUrn the URN of the group
   * @return List of role URNs the user belongs to
   * @throws Exception if there's an error fetching user data
   */
  @Nonnull
  public List<Urn> getGroupRoles(@Nonnull OperationContext opContext, @Nonnull Urn groupUrn)
      throws Exception {
    List<Urn> roleUrns = new ArrayList<>();

    try {
      final EntityResponse response =
          this.entityClient.getV2(
              opContext, CORP_GROUP_ENTITY_NAME, groupUrn, Set.of(ROLE_MEMBERSHIP_ASPECT_NAME));

      if (response == null) {
        log.warn("Group not found: {}", groupUrn);
        return roleUrns;
      }

      final EnvelopedAspectMap aspects = response.getAspects();

      // Extract role memberships
      if (aspects.get(ROLE_MEMBERSHIP_ASPECT_NAME) != null) {
        EnvelopedAspect aspect = aspects.get(ROLE_MEMBERSHIP_ASPECT_NAME);
        final RoleMembership roleMembership = new RoleMembership(aspect.getValue().data());
        roleUrns.addAll(roleMembership.getRoles());
      }

      return roleUrns;
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(
          String.format("Failed to fetch user data for urn %s", groupUrn), e);
    }
  }

  /**
   * Checks if a user is a member of any of the specified groups.
   *
   * @param opContext the operation context
   * @param userUrn the URN of the user
   * @param groupUrns the group URNs to check membership for
   * @return true if the user is a member of any of the specified groups
   * @throws Exception if there's an error fetching user data
   */
  public boolean isUserMemberOfAnyGroup(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn, @Nonnull List<Urn> groupUrns)
      throws Exception {

    if (groupUrns.isEmpty()) {
      return false;
    }

    List<Urn> userGroups = getUserGroups(opContext, userUrn);
    return userGroups.stream().anyMatch(groupUrns::contains);
  }

  /**
   * Checks if a user has any of the specified roles.
   *
   * @param opContext the operation context
   * @param userUrn the URN of the user
   * @param roleUrns the role URNs to check membership for
   * @return true if the user has any of the specified roles
   * @throws Exception if there's an error fetching user data
   */
  public boolean isUserMemberOfAnyRole(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn, @Nonnull List<Urn> roleUrns)
      throws Exception {

    if (roleUrns.isEmpty()) {
      return false;
    }

    List<Urn> userRoles = getUserRoles(opContext, userUrn);
    return userRoles.stream().anyMatch(roleUrns::contains);
  }

  /**
   * Resolves a list of group URNs to individual user URNs. This method handles pagination and
   * deduplication automatically.
   *
   * @param opContext the operation context
   * @param groupUrns the list of group URNs to resolve
   * @return List of individual user URNs (deduplicated)
   * @throws Exception if there's an error during resolution
   */
  @Nonnull
  public List<Urn> resolveGroupUsers(
      @Nonnull OperationContext opContext, @Nonnull List<Urn> groupUrns) throws Exception {
    if (groupUrns.isEmpty()) {
      return Collections.emptyList();
    }

    final Set<Urn> allUsers = new HashSet<>();

    for (Urn groupUrn : groupUrns) {
      try {
        List<Urn> usersInGroup = getUsersInGroup(opContext, groupUrn);
        allUsers.addAll(usersInGroup);
      } catch (Exception e) {
        log.error("Failed to resolve users for group: {}. Skipping this group.", groupUrn, e);
      }
    }

    return new ArrayList<>(allUsers);
  }

  /**
   * Resolves a list of group URNs to individual user URNs. This method handles pagination and
   * deduplication automatically.
   *
   * @deprecated Use {@link #resolveGroupUsers(OperationContext, List)} instead
   */
  @Deprecated
  @Nonnull
  public List<Urn> resolveUsersFromGroups(
      @Nonnull OperationContext opContext, @Nonnull List<Urn> groupUrns) throws Exception {
    return resolveGroupUsers(opContext, groupUrns);
  }

  /**
   * Resolves a single group to its member users.
   *
   * @param opContext the operation context
   * @param groupUrn the group URN to resolve
   * @return List of user URNs that are members of the specified group
   * @throws Exception if there's an error during resolution
   */
  @Nonnull
  public List<Urn> getUsersInGroup(@Nonnull OperationContext opContext, @Nonnull Urn groupUrn)
      throws Exception {
    int start = 0;
    int total = Integer.MAX_VALUE;
    final List<Urn> resolvedUsers = new ArrayList<>();

    while (start < total) {
      // Fetch users that are members of this group
      final EntityRelationships entityRelationships =
          this.graphClient.getRelatedEntities(
              groupUrn.toString(),
              ImmutableSet.of(
                  IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME,
                  IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              start,
              RESOLUTION_BATCH_SIZE,
              opContext.getActorContext().getActorUrn().toString());

      if (entityRelationships == null || entityRelationships.getRelationships() == null) {
        break;
      }

      // Add fetched users to the list (filter to only include users, not other entity types)
      for (final EntityRelationship relationship : entityRelationships.getRelationships()) {
        Urn entityUrn = relationship.getEntity();
        if (entityUrn.toString().startsWith(CORP_USER_URN_PREFIX)) {
          resolvedUsers.add(entityUrn);
        }
      }

      total = entityRelationships.getTotal();
      start += RESOLUTION_BATCH_SIZE;
    }
    return resolvedUsers;
  }

  /**
   * Resolves a list of roles to their member actors (users and groups).
   *
   * @param opContext the operation context
   * @param roleUrns the role URNs to resolve
   * @return List of actor URNs (users and groups) that have any of the specified roles
   * @throws Exception if there's an error during resolution
   */
  @Nonnull
  public List<Urn> resolveActorsFromRoles(
      @Nonnull OperationContext opContext, @Nonnull List<Urn> roleUrns) throws Exception {
    final Set<Urn> allActorsWithAnyRoles = new HashSet<>();
    for (final Urn role : roleUrns) {
      try {
        List<Urn> allRelationships = getActorsWithRole(opContext, role);
        allActorsWithAnyRoles.addAll(allRelationships);
      } catch (Exception e) {
        log.error(
            "Failed to resolve actors with role: {}. Skipping this role!", role.toString(), e);
      }
    }
    return new ArrayList<>(allActorsWithAnyRoles);
  }

  /**
   * Resolves a single role to its member actors (users and groups).
   *
   * @param opContext the operation context
   * @param roleUrn the role URN to resolve
   * @return List of actor URNs (users and groups) that have the specified role
   * @throws Exception if there's an error during resolution
   */
  @Nonnull
  public List<Urn> getActorsWithRole(@Nonnull OperationContext opContext, @Nonnull Urn roleUrn)
      throws Exception {
    int start = 0;
    int total = Integer.MAX_VALUE;
    final List<Urn> resolvedActors = new ArrayList<>();

    while (start < total) {
      // Fetch actors with role
      final EntityRelationships entityRelationships =
          this.graphClient.getRelatedEntities(
              roleUrn.toString(),
              ImmutableSet.of(IS_MEMBER_OF_ROLE_RELATIONSHIP_NAME),
              RelationshipDirection.INCOMING,
              start,
              RESOLUTION_BATCH_SIZE,
              opContext.getActorContext().getActorUrn().toString());

      if (entityRelationships == null || entityRelationships.getRelationships() == null) {
        break;
      }

      // Add fetched actors to the list
      for (final EntityRelationship relationship : entityRelationships.getRelationships()) {
        resolvedActors.add(relationship.getEntity());
      }

      total = entityRelationships.getTotal();
      start += RESOLUTION_BATCH_SIZE;
    }
    return resolvedActors;
  }

  /**
   * Resolves a list of role URNs to individual user URNs. This method: 1. Resolves roles to actors
   * (users and groups) 2. Extracts direct users from roles 3. Resolves any groups within roles to
   * individual users 4. Combines and deduplicates all users
   *
   * @param opContext the operation context
   * @param roleUrns the list of role URNs to resolve
   * @return List of individual user URNs (deduplicated)
   * @throws Exception if there's an error during resolution
   */
  @Nonnull
  public List<Urn> resolveRoleUsers(
      @Nonnull OperationContext opContext, @Nonnull List<Urn> roleUrns) throws Exception {
    if (roleUrns.isEmpty()) {
      return Collections.emptyList();
    }

    final Set<Urn> allUsers = new HashSet<>();

    try {
      // Resolve roles to actors (users and groups)
      final List<Urn> roleActors = resolveActorsFromRoles(opContext, roleUrns);

      // Extract direct users from roles
      final List<Urn> usersFromRoles =
          roleActors.stream()
              .filter(urn -> urn.toString().startsWith(CORP_USER_URN_PREFIX))
              .collect(Collectors.toList());
      allUsers.addAll(usersFromRoles);

      // Extract groups from roles and resolve them to users
      final List<Urn> groupsFromRoles =
          roleActors.stream()
              .filter(urn -> urn.toString().startsWith(CORP_GROUP_URN_PREFIX))
              .collect(Collectors.toList());

      if (!groupsFromRoles.isEmpty()) {
        final List<Urn> usersFromGroups = resolveGroupUsers(opContext, groupsFromRoles);
        allUsers.addAll(usersFromGroups);
      }

    } catch (Exception e) {
      log.error("Failed to resolve users from roles: {}. Returning empty list.", roleUrns, e);
      return Collections.emptyList();
    }

    return new ArrayList<>(allUsers);
  }

  /**
   * Resolves roles and groups to individual users.
   *
   * <p>This is a convenience method that: 1. Adds directly assigned users 2. Resolves roles to
   * individual users (including users from groups within roles) 3. Resolves groups to individual
   * users 4. Combines all users and deduplicates
   *
   * @param opContext the operation context
   * @param userUrns directly assigned user URNs (can be null)
   * @param groupUrns directly assigned group URNs (can be null)
   * @param roleUrns directly assigned role URNs (can be null)
   * @return List of individual user URNs (deduplicated)
   * @throws Exception if there's an error during resolution
   */
  @Nonnull
  public List<Urn> resolveToIndividualUsers(
      @Nonnull OperationContext opContext,
      @Nullable List<Urn> userUrns,
      @Nullable List<Urn> groupUrns,
      @Nullable List<Urn> roleUrns)
      throws Exception {

    final Set<Urn> allUsers = new HashSet<>();

    // Add directly assigned users
    if (userUrns != null) {
      allUsers.addAll(userUrns);
    }

    // Resolve roles to users
    if (roleUrns != null && !roleUrns.isEmpty()) {
      final List<Urn> usersFromRoles = resolveRoleUsers(opContext, roleUrns);
      allUsers.addAll(usersFromRoles);
    }

    // Resolve groups to users
    if (groupUrns != null && !groupUrns.isEmpty()) {
      final List<Urn> usersFromGroups = resolveGroupUsers(opContext, groupUrns);
      allUsers.addAll(usersFromGroups);
    }

    return new ArrayList<>(allUsers);
  }

  /** Data class representing comprehensive user information including email, groups, and roles. */
  @Value
  public static class UserInfo {
    @Nullable String email;

    @Nonnull List<Urn> groups;

    @Nonnull List<Urn> roles;
  }

  /** Data class representing a user's group and role memberships. */
  @Value
  public static class UserMemberships {
    @Nonnull List<Urn> groups;

    @Nonnull List<Urn> roles;
  }
}
