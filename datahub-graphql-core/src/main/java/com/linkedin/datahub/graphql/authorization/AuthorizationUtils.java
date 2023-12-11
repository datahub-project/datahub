package com.linkedin.datahub.graphql.authorization;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

public class AuthorizationUtils {

  private static final Clock CLOCK = Clock.systemUTC();

  public static AuditStamp createAuditStamp(@Nonnull QueryContext context) {
    return new AuditStamp()
        .setTime(CLOCK.millis())
        .setActor(UrnUtils.getUrn(context.getActorUrn()));
  }

  public static boolean canManageUsersAndGroups(@Nonnull QueryContext context) {
    return isAuthorized(
        context, Optional.empty(), PoliciesConfig.MANAGE_USERS_AND_GROUPS_PRIVILEGE);
  }

  public static boolean canManagePolicies(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_POLICIES_PRIVILEGE);
  }

  public static boolean canGeneratePersonalAccessToken(@Nonnull QueryContext context) {
    return isAuthorized(
        context, Optional.empty(), PoliciesConfig.GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE);
  }

  public static boolean canManageTokens(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_ACCESS_TOKENS);
  }

  /**
   * Returns true if the current used is able to create Domains. This is true if the user has the
   * 'Manage Domains' or 'Create Domains' platform privilege.
   */
  public static boolean canCreateDomains(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.CREATE_DOMAINS_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  public static boolean canManageDomains(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE);
  }

  /**
   * Returns true if the current used is able to create Tags. This is true if the user has the
   * 'Manage Tags' or 'Create Tags' platform privilege.
   */
  public static boolean canCreateTags(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.CREATE_TAGS_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.MANAGE_TAGS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  public static boolean canManageTags(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_TAGS_PRIVILEGE);
  }

  public static boolean canDeleteEntity(@Nonnull Urn entityUrn, @Nonnull QueryContext context) {
    return isAuthorized(
        context,
        Optional.of(new EntitySpec(entityUrn.getEntityType(), entityUrn.toString())),
        PoliciesConfig.DELETE_ENTITY_PRIVILEGE);
  }

  public static boolean canManageUserCredentials(@Nonnull QueryContext context) {
    return isAuthorized(
        context, Optional.empty(), PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE);
  }

  public static boolean canEditGroupMembers(
      @Nonnull String groupUrnStr, @Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        CORP_GROUP_ENTITY_NAME,
        groupUrnStr,
        orPrivilegeGroups);
  }

  public static boolean canCreateGlobalAnnouncements(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.CREATE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE.getType())),
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.MANAGE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  public static boolean canManageGlobalAnnouncements(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.MANAGE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  public static boolean canManageGlobalViews(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_GLOBAL_VIEWS);
  }

  public static boolean canManageOwnershipTypes(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_GLOBAL_OWNERSHIP_TYPES);
  }

  public static boolean canEditEntityQueries(
      @Nonnull List<Urn> entityUrns, @Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType()))));
    return entityUrns.stream()
        .allMatch(
            entityUrn ->
                isAuthorized(
                    context.getAuthorizer(),
                    context.getActorUrn(),
                    entityUrn.getEntityType(),
                    entityUrn.toString(),
                    orPrivilegeGroups));
  }

  public static boolean canCreateQuery(
      @Nonnull List<Urn> subjectUrns, @Nonnull QueryContext context) {
    // Currently - you only need permission to edit an entity's queries to create a query.
    return canEditEntityQueries(subjectUrns, context);
  }

  public static boolean canUpdateQuery(
      @Nonnull List<Urn> subjectUrns, @Nonnull QueryContext context) {
    // Currently - you only need permission to edit an entity's queries to update any query.
    return canEditEntityQueries(subjectUrns, context);
  }

  public static boolean canDeleteQuery(
      @Nonnull Urn entityUrn, @Nonnull List<Urn> subjectUrns, @Nonnull QueryContext context) {
    // Currently - you only need permission to edit an entity's queries to remove any query.
    return canEditEntityQueries(subjectUrns, context);
  }

  public static boolean isAuthorized(
      @Nonnull QueryContext context,
      @Nonnull Optional<EntitySpec> resourceSpec,
      @Nonnull PoliciesConfig.Privilege privilege) {
    final Authorizer authorizer = context.getAuthorizer();
    final String actor = context.getActorUrn();
    final ConjunctivePrivilegeGroup andGroup =
        new ConjunctivePrivilegeGroup(ImmutableList.of(privilege.getType()));
    return AuthUtil.isAuthorized(
        authorizer, actor, resourceSpec, new DisjunctivePrivilegeGroup(ImmutableList.of(andGroup)));
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup) {
    return AuthUtil.isAuthorized(authorizer, actor, Optional.empty(), privilegeGroup);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull String resourceType,
      @Nonnull String resource,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup) {
    final EntitySpec resourceSpec = new EntitySpec(resourceType, resource);
    return AuthUtil.isAuthorized(authorizer, actor, Optional.of(resourceSpec), privilegeGroup);
  }

  private AuthorizationUtils() {}
}
