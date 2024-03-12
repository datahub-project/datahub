package com.linkedin.datahub.graphql.authorization;

import static com.datahub.authorization.AuthUtil.canViewEntity;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.authorization.Conjunctive;
import com.linkedin.metadata.authorization.Disjunctive;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.time.Clock;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AuthorizationUtils {

  private static final Clock CLOCK = Clock.systemUTC();

  public static final ConjunctivePrivilegeGroup ALL_PRIVILEGES_GROUP =
      new ConjunctivePrivilegeGroup(
          ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()));

  public static boolean canManageUsersAndGroups(@Nonnull QueryContext context) {
    return isAuthorized(
        context, null, Disjunctive.disjoint(PoliciesConfig.MANAGE_USERS_AND_GROUPS_PRIVILEGE));
  }

  public static boolean canManagePolicies(@Nonnull QueryContext context) {
    return isAuthorized(
        context, null, PoliciesConfig.lookupEntityAPIPrivilege(POLICY_ENTITY_NAME, MANAGE));
  }

  public static boolean canGeneratePersonalAccessToken(@Nonnull QueryContext context) {
    return isAuthorized(
        context,
        null,
        Disjunctive.disjoint(PoliciesConfig.GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE));
  }

  public static boolean canManageTokens(@Nonnull QueryContext context) {
    return isAuthorized(context, null, Disjunctive.disjoint(PoliciesConfig.MANAGE_ACCESS_TOKENS));
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

    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  public static boolean canManageDomains(@Nonnull QueryContext context) {
    return isAuthorized(
        context, null, Disjunctive.disjoint(PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE));
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

    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  public static boolean canManageTags(@Nonnull QueryContext context) {
    return isAuthorized(context, null, Disjunctive.disjoint(PoliciesConfig.MANAGE_TAGS_PRIVILEGE));
  }

  public static boolean canDeleteEntity(@Nonnull Urn entityUrn, @Nonnull QueryContext context) {
    return AuthUtil.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        PoliciesConfig.lookupAPIPrivilege(ENTITY, DELETE),
        new EntitySpec(entityUrn.getEntityType(), entityUrn.toString()));
  }

  public static boolean canManageUserCredentials(@Nonnull QueryContext context) {
    return isAuthorized(
        context, null, Disjunctive.disjoint(PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE));
  }

  public static boolean canEditGroupMembers(
      @Nonnull String groupUrnStr, @Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                ALL_PRIVILEGES_GROUP,
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE.getType()))));

    return isAuthorized(
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

    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  public static boolean canManageGlobalAnnouncements(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(
                        PoliciesConfig.MANAGE_GLOBAL_ANNOUNCEMENTS_PRIVILEGE.getType()))));

    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), orPrivilegeGroups);
  }

  public static boolean canManageGlobalViews(@Nonnull QueryContext context) {
    return isAuthorized(context, null, Disjunctive.disjoint(PoliciesConfig.MANAGE_GLOBAL_VIEWS));
  }

  public static boolean canManageOwnershipTypes(@Nonnull QueryContext context) {
    return isAuthorized(
        context, null, Disjunctive.disjoint(PoliciesConfig.MANAGE_GLOBAL_OWNERSHIP_TYPES));
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

  /*
   * Optionally check view permissions against a list of urns if the config option is enabled
   */
  public static void checkViewPermissions(@Nonnull List<Urn> urns, @Nonnull QueryContext context) {

    if (!canViewEntity(context.getAuthentication(), context.getAuthorizer(), urns)) {
      throw new AuthorizationException("Unauthorized to view this entity");
    }
  }

  public static boolean isAuthorized(
      @Nonnull QueryContext context,
      @Nullable EntitySpec resourceSpec,
      @Nonnull final PoliciesConfig.Privilege privilege) {
    final Authorizer authorizer = context.getAuthorizer();
    final String actor = context.getActorUrn();

    return AuthUtil.isAuthorized(authorizer, actor, privilege, resourceSpec);
  }

  public static boolean isAuthorized(
      @Nonnull QueryContext context,
      @Nullable EntitySpec resourceSpec,
      @Nonnull final Disjunctive<Conjunctive<PoliciesConfig.Privilege>> privileges) {
    final Authorizer authorizer = context.getAuthorizer();
    final String actor = context.getActorUrn();

    return AuthUtil.isAuthorized(
        authorizer, actor, AuthUtil.buildDisjunctivePrivilegeGroup(privileges), resourceSpec);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup) {
    return AuthUtil.isAuthorized(authorizer, actor, privilegeGroup, (EntitySpec) null);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull String resourceType,
      @Nonnull String resource,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup) {
    final EntitySpec resourceSpec = new EntitySpec(resourceType, resource);
    return AuthUtil.isAuthorized(authorizer, actor, privilegeGroup, resourceSpec);
  }

  public static boolean isViewDatasetUsageAuthorized(
      final Urn resourceUrn, final QueryContext context) {
    return isAuthorized(
        context,
        new EntitySpec(resourceUrn.getEntityType(), resourceUrn.toString()),
        Disjunctive.disjoint(PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE));
  }

  private AuthorizationUtils() {}
}
