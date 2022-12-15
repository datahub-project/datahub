package com.linkedin.datahub.graphql.authorization;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.Authorizer;
import com.datahub.authorization.ResourceSpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import java.time.Clock;
import java.util.Optional;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;
import static com.linkedin.metadata.Constants.*;


public class AuthorizationUtils {

  private static final Clock CLOCK = Clock.systemUTC();

  public static AuditStamp createAuditStamp(@Nonnull QueryContext context) {
    return new AuditStamp().setTime(CLOCK.millis()).setActor(UrnUtils.getUrn(context.getActorUrn()));
  }

  public static boolean canManageUsersAndGroups(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_USERS_AND_GROUPS_PRIVILEGE);
  }

  public static boolean canManagePolicies(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_POLICIES_PRIVILEGE);
  }

  public static boolean canGeneratePersonalAccessToken(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE);
  }

  public static boolean canManageTokens(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_ACCESS_TOKENS);
  }

  /**
   * Returns true if the current used is able to create Domains. This is true if the user has the 'Manage Domains' or 'Create Domains' platform privilege.
   */
  public static boolean canCreateDomains(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(
        ImmutableList.of(
            new ConjunctivePrivilegeGroup(ImmutableList.of(
                PoliciesConfig.CREATE_DOMAINS_PRIVILEGE.getType())),
            new ConjunctivePrivilegeGroup(ImmutableList.of(
                PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE.getType()))
        ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        orPrivilegeGroups);
  }

  public static boolean canManageDomains(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE);
  }

  public static boolean canManageGlossaries(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_GLOSSARIES_PRIVILEGE);
  }

  /**
   * Returns true if the current used is able to create Tags. This is true if the user has the 'Manage Tags' or 'Create Tags' platform privilege.
   */
  public static boolean canCreateTags(@Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(
        ImmutableList.of(
            new ConjunctivePrivilegeGroup(ImmutableList.of(
            PoliciesConfig.CREATE_TAGS_PRIVILEGE.getType())),
            new ConjunctivePrivilegeGroup(ImmutableList.of(
                PoliciesConfig.MANAGE_TAGS_PRIVILEGE.getType()))
        ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        orPrivilegeGroups);
  }

  public static boolean canManageTags(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_TAGS_PRIVILEGE);
  }

  public static boolean canDeleteEntity(@Nonnull Urn entityUrn, @Nonnull QueryContext context) {
    return isAuthorized(context, Optional.of(new ResourceSpec(entityUrn.getEntityType(), entityUrn.toString())), PoliciesConfig.DELETE_ENTITY_PRIVILEGE);
  }

  public static boolean canManageUserCredentials(@Nonnull QueryContext context) {
    return isAuthorized(context, Optional.empty(), PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE);
  }

  public static boolean canEditGroupMembers(@Nonnull String groupUrnStr, @Nonnull QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(
        ImmutableList.of(ALL_PRIVILEGES_GROUP,
            new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_GROUP_MEMBERS_PRIVILEGE.getType()))));

    return AuthorizationUtils.isAuthorized(context.getAuthorizer(), context.getActorUrn(), CORP_GROUP_ENTITY_NAME,
        groupUrnStr, orPrivilegeGroups);
  }

  public static boolean isAuthorized(
      @Nonnull QueryContext context,
      @Nonnull Optional<ResourceSpec> resourceSpec,
      @Nonnull PoliciesConfig.Privilege privilege) {
    final Authorizer authorizer = context.getAuthorizer();
    final String actor = context.getActorUrn();
    final ConjunctivePrivilegeGroup andGroup = new ConjunctivePrivilegeGroup(ImmutableList.of(privilege.getType()));
    return isAuthorized(authorizer, actor, resourceSpec, new DisjunctivePrivilegeGroup(ImmutableList.of(andGroup)));
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    return isAuthorized(authorizer, actor, Optional.empty(), privilegeGroup);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull String resourceType,
      @Nonnull String resource,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    final ResourceSpec resourceSpec = new ResourceSpec(resourceType, resource);
    return isAuthorized(authorizer, actor, Optional.of(resourceSpec), privilegeGroup);
  }

  public static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull Optional<ResourceSpec> maybeResourceSpec,
      @Nonnull DisjunctivePrivilegeGroup privilegeGroup
  ) {
    for (ConjunctivePrivilegeGroup andPrivilegeGroup : privilegeGroup.getAuthorizedPrivilegeGroups()) {
      // If any conjunctive privilege group is authorized, then the entire request is authorized.
      if (isAuthorized(authorizer, actor, andPrivilegeGroup, maybeResourceSpec)) {
        return true;
      }
    }
    // If none of the disjunctive privilege groups were authorized, then the entire request is not authorized.
    return false;
  }

  private static boolean isAuthorized(
      @Nonnull Authorizer authorizer,
      @Nonnull String actor,
      @Nonnull ConjunctivePrivilegeGroup requiredPrivileges,
      @Nonnull Optional<ResourceSpec> resourceSpec) {
    // Each privilege in a group _must_ all be true to permit the operation.
    for (final String privilege : requiredPrivileges.getRequiredPrivileges()) {
      // Create and evaluate an Authorization request.
      final AuthorizationRequest request = new AuthorizationRequest(actor, privilege, resourceSpec);
      final AuthorizationResult result = authorizer.authorize(request);
      if (AuthorizationResult.Type.DENY.equals(result.getType())) {
        // Short circuit.
        return false;
      }
    }
    return true;
  }

  private AuthorizationUtils() { }

}

