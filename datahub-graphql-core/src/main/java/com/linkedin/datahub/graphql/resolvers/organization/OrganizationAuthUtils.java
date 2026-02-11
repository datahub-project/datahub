package com.linkedin.datahub.graphql.resolvers.organization;

import static com.linkedin.metadata.Constants.DATAHUB_ACTOR;
import static com.linkedin.metadata.Constants.ORGANIZATION_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.Constants.USER_ORGANIZATIONS_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.CREATE;
import static com.linkedin.metadata.authorization.ApiOperation.MANAGE;

import com.datahub.authorization.AuthUtil;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.identity.UserOrganizations;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrganizationAuthUtils {

  /** Returns true if the current user is authorized to create an organization. */
  public static boolean canCreateOrganization(@Nonnull QueryContext context) {
    return AuthUtil.isAuthorizedEntityType(
        context.getOperationContext(), CREATE, List.of(ORGANIZATION_ENTITY_NAME));
  }

  /**
   * Returns true if the current user is authorized to manage (update/delete) a specific
   * organization.
   */
  public static boolean canManageOrganization(
      @Nonnull QueryContext context, @Nonnull Urn organizationUrn) {
    return AuthUtil.isAuthorizedEntityUrns(
        context.getOperationContext(), MANAGE, List.of(organizationUrn));
  }

  /**
   * Returns true if the current user is authorized to manage members of a specific organization.
   * Currently reuses the MANAGE privilege on the organization entity.
   */
  public static boolean canManageOrganizationMembers(
      @Nonnull QueryContext context, @Nonnull Urn organizationUrn) {
    return canManageOrganization(context, organizationUrn);
  }

  /**
   * Returns true if the current user is authorized to view a specific organization. For
   * multi-tenant isolation, users can only view organizations they belong to, unless they are
   * system actors or have explicit READ permissions.
   */
  public static boolean canViewOrganization(
      @Nonnull QueryContext context, @Nonnull Urn organizationUrn) {
    OperationContext opContext = context.getOperationContext();

    // Get authenticated user
    Urn userUrn = UrnUtils.getUrn(context.getActorUrn());

    // System actors bypass organization checks
    if (SYSTEM_ACTOR.equals(userUrn.toString()) || DATAHUB_ACTOR.equals(userUrn.toString())) {
      return true;
    }

    // Check if user belongs to the organization
    Set<Urn> userOrganizations = getUserOrganizations(opContext, userUrn);
    if (userOrganizations.contains(organizationUrn)) {
      return true;
    }

    // If user has no organizations, deny access (full isolation)
    if (userOrganizations.isEmpty()) {
      log.debug(
          "User {} has no organizations - denying access to organization {}",
          userUrn,
          organizationUrn);
      return false;
    }

    // User has organizations but not this one - deny access
    log.debug(
        "User {} does not belong to organization {} - denying access", userUrn, organizationUrn);
    return false;
  }

  /** Fetch organizations for a given user from the UserOrganizations aspect. */
  private static Set<Urn> getUserOrganizations(OperationContext opContext, Urn userUrn) {
    try {
      if (opContext.getAspectRetriever() != null) {
        com.linkedin.entity.Aspect aspect =
            opContext
                .getAspectRetriever()
                .getLatestAspectObject(userUrn, USER_ORGANIZATIONS_ASPECT_NAME);

        if (aspect != null) {
          UserOrganizations userOrgsAspect = new UserOrganizations(aspect.data());
          if (userOrgsAspect.getOrganizations() != null) {
            return userOrgsAspect.getOrganizations().stream().collect(Collectors.toSet());
          }
        }
      }
    } catch (Exception e) {
      log.error("Failed to fetch organizations for user: {}", userUrn, e);
    }
    return Set.of();
  }

  private OrganizationAuthUtils() {}
}
